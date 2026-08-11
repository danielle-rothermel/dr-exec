"""A disposable parent that spawns one worker and then waits to be killed.

Run as a script by the orphan-cleanup tests. It prints the worker's pid on
stdout so the test can watch that exact process, then blocks forever: the test
is what ends this parent, with SIGKILL, so no cleanup of its own can run.

``mode`` selects which of the two orphan cases the worker is left in:

``idle``
    The worker has finished starting and is waiting for a request. Losing the
    parent closes the request pipe, so the worker should end at end of file.

``busy``
    The worker is running a job that never returns on its own. It is not
    reading the request pipe, so only the parent-liveness watchdog can end it.
"""

from __future__ import annotations

import os
import sys
import threading

from dr_serialize import build_identity_document, canonical_identity_json_bytes

from dr_exec import ImportableEntryPoint
from dr_exec.execution.worker_pool import _spawn_worker, _StopWatch
from dr_exec.execution.worker_pool_worker import (
    ENVELOPE_SCHEMA,
    ENVELOPE_SCHEMA_VERSION,
    FRAME_TERMINATOR,
)

IDLE = "idle"
BUSY = "busy"

# Large enough that the job cannot end on its own during the test; the test
# never waits for it, and the watchdog is what the case is about.
_NEVER_RETURNS_SECONDS = 100_000


def main() -> None:
    mode = sys.argv[1]
    entry_point = ImportableEntryPoint(
        module_name="support.in_process_entry_points",
        attribute_name="echo" if mode == IDLE else "sleep_long",
    )
    worker = _spawn_worker(entry_point)
    worker.wait_for_ready(stop=_StopWatch(None, None))

    if mode == BUSY:
        envelope = build_identity_document(
            schema=ENVELOPE_SCHEMA,
            schema_version=ENVELOPE_SCHEMA_VERSION,
            payload={"seconds": _NEVER_RETURNS_SECONDS},
        )
        worker.send(canonical_identity_json_bytes(envelope) + FRAME_TERMINATOR)

    print(worker.process.pid, flush=True)
    # Nothing here may exit on its own: the test's SIGKILL is the only end.
    threading.Event().wait()


if __name__ == "__main__":
    main()
    os._exit(0)
