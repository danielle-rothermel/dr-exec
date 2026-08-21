"""Worker-side loop for the worker-pool importable JSON executor.

This module is the entry point of every spawned worker process. It imports the
declared entry-point module once at startup, then serves one request at a time
over the two pipes the parent handed it.
"""

from __future__ import annotations

import importlib
import json
import os
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable
from enum import UNIQUE, StrEnum, verify
from typing import IO, Final, cast

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    StrictJsonError,
    build_identity_document,
    canonical_identity_json_bytes,
    validate_identity_document,
    validate_strict_json,
)

# Persisted-format contract: the worker exchanges the same fixed envelope
# literals as the in-process and process executors. This module is the ``-c``
# entry module of every spawned worker and imports nothing from ``dr_exec``, so
# the literals are re-declared here deliberately and a golden test pins them
# equal to the owning definitions in ``dr_exec.importable_json``.
ENVELOPE_SCHEMA: Final = "dr_exec.importable_json"
ENVELOPE_SCHEMA_VERSION: Final = 1

WORKER_FRAME_TERMINATOR: Final = b"\n"

# Wire values for the one status field of a worker result frame. Never derived
# from Python identifiers.
STATUS_KEY: Final = "status"
DETAIL_KEY: Final = "detail"
RESULT_KEY: Final = "result"


@verify(UNIQUE)
class WorkerFrameStatus(StrEnum):
    OK = "ok"
    PAYLOAD_RAISED = "payload_raised"
    PAYLOAD_RESULT_INVALID = "payload_result_invalid"
    EXECUTOR_REJECTED = "executor_rejected"


# Persisted-format contract: the bounded payload-error rendering is the same
# one ``dr_exec.importable_json`` applies in-process. This module cannot import
# it, so the constants and the formatting are re-declared here deliberately and
# a golden test pins both copies equal to the owning definitions.
PAYLOAD_ERROR_DETAIL_MAX_BYTES: Final = 8192
PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER: Final = "... [truncated]"
PAYLOAD_ERROR_TRACEBACK_FRAME_LIMIT: Final = 20
PAYLOAD_RAISED_DETAIL_PREFIX: Final = "the importable JSON entry point raised"


def format_payload_error_detail(error: BaseException, /) -> str:
    """Render one payload exception as bounded detail for the parent.

    The cap applies to the whole rendered string, so no entry point can grow
    a result frame without bound by raising with a huge message.
    """

    error_type = type(error)
    qualified = (
        error_type.__qualname__
        if error_type.__module__ == "builtins"
        else f"{error_type.__module__}.{error_type.__qualname__}"
    )
    rendered = traceback.format_exception(
        error, limit=-PAYLOAD_ERROR_TRACEBACK_FRAME_LIMIT
    )
    detail = (
        f"{PAYLOAD_RAISED_DETAIL_PREFIX}: {qualified}: {error}\n"
        + "".join(rendered)
    )
    encoded = detail.encode("utf-8")
    if len(encoded) <= PAYLOAD_ERROR_DETAIL_MAX_BYTES:
        return detail
    budget = PAYLOAD_ERROR_DETAIL_MAX_BYTES - len(
        PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER.encode("utf-8")
    )
    kept = encoded[:budget].decode("utf-8", errors="ignore")
    return kept + PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER


READY_FRAME: Final = b"ready" + WORKER_FRAME_TERMINATOR

_STARTUP_IMPORT_FAILED_EXIT_CODE: Final = 3

# How often the worker checks that its parent is still alive. This is a
# liveness heartbeat, not a limit: it bounds how long an orphan survives its
# parent, and never how long a job may run or how large a payload may be.
PARENT_LIVENESS_POLL_SECONDS: Final = 2.0

ORPHANED_WORKER_EXIT_CODE: Final = 4


def main() -> None:
    """Serve requests until the parent closes the request pipe."""

    module_name = sys.argv[1]
    attribute_name = sys.argv[2]
    request_fd = int(sys.argv[3])
    result_fd = int(sys.argv[4])
    parent_pid = int(sys.argv[5])

    # Own group so orphan and parent-side stop reach grandchildren that
    # stay in it. Idempotent when spawn already passed process_group=0.
    try:
        os.setpgid(0, 0)
    except OSError:
        pass

    # Started before the entry-point import, because an import that blocks
    # forever would otherwise outlive the parent just as a job would.
    watch_parent(parent_pid)

    requests = open_request_reader(request_fd)
    # The result pipe stays unbuffered because every frame is written and
    # flushed whole, and the parent must see it immediately.
    results = os.fdopen(result_fd, "wb", buffering=0)
    try:
        try:
            entry_point = _resolve_entry_point(module_name, attribute_name)
        except BaseException:  # noqa: BLE001 - the parent sees a closed pipe
            results.close()
            sys.exit(_STARTUP_IMPORT_FAILED_EXIT_CODE)
        _write_frame(results, READY_FRAME)
        _serve(entry_point, requests=requests, results=results)
    finally:
        # A descendant can inherit the result pipe. Normal leader exit,
        # SystemExit, and a failed startup import would otherwise leave
        # that copy open so the parent waits forever for EOF and never
        # reaches its own group kill.
        kill_own_process_group()


def watch_parent(parent_pid: int, /) -> threading.Thread:
    """Exit this worker once its parent process is gone.

    An idle worker already ends itself when the request pipe reaches end of
    file, but a worker in the middle of a job is not reading that pipe and
    would otherwise run to completion — possibly forever, at full CPU — with
    nobody left to receive its answer. The kernel reparents an orphan, so a
    parent pid that no longer matches is the signal that the pool that owns
    this worker died.

    The owning pid is read in the parent and handed down rather than read here
    with ``os.getppid()``: a parent that dies before this worker reaches its
    first poll has already been replaced by the reaper, and a worker that
    learned its parent's identity that late would treat the reaper as its
    owner and never notice it had been orphaned at all.

    This is best-effort cleanup, not supervision: it imposes no ceiling on a
    job's runtime, its payload size, or anything else a live parent asked for.
    """

    def poll() -> None:
        while os.getppid() == parent_pid:
            time.sleep(PARENT_LIVENESS_POLL_SECONDS)
        # The pipes lead nowhere and the entry point may hold locks or be
        # uninterruptible, so leave immediately rather than unwinding.
        kill_own_process_group()
        os._exit(ORPHANED_WORKER_EXIT_CODE)

    watchdog = threading.Thread(
        target=poll, name="dr-exec-worker-parent-watchdog", daemon=True
    )
    watchdog.start()
    return watchdog


def kill_own_process_group() -> None:
    """SIGKILL this worker's group only when this process leads it.

    A worker that failed ``setpgid`` still shares the parent's group.
    ``killpg(getpgrp())`` in that state would take the parent with it.
    """

    pid = os.getpid()
    if os.getpgrp() != pid:
        return
    try:
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        pass


def open_request_reader(request_fd: int, /) -> IO[bytes]:
    """Open the request pipe buffered, so frames are read in blocks.

    A newline-delimited read on an unbuffered stream reads one byte per
    syscall, which costs roughly a second per megabyte. Buffering bounds
    nothing: a frame is still read whole, at whatever size it arrives.
    """

    return os.fdopen(request_fd, "rb")


def _resolve_entry_point(
    module_name: str, attribute_name: str, /
) -> Callable[[Jsonable], object]:
    module = importlib.import_module(module_name)
    entry_point = getattr(module, attribute_name)
    if not callable(entry_point):
        raise TypeError("the imported module attribute is not callable")
    return cast("Callable[[Jsonable], object]", entry_point)


def _serve(
    entry_point: Callable[[Jsonable], object],
    /,
    *,
    requests: IO[bytes],
    results: IO[bytes],
) -> None:
    while True:
        frame = _read_frame(requests)
        if frame is None:
            return
        _write_frame(results, _handle(entry_point, frame))


def _handle(
    entry_point: Callable[[Jsonable], object], frame: bytes, /
) -> bytes:
    try:
        request = _decode_request(frame)
    except Exception as error:  # noqa: BLE001 - reported to the parent
        return _status_frame(WorkerFrameStatus.EXECUTOR_REJECTED, str(error))
    try:
        payload = validate_strict_json(request.payload)
    except StrictJsonError as error:
        return _status_frame(WorkerFrameStatus.EXECUTOR_REJECTED, str(error))
    try:
        returned = entry_point(payload)
    except Exception as error:  # noqa: BLE001 - a payload failure is result data
        return _status_frame(
            WorkerFrameStatus.PAYLOAD_RAISED,
            format_payload_error_detail(error),
        )
    try:
        result = validate_strict_json(returned)
    except StrictJsonError as error:
        return _status_frame(
            WorkerFrameStatus.PAYLOAD_RESULT_INVALID, str(error)
        )
    return _result_frame(result)


def _decode_request(frame: bytes, /) -> IdentityDocument:
    document = validate_identity_document(json.loads(frame.decode("utf-8")))
    if (
        document.schema != ENVELOPE_SCHEMA
        or document.schema_version != ENVELOPE_SCHEMA_VERSION
    ):
        raise ValueError("request does not use the importable JSON envelope")
    return document


def _result_frame(result: Jsonable, /) -> bytes:
    envelope = build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        payload={STATUS_KEY: WorkerFrameStatus.OK, RESULT_KEY: result},
    )
    return canonical_identity_json_bytes(envelope) + WORKER_FRAME_TERMINATOR


def _status_frame(status: WorkerFrameStatus, detail: str, /) -> bytes:
    envelope = build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        payload={STATUS_KEY: status, DETAIL_KEY: detail},
    )
    return canonical_identity_json_bytes(envelope) + WORKER_FRAME_TERMINATOR


def _read_frame(stream: IO[bytes], /) -> bytes | None:
    """Read one newline-terminated frame, streaming rather than bounding it."""

    chunks: list[bytes] = []
    while True:
        chunk = stream.readline()
        if not chunk:
            return None if not chunks else b"".join(chunks)
        chunks.append(chunk)
        if chunk.endswith(WORKER_FRAME_TERMINATOR):
            return b"".join(chunks)


def _write_frame(stream: IO[bytes], frame: bytes, /) -> None:
    stream.write(frame)
    stream.flush()


if __name__ == "__main__":
    main()
