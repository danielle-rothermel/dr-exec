"""An entry-point module whose import blocks until a caller opens its gate.

A worker importing this module never becomes ready, so a job dispatched to it
is waiting on worker startup rather than on the entry point. The gate path
comes from the environment because the import runs before any request arrives.
"""

from __future__ import annotations

import os
from pathlib import Path

GATE_VARIABLE = "DR_EXEC_TEST_IMPORT_GATE"

_gate = Path(os.environ[GATE_VARIABLE])
with _gate.open("rb") as _blocked:
    _blocked.read(1)


def echo(value: object) -> dict[str, object]:
    return {"value": value}
