from __future__ import annotations

import time
from pathlib import Path


def echo(value: object) -> dict[str, object]:
    return {"value": value}


def return_null(_value: object) -> None:
    return None


def raise_error(_value: object) -> None:
    raise RuntimeError("entry point failed")


def raise_system_exit(_value: object) -> None:
    raise SystemExit(7)


NOT_CALLABLE = "sentinel"


def sleep_long(value: object) -> dict[str, bool]:
    seconds = 0.2
    if isinstance(value, dict):
        raw_seconds = value.get("seconds")
        if isinstance(raw_seconds, int | float):
            seconds = float(raw_seconds)
    time.sleep(seconds)
    return {"ok": True}


def block_after_ready(value: dict[str, object]) -> None:
    Path(str(value["ready_path"])).touch()
    with Path(str(value["gate_path"])).open("rb") as gate:
        gate.read(1)
