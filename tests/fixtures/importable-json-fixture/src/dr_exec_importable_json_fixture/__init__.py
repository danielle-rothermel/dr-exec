from __future__ import annotations

import atexit
import os
from pathlib import Path
from typing import Any

NOT_CALLABLE = "fixture sentinel"


def echo(value: Any) -> dict[str, Any]:
    return {"module_file": __file__, "value": value}


def return_null(_value: Any) -> None:
    return None


def raise_error(_value: Any) -> None:
    raise RuntimeError("fixture invocation failed")


def return_object(_value: Any) -> object:
    return object()


async def return_coroutine(value: Any) -> Any:
    return value


def return_generator(value: Any):
    yield value


def emit_payload_output(value: dict[str, Any]) -> Any:
    print("x" * int(value["bytes"]))
    return value["result"]


def register_nonzero_exit(value: Any) -> Any:
    atexit.register(os._exit, 7)
    return value


def block_after_ready(value: dict[str, Any]) -> None:
    Path(value["ready_path"]).touch()
    with Path(value["gate_path"]).open("rb") as gate:
        gate.read(1)


def nested(value: dict[str, Any]) -> Any:
    result: Any = value["leaf"]
    for _ in range(int(value["depth"])):
        result = [result]
    return result
