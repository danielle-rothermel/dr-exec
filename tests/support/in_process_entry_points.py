from __future__ import annotations

import os
import time
from pathlib import Path
from typing import cast
from uuid import uuid4

# Set once per interpreter that imports this module. A worker imports its
# entry-point module exactly once, so every job a given worker serves reports
# the same import id.
IMPORT_ID = str(uuid4())

_CALLS = 0


def echo(value: object) -> dict[str, object]:
    return {"value": value}


def return_null(_value: object) -> None:
    return None


def raise_error(_value: object) -> None:
    raise RuntimeError("entry point failed")


def raise_sentinel_value_error(_value: object) -> None:
    raise ValueError("SENTINEL-12345")


# The message alone is far larger than the detail cap, so the rendered detail
# has to be truncated no matter how few frames the traceback carries.
HUGE_MESSAGE_CHARACTER_COUNT = 200_000


def raise_huge_message(_value: object) -> None:
    raise ValueError("SENTINEL-12345" + "x" * HUGE_MESSAGE_CHARACTER_COUNT)


class UnprintableError(Exception):
    """An exception whose own rendering raises.

    A payload controls its exception type, so the diagnostic formatter must
    stay total against one that cannot be stringified at all.
    """

    def __str__(self) -> str:
        raise TypeError("SENTINEL-UNPRINTABLE")


def raise_unprintable(_value: object) -> None:
    raise UnprintableError()


# A payload also controls the *type* of the strings the formatter reads. A
# ``str`` subclass passes ``isinstance(x, str)`` while overriding the very
# methods the formatter calls, so each of these hides a raise behind one.
HOSTILE_STR_SENTINEL = "SENTINEL-HOSTILE-STR"


class _EncodeRaisingStr(str):
    def encode(self, *args: object, **kwargs: object) -> bytes:
        raise RuntimeError("SENTINEL-HOSTILE-ENCODE")

    def __format__(self, spec: str) -> str:
        raise RuntimeError("SENTINEL-HOSTILE-ENCODE")


class _SizingRaisingStr(str):
    # Interpolation is the first step that touches the message, so a raising
    # __format__ is what actually escapes the old guards; __len__, __getitem__
    # and __add__ cover the sizing and concatenation steps behind it.
    def __format__(self, spec: str) -> str:
        raise RuntimeError("SENTINEL-HOSTILE-FORMAT")

    def __len__(self) -> int:
        raise RuntimeError("SENTINEL-HOSTILE-LEN")

    def __getitem__(self, index: object) -> str:
        raise RuntimeError("SENTINEL-HOSTILE-GETITEM")

    def __add__(self, other: object) -> str:
        raise RuntimeError("SENTINEL-HOSTILE-ADD")


class _HostileMessageError(Exception):
    """An exception whose ``__str__`` returns a hostile ``str`` subclass."""

    def __init__(self, message: str) -> None:
        super().__init__()
        self._message = message

    def __str__(self) -> str:
        return self._message


def raise_hostile_encode_message(_value: object) -> None:
    raise _HostileMessageError(_EncodeRaisingStr(HOSTILE_STR_SENTINEL))


def raise_hostile_sizing_message(_value: object) -> None:
    raise _HostileMessageError(_SizingRaisingStr(HOSTILE_STR_SENTINEL))


class HostileQualnameError(Exception):
    """An exception whose class metadata is a hostile ``str`` subclass."""


# Assigned after the class body so the hostile subclass replaces the plain
# ``str`` the class statement installed.
HostileQualnameError.__qualname__ = _EncodeRaisingStr(  # type: ignore[assignment]
    "HostileQualnameError"
)


def raise_hostile_qualname(_value: object) -> None:
    raise HostileQualnameError(HOSTILE_STR_SENTINEL)


# A lone surrogate is a legal ``str`` that strict UTF-8 refuses to encode.
LONE_SURROGATE_MESSAGE = "SENTINEL-12345\ud800"


def raise_lone_surrogate(_value: object) -> None:
    raise ValueError(LONE_SURROGATE_MESSAGE)


# Undecodable OS-level bytes reach Python as ``surrogateescape`` code points,
# which strict UTF-8 also refuses to encode.
SURROGATE_ESCAPE_MESSAGE = "SENTINEL-12345" + b"\xff\xfe".decode(
    "utf-8", errors="surrogateescape"
)


def raise_surrogate_escape(_value: object) -> None:
    raise OSError(SURROGATE_ESCAPE_MESSAGE)


def raise_system_exit(value: object) -> None:
    if isinstance(value, dict) and "gate_path" in value:
        block_on_gate(cast("dict[str, object]", value))
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


def return_non_json(value: object) -> object:
    if isinstance(value, dict) and "gate_path" in value:
        block_on_gate(cast("dict[str, object]", value))
    return object()


def echo_unless_asked_to_raise(value: object) -> dict[str, object]:
    """Echo, except for the one input that asks this call to fail."""

    if isinstance(value, dict) and value.get("raise"):
        raise RuntimeError("entry point failed")
    return {"value": value}


def import_count(_value: object) -> dict[str, object]:
    """Report this interpreter's one import and how many jobs it has served."""

    global _CALLS
    _CALLS += 1
    return {"import_id": IMPORT_ID, "calls": _CALLS, "pid": os.getpid()}


def exit_abruptly(_value: object) -> None:
    """Kill this worker mid-job without unwinding."""

    os._exit(9)


def exit_abruptly_unless_asked_to_echo(value: object) -> dict[str, object]:
    """Kill this worker for the one input that asks for it, else echo."""

    if isinstance(value, dict) and value.get("die"):
        os._exit(9)
    return {"value": value}


def echo_or_block_on_gate(value: object) -> dict[str, object]:
    """Block on a caller-opened gate when given one, else echo."""

    if isinstance(value, dict) and "gate_path" in value:
        return block_on_gate(cast("dict[str, object]", value))
    return {"value": value}


def block_on_barrier(value: dict[str, object]) -> dict[str, object]:
    """Announce arrival, then wait until every peer has also arrived.

    Real parallelism is the property: with fewer workers than parties, no
    call can ever observe the full party count and the test's watchdog fires.
    """

    directory = Path(str(value["barrier_directory"]))
    parties = int(str(value["parties"]))
    identity = str(value["identity"])
    (directory / identity).touch()
    while len(list(directory.iterdir())) < parties:
        time.sleep(0.001)
    return {"identity": identity, "pid": os.getpid()}


def block_on_gate(value: dict[str, object]) -> dict[str, object]:
    """Announce readiness, then block until the caller opens the gate.

    The gate is a FIFO the caller alone opens for writing, so the call is
    released by an explicit event rather than by elapsed time.
    """

    Path(str(value["ready_path"])).touch()
    with Path(str(value["gate_path"])).open("rb") as gate:
        gate.read(1)
    return {"released": True}


def fork_child(value: dict[str, object]) -> dict[str, object]:
    """Fork a grandchild that stays in this process group, then block.

    The grandchild writes its pid and lives until the group is signaled.
    The worker announces ready and waits on the caller's gate when one is
    given, or sleeps when the orphan-parent helper asks it to.
    """

    grandchild_pid_path = Path(str(value["grandchild_pid_path"]))
    child = os.fork()
    if child == 0:
        staged = grandchild_pid_path.with_name(
            grandchild_pid_path.name + ".staging"
        )
        staged.write_text(str(os.getpid()), encoding="utf-8")
        staged.replace(grandchild_pid_path)
        while True:
            time.sleep(3600)
    if "gate_path" in value:
        Path(str(value["ready_path"])).touch()
        with Path(str(value["gate_path"])).open("rb") as gate:
            gate.read(1)
        return {"released": True}
    seconds = 100_000.0
    raw_seconds = value.get("seconds")
    if isinstance(raw_seconds, int | float):
        seconds = float(raw_seconds)
    time.sleep(seconds)
    return {"ok": True}


def fork_then_system_exit(value: dict[str, object]) -> None:
    """Fork a grandchild, then leave through SystemExit.

    The parent waits until the grandchild has written its pid so a test
    can observe it, then raises. Group cleanup on that unwind is what
    must kill the leftover and unblock the worker-pool parent.
    """

    grandchild_pid_path = Path(str(value["grandchild_pid_path"]))
    child = os.fork()
    if child == 0:
        staged = grandchild_pid_path.with_name(
            grandchild_pid_path.name + ".staging"
        )
        staged.write_text(str(os.getpid()), encoding="utf-8")
        staged.replace(grandchild_pid_path)
        while True:
            time.sleep(3600)
    while (
        not grandchild_pid_path.exists()
        or not grandchild_pid_path.read_text(encoding="utf-8").strip()
    ):
        time.sleep(0.01)
    raise SystemExit(7)


def burn_until_gate(value: dict[str, object]) -> dict[str, object]:
    """Announce readiness, then spin on CPU until killed or released.

    Nothing here observes cancellation: only killing the worker process can
    stop it, which is what a worker-pool wall-time budget must do.
    """

    Path(str(value["ready_path"])).touch()
    gate = Path(str(value["gate_path"]))
    while not gate.exists():
        pass
    return {"released": True}
