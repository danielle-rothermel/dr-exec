from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from typing import Final

SPAWN_HELPER_ARGUMENTS: Final = ("-I", "-c")
STATUS_STAGE_KEY: Final = "stage"
STATUS_ERRNO_KEY: Final = "errno"
SETUP_STAGE_SESSION: Final = "session"
SETUP_STAGE_CHDIR: Final = "chdir"
SETUP_STAGE_DESCRIPTORS: Final = "descriptors"
SETUP_STAGE_EXEC: Final = "exec"

SETUP_FAILURE_EXIT_CODE: Final = 127

PAYLOAD_STDIN_DESCRIPTOR: Final = 0
PAYLOAD_STDOUT_DESCRIPTOR: Final = 1
PAYLOAD_STDERR_DESCRIPTOR: Final = 2
PAYLOAD_PROTOCOL_DESCRIPTOR: Final = 3

_HELPER_BODY = '''
import json as _dr_exec_json
import os as _dr_exec_os


def _dr_exec_fail(stage, error):
    """Report one setup failure and leave without reaching exec."""
    line = _dr_exec_json.dumps(
        {
            _DR_EXEC_STATUS_STAGE_KEY: stage,
            _DR_EXEC_STATUS_ERRNO_KEY: getattr(error, "errno", None),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    try:
        _dr_exec_os.write(_DR_EXEC_STATUS_DESCRIPTOR, line + b"\\n")
    except OSError:
        pass
    _dr_exec_os._exit(_DR_EXEC_SETUP_FAILURE_EXIT_CODE)


def _dr_exec_setup():
    # The parent passed every intended descriptor inheritable, so the
    # status descriptor is made close-on-exec here: that is what makes a
    # successful payload exec close it and show the parent an EOF on an
    # empty pipe. Without it the payload would hold the status pipe open
    # and the parent could never tell "started" from "still setting up".
    _dr_exec_os.set_inheritable(_DR_EXEC_STATUS_DESCRIPTOR, False)
    try:
        _dr_exec_os.setsid()
    except OSError as error:
        _dr_exec_fail(_DR_EXEC_STAGE_SESSION, error)
    try:
        _dr_exec_os.chdir(_DR_EXEC_WORKING_DIRECTORY)
    except OSError as error:
        _dr_exec_fail(_DR_EXEC_STAGE_CHDIR, error)
    try:
        # Stage every source above the target range first, then place
        # them. Staging is what makes the placement order irrelevant: a
        # source whose number happens to be another pair's target would
        # otherwise be overwritten before it was read. The parent passed
        # the sources inheritable, so every staged copy is closed after
        # it lands -- an unclosed original would survive exec as a second
        # copy of the same pipe end, and the payload's own exit would
        # then never produce the EOF the parent drains on.
        staged = []
        for source, target in _DR_EXEC_DESCRIPTOR_MAP:
            held = _dr_exec_os.dup(source)
            while held <= _DR_EXEC_HIGHEST_TARGET_DESCRIPTOR:
                held = _dr_exec_os.dup(held)
            staged.append((held, target))
        for source, _ in _DR_EXEC_DESCRIPTOR_MAP:
            _dr_exec_os.close(source)
        for held, target in staged:
            _dr_exec_os.dup2(held, target, inheritable=True)
            _dr_exec_os.close(held)
    except OSError as error:
        _dr_exec_fail(_DR_EXEC_STAGE_DESCRIPTORS, error)
    try:
        _dr_exec_os.execv(_DR_EXEC_EXECUTABLE, _DR_EXEC_ARGV)
    except OSError as error:
        _dr_exec_fail(_DR_EXEC_STAGE_EXEC, error)


_dr_exec_setup()
'''


def spawn_bootstrap_source(
    *,
    executable: str,
    argv: tuple[str, ...],
    working_directory: str,
    descriptor_map: tuple[tuple[int, int], ...],
    status_descriptor: int,
) -> str:
    bindings: dict[str, object] = {
        "_DR_EXEC_EXECUTABLE": executable,
        "_DR_EXEC_ARGV": list(argv),
        "_DR_EXEC_WORKING_DIRECTORY": working_directory,
        "_DR_EXEC_DESCRIPTOR_MAP": [list(pair) for pair in descriptor_map],
        "_DR_EXEC_STATUS_DESCRIPTOR": status_descriptor,
        "_DR_EXEC_HIGHEST_TARGET_DESCRIPTOR": PAYLOAD_PROTOCOL_DESCRIPTOR,
        "_DR_EXEC_STATUS_STAGE_KEY": STATUS_STAGE_KEY,
        "_DR_EXEC_STATUS_ERRNO_KEY": STATUS_ERRNO_KEY,
        "_DR_EXEC_STAGE_SESSION": SETUP_STAGE_SESSION,
        "_DR_EXEC_STAGE_CHDIR": SETUP_STAGE_CHDIR,
        "_DR_EXEC_STAGE_DESCRIPTORS": SETUP_STAGE_DESCRIPTORS,
        "_DR_EXEC_STAGE_EXEC": SETUP_STAGE_EXEC,
        "_DR_EXEC_SETUP_FAILURE_EXIT_CODE": SETUP_FAILURE_EXIT_CODE,
    }
    rendered = [f"{name} = {value!r}" for name, value in bindings.items()]
    return "\n".join((*rendered, _HELPER_BODY))


@dataclass(frozen=True, slots=True)
class SetupFailure:
    """Bootstrap setup failure reported before payload exec."""

    stage: str
    errno: int | None


def parse_setup_status(line: bytes, /) -> SetupFailure | None:
    """Treat malformed nonempty status as setup failure, never success."""

    if not line:
        return None
    try:
        payload = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return SetupFailure(stage=SETUP_STAGE_EXEC, errno=None)
    if not isinstance(payload, dict):
        return SetupFailure(stage=SETUP_STAGE_EXEC, errno=None)
    stage = payload.get(STATUS_STAGE_KEY)
    reported = payload.get(STATUS_ERRNO_KEY)
    return SetupFailure(
        stage=stage if isinstance(stage, str) else SETUP_STAGE_EXEC,
        errno=reported if isinstance(reported, int) else None,
    )


def launch_bootstrap(
    *,
    executable: str,
    argv: tuple[str, ...],
    environment: dict[str, str],
    working_directory: str,
    descriptor_map: tuple[tuple[int, int], ...],
    status_write: int,
) -> subprocess.Popen[bytes]:
    """Launch with only the explicitly mapped descriptors inherited."""

    source = spawn_bootstrap_source(
        executable=executable,
        argv=argv,
        working_directory=working_directory,
        descriptor_map=descriptor_map,
        status_descriptor=status_write,
    )
    passed = (*(source_fd for source_fd, _ in descriptor_map), status_write)
    return subprocess.Popen(
        [sys.executable, *SPAWN_HELPER_ARGUMENTS, source],
        close_fds=True,
        pass_fds=passed,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def signal_process_group(pid: int, number: int, /) -> bool:
    try:
        os.killpg(pid, number)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    return True


TERMINATION_SIGNAL: Final = signal.SIGTERM
ESCALATION_SIGNAL: Final = signal.SIGKILL


__all__ = [
    "ESCALATION_SIGNAL",
    "PAYLOAD_PROTOCOL_DESCRIPTOR",
    "PAYLOAD_STDERR_DESCRIPTOR",
    "PAYLOAD_STDIN_DESCRIPTOR",
    "PAYLOAD_STDOUT_DESCRIPTOR",
    "SETUP_FAILURE_EXIT_CODE",
    "SETUP_STAGE_CHDIR",
    "SETUP_STAGE_DESCRIPTORS",
    "SETUP_STAGE_EXEC",
    "SETUP_STAGE_SESSION",
    "SPAWN_HELPER_ARGUMENTS",
    "STATUS_ERRNO_KEY",
    "STATUS_STAGE_KEY",
    "TERMINATION_SIGNAL",
    "SetupFailure",
    "launch_bootstrap",
    "parse_setup_status",
    "signal_process_group",
    "spawn_bootstrap_source",
]
