"""The library-owned child wrapper against a real isolated interpreter.

Every case here spawns one real interpreter with the protected pipe mapped
onto fd 3 and the request on stdin, then synchronizes on the protected
stream reaching EOF and on the child's terminal exit status. Nothing here
waits on elapsed time or treats it as evidence.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from dr_serialize import IdentityDocument, build_identity_document

from dr_exec import ExecutorSelfBudgets, ProtocolFailureCode
from dr_exec._bootstrap import (
    DRIVER_ENTRYPOINT_NAME,
    DRIVER_SOURCE_BINDING,
    ISOLATED_INVOCATION_ARGUMENTS,
    PROTOCOL_DESCRIPTOR,
    driver_wrapper_source,
)
from dr_exec._protocol import (
    ProtocolStreamResult,
    read_protocol_stream,
    request_identity_digest,
    request_transport_bytes,
)

ECHO_DRIVER = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    for index in range(request["payload"]["count"]):
        emit({{
            "schema": "dr_exec.test_output",
            "schema_version": 1,
            "payload": {{"index": index, "echo": request["payload"]["echo"]}},
        }})
"""


@dataclass(frozen=True, slots=True)
class ChildRun:
    """One completed child: its protected stream and its exit status."""

    stream: ProtocolStreamResult
    exit_status: int
    stdout: bytes
    stderr: bytes

    @property
    def exited_cleanly(self) -> bool:
        return os.WIFEXITED(self.exit_status) and (
            os.waitstatus_to_exitcode(self.exit_status) == 0
        )


def _request(count: int = 2, echo: str = "value") -> IdentityDocument:
    return build_identity_document(
        schema="dr_exec.test_request",
        schema_version=1,
        payload={"count": count, "echo": echo},
    )


def _run_driver(
    driver_source: str,
    request: IdentityDocument,
    tmp_path: Path,
    /,
    *,
    request_bytes: bytes | None = None,
    self_budgets: ExecutorSelfBudgets | None = None,
) -> ChildRun:
    """Spawn one wrapper child with fd 3 mapped to the protected pipe.

    ``posix_spawn`` file actions place the descriptors exactly, so no
    caller or package Python runs between the fork and the child's
    ``exec``. The engine that will own this mapping in production does not
    exist yet; this helper stands in for it and nothing more.
    """
    wrapper = driver_wrapper_source(driver_source)
    stdout_path = tmp_path / "stdout.bin"
    stderr_path = tmp_path / "stderr.bin"
    stdin_read, stdin_write = os.pipe()
    protocol_read, protocol_write = os.pipe()
    stdout_fd = os.open(stdout_path, os.O_WRONLY | os.O_CREAT, 0o600)
    stderr_fd = os.open(stderr_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, *ISOLATED_INVOCATION_ARGUMENTS, wrapper],
            {},
            file_actions=[
                (os.POSIX_SPAWN_DUP2, stdin_read, 0),
                (os.POSIX_SPAWN_DUP2, stdout_fd, 1),
                (os.POSIX_SPAWN_DUP2, stderr_fd, 2),
                (os.POSIX_SPAWN_DUP2, protocol_write, PROTOCOL_DESCRIPTOR),
            ],
        )
    finally:
        for descriptor in (stdin_read, protocol_write, stdout_fd, stderr_fd):
            os.close(descriptor)
    payload = (
        request_transport_bytes(request)
        if request_bytes is None
        else request_bytes
    )
    with os.fdopen(stdin_write, "wb") as stdin:
        stdin.write(payload)
    with os.fdopen(protocol_read, "rb") as protocol:
        stream = read_protocol_stream(
            protocol,
            request_id_sha256=request_identity_digest(request),
            self_budgets=self_budgets or ExecutorSelfBudgets.unbudgeted(),
        )
    _, exit_status = os.waitpid(pid, 0)
    return ChildRun(
        stream=stream,
        exit_status=exit_status,
        stdout=stdout_path.read_bytes(),
        stderr=stderr_path.read_bytes(),
    )


# --- Invocation shape ----------------------------------------------------


def test_the_wrapper_embeds_consumer_source_as_an_inert_literal() -> None:
    hostile = "'\"\\\n#'''" + '"""'
    wrapper = driver_wrapper_source(hostile)
    binding, _, _ = wrapper.partition("\n")
    assert binding == f"{DRIVER_SOURCE_BINDING} = {hostile!r}"
    namespace: dict[str, object] = {}
    exec(binding, namespace)  # noqa: S102 - the embedding is under test
    assert namespace[DRIVER_SOURCE_BINDING] == hostile


def test_the_wrapper_rejects_nul_bearing_consumer_source() -> None:
    with pytest.raises(ValueError, match="NUL"):
        driver_wrapper_source("x = 1\0")


def test_the_child_observable_literals_are_exactly_pinned() -> None:
    """These are what the child sees; changing one is a contract revision.

    Every other case reads them symbolically and so would follow a
    rename; only spelling the values out catches silent drift.
    """
    assert PROTOCOL_DESCRIPTOR == 3
    assert DRIVER_ENTRYPOINT_NAME == "dr_exec_main"
    assert DRIVER_SOURCE_BINDING == "DR_EXEC_DRIVER_SOURCE"
    assert ISOLATED_INVOCATION_ARGUMENTS == ("-I", "-c")


def test_the_wrapper_renders_the_pinned_literals_once_each() -> None:
    wrapper = driver_wrapper_source("")

    assert "_DR_EXEC_PROTOCOL_DESCRIPTOR = 3\n" in wrapper
    assert "_DR_EXEC_ENTRYPOINT_NAME = 'dr_exec_main'\n" in wrapper
    assert wrapper.count("_DR_EXEC_PROTOCOL_DESCRIPTOR = ") == 1
    assert wrapper.count("_DR_EXEC_ENTRYPOINT_NAME = ") == 1


def test_a_child_spelling_both_literals_bare_runs_and_completes(
    tmp_path: Path,
) -> None:
    """One real child that hardcodes `dr_exec_main` and fd 3.

    The symbolic cases follow a rename of either literal; this one
    cannot, so it fails if the child-observable contract changes.
    """
    literal_driver = """
def dr_exec_main(request, emit):
    emit({
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {"echo": request["payload"]["echo"]},
    })
"""
    request = _request(echo="bare")
    wrapper = driver_wrapper_source(literal_driver)
    protocol_read, protocol_write = os.pipe()
    stdin_read, stdin_write = os.pipe()
    try:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, "-I", "-c", wrapper],
            {},
            file_actions=[
                (os.POSIX_SPAWN_DUP2, stdin_read, 0),
                (os.POSIX_SPAWN_DUP2, protocol_write, 3),
            ],
        )
    finally:
        os.close(stdin_read)
        os.close(protocol_write)
    with os.fdopen(stdin_write, "wb") as stdin:
        stdin.write(request_transport_bytes(request))
    with os.fdopen(protocol_read, "rb") as protocol:
        stream = read_protocol_stream(
            protocol,
            request_id_sha256=request_identity_digest(request),
            self_budgets=ExecutorSelfBudgets.unbudgeted(),
        )
    _, exit_status = os.waitpid(pid, 0)

    assert os.waitstatus_to_exitcode(exit_status) == 0
    assert stream.completed
    assert len(stream.outputs) == 1
    assert stream.outputs[0].payload == {"echo": "bare"}


# --- Complete streams ----------------------------------------------------


def test_a_driver_emitting_outputs_produces_a_complete_stream(
    tmp_path: Path,
) -> None:
    request = _request(count=3, echo="hello")
    run = _run_driver(ECHO_DRIVER, request, tmp_path)
    assert run.exited_cleanly
    assert run.stream.completed
    assert [document.payload for document in run.stream.outputs] == [
        {"index": index, "echo": "hello"} for index in range(3)
    ]


def test_a_driver_emitting_nothing_produces_a_complete_empty_stream(
    tmp_path: Path,
) -> None:
    run = _run_driver(ECHO_DRIVER, _request(count=0), tmp_path)
    assert run.exited_cleanly
    assert run.stream.completed
    assert run.stream.outputs == ()


def test_the_child_receives_exactly_the_canonical_request(
    tmp_path: Path,
) -> None:
    """The prelude digest is computed by the child over the bytes it read.

    A parent-side digest match therefore proves the transported bytes
    arrived intact and complete through EOF.
    """
    request = _request(count=1, echo="é中\U0001f600")
    run = _run_driver(ECHO_DRIVER, request, tmp_path)
    assert run.stream.completed
    assert run.stream.outputs[0].payload == {
        "index": 0,
        "echo": "é中\U0001f600",
    }


def test_protocol_frames_never_reach_the_payload_streams(
    tmp_path: Path,
) -> None:
    driver = f"""
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    sys.stdout.write("payload stdout\\n")
    sys.stderr.write("payload stderr\\n")
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"ok": True}},
    }})
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert run.stream.completed
    assert run.stdout == b"payload stdout\n"
    assert run.stderr == b"payload stderr\n"
    assert b"prelude" not in run.stdout + run.stderr


def test_the_protected_handle_survives_replaced_language_level_streams(
    tmp_path: Path,
) -> None:
    driver = f"""
import io
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    sys.stdin = io.StringIO()
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"survived": True}},
    }})
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert run.stream.completed
    assert run.stream.outputs[0].payload == {"survived": True}


def test_the_payload_cannot_reach_the_stream_through_the_raw_descriptor(
    tmp_path: Path,
) -> None:
    """The wrapper closes the original fd 3 after duplicating it.

    Domain code writing to the well-known descriptor number therefore
    cannot inject bytes into the protected stream; the library-owned
    handle remains the only writer.
    """
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    try:
        os.write({PROTOCOL_DESCRIPTOR}, b"tamper\\n")
        reached = True
    except OSError:
        reached = False
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"reached": reached}},
    }})
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert run.stream.completed
    assert run.stream.outputs[0].payload == {"reached": False}


# --- Payload-owned failures ---------------------------------------------


@pytest.mark.parametrize(
    "driver",
    [
        pytest.param("", id="missing-entrypoint"),
        pytest.param(
            f"{DRIVER_ENTRYPOINT_NAME} = 3",
            id="non-callable-entrypoint",
        ),
        pytest.param("raise RuntimeError('load')", id="source-load-failure"),
        pytest.param("this is not python", id="syntax-error"),
    ],
)
def test_a_broken_driver_leaves_an_incomplete_stream(
    driver: str,
    tmp_path: Path,
) -> None:
    run = _run_driver(driver, _request(), tmp_path)
    assert not run.exited_cleanly
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert run.stream.outputs == ()


def test_a_callback_failure_preserves_the_outputs_already_accepted(
    tmp_path: Path,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"index": 0}},
    }})
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"index": 1}},
    }})
    raise RuntimeError("driver failed midway")
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert not run.exited_cleanly
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert [document.payload for document in run.stream.outputs] == [
        {"index": 0},
        {"index": 1},
    ]


def test_a_driver_emitting_a_non_identity_document_fails_the_stream(
    tmp_path: Path,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit({{"not": "an identity document"}})
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert not run.exited_cleanly
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert run.stream.outputs == ()


def test_a_driver_exiting_the_process_leaves_an_incomplete_stream(
    tmp_path: Path,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit({{
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {{"index": 0}},
    }})
    os._exit(7)
"""
    run = _run_driver(driver, _request(), tmp_path)
    assert os.waitstatus_to_exitcode(run.exit_status) == 7
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert [document.payload for document in run.stream.outputs] == [
        {"index": 0}
    ]


# --- Invalid request: no protocol output --------------------------------


@pytest.mark.parametrize(
    "request_bytes",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"{", id="truncated"),
        pytest.param(b'{"schema": "s"}', id="incomplete-identity"),
        pytest.param(
            b'{"payload":{},"schema":"s","schema_version":1,"extra":0}',
            id="extra-field",
        ),
        pytest.param(
            b'{"payload": {}, "schema": "s", "schema_version": 1}',
            id="non-canonical-spacing",
        ),
        pytest.param(
            b'{"schema":"s","schema_version":1,"payload":{}}',
            id="unsorted-keys",
        ),
        pytest.param(b"\xff\xfe", id="invalid-utf8"),
    ],
)
def test_an_invalid_request_produces_no_protocol_output(
    request_bytes: bytes,
    tmp_path: Path,
) -> None:
    run = _run_driver(
        ECHO_DRIVER,
        _request(),
        tmp_path,
        request_bytes=request_bytes,
    )
    assert not run.exited_cleanly
    assert run.stream.outputs == ()
    assert run.stream.bytes_received == 0
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM


def test_a_request_the_child_did_not_receive_fails_the_identity_binding(
    tmp_path: Path,
) -> None:
    """A prelude can only match bytes the child actually read.

    Sending one canonical request while the parent expects another's
    digest is exactly the substitution the binding exists to catch.
    """
    run = _run_driver(
        ECHO_DRIVER,
        _request(count=1, echo="expected"),
        tmp_path,
        request_bytes=request_transport_bytes(_request(count=1, echo="other")),
    )
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.ID_MISMATCH
    assert run.stream.outputs == ()
