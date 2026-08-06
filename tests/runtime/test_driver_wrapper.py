from __future__ import annotations

import os
import signal
import sys
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from dr_serialize import IdentityDocument, Jsonable, build_identity_document

from dr_exec import ExecutorSelfBudgets, ProtocolFailureCode
from dr_exec.declarations.transport import request_transport_bytes
from dr_exec.runtime.bootstrap import (
    DRIVER_ENTRYPOINT_NAME,
    DRIVER_SOURCE_BINDING,
    ISOLATED_INVOCATION_ARGUMENTS,
    PROTOCOL_DESCRIPTOR,
    driver_wrapper_source,
)
from dr_exec.runtime.protocol import (
    ProtocolStreamResult,
    read_protocol_stream,
    request_identity_digest,
)

pytestmark = [pytest.mark.integration, pytest.mark.subprocess]

ECHO_DRIVER = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    for index in range(request["payload"]["count"]):
        emit({{
            "schema": "dr_exec.test_output",
            "schema_version": 1,
            "payload": {{"index": index, "echo": request["payload"]["echo"]}},
        }})
"""

IDENTITY_ECHO_DRIVER = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit(request)
"""

WATCHDOG_SECONDS = 30.0


@pytest.fixture(autouse=True)
def watchdog() -> Iterator[object]:
    timer = threading.Timer(
        WATCHDOG_SECONDS,
        lambda: os.kill(os.getpid(), signal.SIGALRM),
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: child did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


@dataclass(frozen=True, slots=True)
class ChildRun:
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
    invocation_arguments: tuple[str, ...] = ISOLATED_INVOCATION_ARGUMENTS,
    protocol_descriptor: int = PROTOCOL_DESCRIPTOR,
) -> ChildRun:
    wrapper = driver_wrapper_source(driver_source)
    stdout_path = tmp_path / "stdout.bin"
    stderr_path = tmp_path / "stderr.bin"
    stdin_read, stdin_write = os.pipe()
    protocol_read, protocol_write = os.pipe()
    stdout_fd = os.open(
        stdout_path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    stderr_fd = os.open(
        stderr_path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    owned_descriptors = {
        stdin_read,
        stdin_write,
        protocol_read,
        protocol_write,
        stdout_fd,
        stderr_fd,
    }
    pid: int | None = None
    try:
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, *invocation_arguments, wrapper],
            {},
            file_actions=[
                (os.POSIX_SPAWN_DUP2, stdin_read, 0),
                (os.POSIX_SPAWN_DUP2, stdout_fd, 1),
                (os.POSIX_SPAWN_DUP2, stderr_fd, 2),
                (
                    os.POSIX_SPAWN_DUP2,
                    protocol_write,
                    protocol_descriptor,
                ),
            ],
        )
        for descriptor in (stdin_read, protocol_write, stdout_fd, stderr_fd):
            os.close(descriptor)
            owned_descriptors.remove(descriptor)
        payload = (
            request_transport_bytes(request)
            if request_bytes is None
            else request_bytes
        )
        owned_descriptors.remove(stdin_write)
        with os.fdopen(stdin_write, "wb") as stdin:
            stdin.write(payload)
        owned_descriptors.remove(protocol_read)
        with os.fdopen(protocol_read, "rb") as protocol:
            stream = read_protocol_stream(
                protocol,
                request_id_sha256=request_identity_digest(request),
                self_budgets=(
                    self_budgets or ExecutorSelfBudgets.unbudgeted()
                ),
            )
        waited_pid, exit_status = os.waitpid(pid, 0)
        assert waited_pid == pid
        pid = None
    finally:
        for descriptor in owned_descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if pid is not None:
            try:
                waited_pid, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
            else:
                if waited_pid == 0:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    os.waitpid(pid, 0)
    return ChildRun(
        stream=stream,
        exit_status=exit_status,
        stdout=stdout_path.read_bytes(),
        stderr=stderr_path.read_bytes(),
    )


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
    assert PROTOCOL_DESCRIPTOR == 3
    assert DRIVER_ENTRYPOINT_NAME == "dr_exec_main"
    assert DRIVER_SOURCE_BINDING == "DR_EXEC_DRIVER_SOURCE"
    assert ISOLATED_INVOCATION_ARGUMENTS == ("-I", "-c")


def test_a_child_spelling_both_literals_bare_runs_and_completes(
    tmp_path: Path,
) -> None:
    literal_driver = """
def dr_exec_main(request, emit):
    emit({
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {"echo": request["payload"]["echo"]},
    })
"""
    request = _request(echo="bare")
    run = _run_driver(
        literal_driver,
        request,
        tmp_path,
        invocation_arguments=("-I", "-c"),
        protocol_descriptor=3,
    )

    assert run.exited_cleanly
    assert run.stream.completed
    assert len(run.stream.outputs) == 1
    assert run.stream.outputs[0].payload == {"echo": "bare"}


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
    request = _request(count=1, echo="é中\U0001f600")
    run = _run_driver(ECHO_DRIVER, request, tmp_path)
    assert run.stream.completed
    assert run.stream.outputs[0].payload == {
        "index": 0,
        "echo": "é中\U0001f600",
    }


@pytest.mark.parametrize(
    ("request_bytes", "payload"),
    [
        pytest.param(
            b'{"payload":{"bool":true,"float":1.5,"int":-2,'
            b'"none":null,"text":"line\\n\\u00e9"},'
            b'"schema":"dr_exec.test_request","schema_version":1}',
            {
                "bool": True,
                "float": 1.5,
                "int": -2,
                "none": None,
                "text": "line\né",
            },
            id="strict-leaves",
        ),
        pytest.param(
            b'{"payload":{"items":[0,{"enabled":false},'
            b'["x",2.25]]},"schema":"dr_exec.test_request",'
            b'"schema_version":1}',
            {"items": [0, {"enabled": False}, ["x", 2.25]]},
            id="nested-containers",
        ),
    ],
)
def test_child_bootstrap_accepts_the_strict_canonical_identity_corpus(
    request_bytes: bytes,
    payload: Jsonable,
    tmp_path: Path,
) -> None:
    request = build_identity_document(
        schema="dr_exec.test_request",
        schema_version=1,
        payload=payload,
    )
    run = _run_driver(
        IDENTITY_ECHO_DRIVER,
        request,
        tmp_path,
        request_bytes=request_bytes,
    )

    assert run.exited_cleanly
    assert run.stream.completed
    assert run.stream.outputs == (request,)


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


@pytest.mark.parametrize(
    "emitted_expression",
    [
        pytest.param("None", id="non-object"),
        pytest.param(
            "{'schema_version': 1, 'payload': {}}",
            id="missing-schema",
        ),
        pytest.param(
            "{'schema': 's', 'payload': {}}",
            id="missing-schema-version",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': 1}",
            id="missing-payload",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': 1, 'payload': {}, 'extra': 0}",
            id="extra-field",
        ),
        pytest.param(
            "{'schema': 1, 'schema_version': 1, 'payload': {}}",
            id="non-string-schema",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': True, 'payload': {}}",
            id="boolean-schema-version",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': 1, 'payload': float('nan')}",
            id="non-finite-payload",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': 1, 'payload': {1, 2}}",
            id="unsupported-payload",
        ),
        pytest.param(
            "{'schema': 's', 'schema_version': 1, 'payload': {1: 'value'}}",
            id="non-string-payload-key",
        ),
    ],
)
def test_child_bootstrap_rejects_invalid_emitted_identity_values(
    emitted_expression: str,
    tmp_path: Path,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit({emitted_expression})
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


@pytest.mark.parametrize(
    "request_bytes",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"{", id="truncated"),
        pytest.param(b"[]", id="non-object"),
        pytest.param(
            b'{"payload":{},"schema_version":1}',
            id="missing-schema",
        ),
        pytest.param(
            b'{"payload":{},"schema":"s"}',
            id="missing-schema-version",
        ),
        pytest.param(
            b'{"schema":"s","schema_version":1}',
            id="missing-payload",
        ),
        pytest.param(
            b'{"payload":{},"schema":"s","schema_version":1,"extra":0}',
            id="extra-field",
        ),
        pytest.param(
            b'{"payload":{},"schema":1,"schema_version":1}',
            id="non-string-schema",
        ),
        pytest.param(
            b'{"payload":{},"schema":"s","schema_version":true}',
            id="boolean-schema-version",
        ),
        pytest.param(
            b'{"payload":NaN,"schema":"s","schema_version":1}',
            id="non-finite-payload",
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
    run = _run_driver(
        ECHO_DRIVER,
        _request(count=1, echo="expected"),
        tmp_path,
        request_bytes=request_transport_bytes(_request(count=1, echo="other")),
    )
    assert run.stream.failure is not None
    assert run.stream.failure.code == ProtocolFailureCode.ID_MISMATCH
    assert run.stream.outputs == ()
