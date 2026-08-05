from __future__ import annotations

import os
import sys
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dr_serialize import IdentityDocument, build_identity_document
from support.process import Gate, finish_threaded_calls, start_threaded_calls

import dr_exec.execution.engine
from dr_exec import (
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompleteRecordReceipt,
    ContainmentProfile,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutorFailure,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FailureOwner,
    FinalizedRecord,
    FiniteByteLimit,
    FiniteCountLimit,
    FiniteDurationLimit,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
    ProtocolFailedOutcome,
    ProtocolFailureCode,
    RecordState,
    UntrustedPythonTarget,
    UntrustedPythonTargetRecord,
)
from dr_exec.runtime.bootstrap import (
    DRIVER_ENTRYPOINT_NAME,
    PROTOCOL_DESCRIPTOR,
)

if TYPE_CHECKING:
    from dr_exec.recording.models import CompletedExecution

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.subprocess,
    pytest.mark.platform_macos,
    pytest.mark.usefixtures("process_watchdog"),
]

WATCHDOG_WALL_TIME = FiniteDurationLimit(max_ns=10_000_000_000)
WATCHDOG_JOIN_TIME = FiniteDurationLimit(max_ns=10_000_000_000)

REQUEST_SCHEMA = "dr_exec.test_request"
OUTPUT_SCHEMA = "dr_exec.test_output"


def emit_call(payload: str, /) -> str:
    return (
        "emit({"
        f"'schema': {OUTPUT_SCHEMA!r}, "
        "'schema_version': 1, "
        f"'payload': {payload}"
        "})"
    )


ECHO_DRIVER = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    for index in range(request["payload"]["count"]):
        {emit_call("{'index': index, 'echo': request['payload']['echo']}")}
"""


@dataclass(frozen=True, slots=True)
class Harness:
    executor: ProcessExecutor
    store: DirectoryRunStore
    root: Path

    def run(
        self,
        driver_source: str,
        /,
        *,
        count: int = 1,
        echo: str = "value",
        request: IdentityDocument | None = None,
        env: EnvGrant | None = None,
        budgets: Budgets | None = None,
        self_budgets: ExecutorSelfBudgets | None = None,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        executor = (
            self.executor
            if self_budgets is None
            else ProcessExecutor(
                runtime=self.executor.runtime,
                run_store=self.executor.run_store,
                self_budgets=self_budgets,
            )
        )
        return executor.run(
            self.job(
                driver_source,
                count=count,
                echo=echo,
                request=request,
                env=env,
                budgets=budgets,
            ),
            cancellation=cancellation,
        )

    def job(
        self,
        driver_source: str,
        /,
        *,
        count: int = 1,
        echo: str = "value",
        request: IdentityDocument | None = None,
        env: EnvGrant | None = None,
        budgets: Budgets | None = None,
    ) -> ExecutionJob:
        return ExecutionJob(
            job_id=JobId(uuid4()),
            target=UntrustedPythonTarget(
                driver_source=driver_source,
                request=(
                    build_identity_document(
                        schema=REQUEST_SCHEMA,
                        schema_version=1,
                        payload={"count": count, "echo": echo},
                    )
                    if request is None
                    else request
                ),
                containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
            ),
            env=env if env is not None else EnvGrant.none(),
            budgets=budgets if budgets is not None else Budgets.unbudgeted(),
        )


@pytest.fixture
def harness(
    tmp_path: Path, host_runtime: IsolatedHostPythonRuntime
) -> Harness:
    root = tmp_path / "records"
    root.mkdir()
    store = DirectoryRunStore(root=root)
    return Harness(
        executor=ProcessExecutor(
            runtime=host_runtime,
            run_store=store,
        ),
        store=store,
        root=root,
    )


def record_dir_of(completed: CompletedExecution, /) -> Path:
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    return receipt.record_dir


def payloads_of(completed: CompletedExecution, /) -> list[object]:
    return [document.payload for document in completed.result.protocol_outputs]


def sole_mapping(completed: CompletedExecution, /) -> Mapping[str, object]:
    (payload,) = payloads_of(completed)
    assert isinstance(payload, Mapping)
    return cast("Mapping[str, object]", payload)


def protocol_failure_of(
    completed: CompletedExecution, /
) -> ProtocolFailedOutcome:
    outcome = completed.result.outcome
    assert isinstance(outcome, ProtocolFailedOutcome)
    return outcome


@requires_macos
def test_a_python_target_returns_its_outputs_and_a_clean_exit(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER, count=3, echo="hello")

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.attribution.owner is FailureOwner.NONE
    assert payloads_of(completed) == [
        {"index": index, "echo": "hello"} for index in range(3)
    ]


@requires_macos
def test_a_driver_emitting_nothing_completes_with_no_outputs(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER, count=0)

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.protocol_outputs == ()


@requires_macos
def test_the_request_reaches_the_child_intact_through_eof(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER, count=1, echo="é中\U0001f600")

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert payloads_of(completed) == [{"index": 0, "echo": "é中\U0001f600"}]


@requires_macos
def test_recorded_input_bytes_are_the_canonical_request_length(
    harness: Harness,
) -> None:
    from dr_exec.declarations.transport import request_transport_bytes

    request = build_identity_document(
        schema=REQUEST_SCHEMA,
        schema_version=1,
        payload={"count": 0, "echo": "measured"},
    )
    completed = harness.run(ECHO_DRIVER, request=request)

    assert completed.result.measurements.input_bytes == len(
        request_transport_bytes(request)
    )


@requires_macos
def test_protocol_bytes_are_counted_apart_from_payload_output(
    harness: Harness,
) -> None:
    driver = f"""
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    sys.stdout.write("payload stdout")
    sys.stderr.write("payload stderr")
    {emit_call("{'ok': True}")}
"""
    completed = harness.run(driver)

    assert completed.result.payload_outputs.stdout.head == b"payload stdout"
    assert completed.result.payload_outputs.stderr.head == b"payload stderr"
    assert completed.result.measurements.protocol_bytes_received > 0
    assert b"prelude" not in (
        completed.result.payload_outputs.stdout.head
        + completed.result.payload_outputs.stderr.head
    )


@requires_macos
def test_python_wrapper_closes_protected_fd_before_domain_code(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    live = {{}}
    for fd in range(4):
        try:
            os.fstat(fd)
        except OSError:
            live[str(fd)] = False
        else:
            live[str(fd)] = True
    {emit_call("{'live': live}")}
"""
    completed = harness.run(driver)

    live = sole_mapping(completed)["live"]
    assert live == {"0": True, "1": True, "2": True, "3": False}


@requires_macos
def test_the_payload_cannot_write_the_protected_stream_directly(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    try:
        os.write({PROTOCOL_DESCRIPTOR}, b"tamper\\n")
        reached = True
    except OSError:
        reached = False
    {emit_call("{'reached': reached}")}
"""
    completed = harness.run(driver)

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert payloads_of(completed) == [{"reached": False}]


@requires_macos
def test_the_protected_handle_survives_replaced_language_level_streams(
    harness: Harness,
) -> None:
    driver = f"""
import io
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    sys.stdin = io.StringIO()
    {emit_call("{'survived': True}")}
"""
    completed = harness.run(driver)

    assert payloads_of(completed) == [{"survived": True}]


@requires_macos
def test_driver_source_is_not_a_payload_argument_or_shell_syntax(
    harness: Harness,
) -> None:
    driver = f"""
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    marker = "$(echo pwned); `id`; '\\"\\\\"
    {emit_call("{'marker': marker, 'argv': sys.argv}")}
"""
    completed = harness.run(driver)

    reported = sole_mapping(completed)
    assert reported["marker"] == "$(echo pwned); `id`; '\"\\"
    assert reported["argv"] == ["-c"]


@requires_macos
def test_a_started_protocol_worker_failure_raises_after_lifecycle_cleanup(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:

    def failing_read(*_: object, **__: object) -> None:
        raise RuntimeError("synthetic protocol failure")

    monkeypatch.setattr(
        dr_exec.execution.engine, "read_protocol_stream", failing_read
    )
    before = len(os.listdir("/dev/fd"))
    driver = f"""
import time


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    while True:
        time.sleep(3600)
"""

    with pytest.raises(
        ExecutorFailure, match="protocol transport worker"
    ) as exc:
        harness.run(
            driver,
            budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
            self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
        )

    assert isinstance(exc.value.__cause__, RuntimeError)
    assert len(os.listdir("/dev/fd")) == before
    (record_dir,) = harness.root.iterdir()
    assert harness.store.load(record_dir).state is RecordState.RUNNING
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
@pytest.mark.parametrize(
    "driver",
    [
        pytest.param("", id="missing-entrypoint"),
        pytest.param(f"{DRIVER_ENTRYPOINT_NAME} = 3", id="non-callable"),
        pytest.param("raise RuntimeError('load')", id="source-load-failure"),
        pytest.param("this is not python", id="syntax-error"),
    ],
)
def test_a_payload_owned_driver_failure_is_an_incomplete_stream_outcome(
    harness: Harness, driver: str
) -> None:
    completed = harness.run(driver)

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.INCOMPLETE_STREAM
    assert outcome.accepted_output_count == 0
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    assert isinstance(completed.record_receipt, CompleteRecordReceipt)


@requires_macos
def test_a_later_failure_preserves_every_previously_accepted_output(
    harness: Harness,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'index': 0}")}
    {emit_call("{'index': 1}")}
    raise RuntimeError("driver failed midway")
"""
    completed = harness.run(driver)

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.INCOMPLETE_STREAM
    assert outcome.accepted_output_count == 2
    assert payloads_of(completed) == [{"index": 0}, {"index": 1}]


@requires_macos
def test_accepted_outputs_survive_a_driver_that_exits_the_process(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'index': 0}")}
    os._exit(7)
"""
    completed = harness.run(driver)

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.INCOMPLETE_STREAM
    assert payloads_of(completed) == [{"index": 0}]


@requires_macos
def test_a_driver_emitting_a_non_identity_document_fails_the_stream(
    harness: Harness,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    emit({{"not": "an identity document"}})
"""
    completed = harness.run(driver)

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.INCOMPLETE_STREAM
    assert completed.result.protocol_outputs == ()


@requires_macos
def test_a_protocol_failure_is_recorded_with_its_accepted_outputs(
    harness: Harness,
) -> None:
    driver = f"""
def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'index': 0}")}
    raise RuntimeError("midway")
"""
    completed = harness.run(driver)

    record = harness.store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert [
        document.payload for document in record.result.protocol_outputs
    ] == [{"index": 0}]


@requires_macos
def test_an_output_count_budget_stops_the_stream_as_an_oversized_frame(
    harness: Harness,
) -> None:
    completed = harness.run(
        ECHO_DRIVER,
        count=5,
        self_budgets=ExecutorSelfBudgets(
            protocol_output_count=FiniteCountLimit(max_count=2),
            join_time=WATCHDOG_JOIN_TIME,
        ),
    )

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.OVERSIZED_FRAME
    assert outcome.accepted_output_count == 2
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


@requires_macos
def test_an_output_count_exactly_at_its_budget_completes(
    harness: Harness,
) -> None:
    completed = harness.run(
        ECHO_DRIVER,
        count=2,
        self_budgets=ExecutorSelfBudgets(
            protocol_output_count=FiniteCountLimit(max_count=2)
        ),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert len(completed.result.protocol_outputs) == 2


@requires_macos
def test_a_frame_byte_budget_refuses_an_oversized_frame(
    harness: Harness,
) -> None:
    completed = harness.run(
        ECHO_DRIVER,
        count=1,
        echo="x" * 4096,
        self_budgets=ExecutorSelfBudgets(
            protocol_frame_bytes=FiniteByteLimit(max_bytes=256),
            join_time=WATCHDOG_JOIN_TIME,
        ),
    )

    outcome = protocol_failure_of(completed)
    assert outcome.failure_code is ProtocolFailureCode.OVERSIZED_FRAME


@requires_macos
def test_an_unbudgeted_protocol_axis_installs_no_hidden_limit(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER, count=64, echo="y" * 2048)

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert len(completed.result.protocol_outputs) == 64


@requires_macos
def test_an_over_budget_request_is_refused_before_any_spawn(
    harness: Harness,
) -> None:
    with pytest.raises(DeclarationError, match="input budget"):
        harness.run(
            ECHO_DRIVER,
            echo="z" * 4096,
            budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=16)),
        )

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_wall_time_overflow_beats_the_incomplete_stream_it_causes(
    harness: Harness,
) -> None:
    driver = f"""
import time


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'index': 0}")}
    while True:
        time.sleep(3600)
"""
    completed = harness.run(
        driver,
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
        self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.WALL_TIME
    )
    assert payloads_of(completed) == [{"index": 0}]


@requires_macos
def test_post_spawn_cancellation_tears_down_and_returns_cancelled(
    harness: Harness, tmp_path: Path
) -> None:
    gate = Gate.create(tmp_path, "started")
    token = CancelToken()
    canceller = threading.Thread(
        target=lambda: (gate.receive(), token.cancel()),
        daemon=True,
    )
    canceller.start()

    driver = f"""
import time


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'index': 0}")}
    gate = open({str(gate.path)!r}, "w")
    gate.write("started")
    gate.close()
    while True:
        time.sleep(3600)
"""
    completed = harness.run(
        driver,
        budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
        cancellation=token,
    )
    canceller.join()

    assert completed.result.outcome == CancelledOutcome()
    assert payloads_of(completed) == [{"index": 0}]
    record = harness.store.load(record_dir_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_the_record_carries_python_specific_durable_evidence(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER)

    record = harness.store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    target = record.declaration.target
    assert isinstance(target, UntrustedPythonTargetRecord)
    assert len(target.request_id_sha256) == 64
    assert len(target.canonical_declaration_sha256) == 64
    assert target.containment_profile is (
        ContainmentProfile.PROCESS_BOUNDARY_ONLY
    )
    assert target.runtime == harness.executor.runtime.describe()


@requires_macos
def test_the_record_never_exposes_the_driver_source_or_the_request(
    harness: Harness,
) -> None:
    secret_source = f"""
SECRET_LITERAL = "a-secret-in-the-driver"


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'ok': True}")}
"""
    completed = harness.run(secret_source, echo="a-secret-in-the-request")

    manifest = (record_dir_of(completed) / "record.json").read_text()
    assert "a-secret-in-the-driver" not in manifest
    assert "a-secret-in-the-request" not in manifest
    assert "SECRET_LITERAL" not in manifest


@requires_macos
def test_accepted_outputs_are_recorded_inline_not_as_digests(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER, count=2, echo="inline")

    record = harness.store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert [
        document.payload for document in record.result.protocol_outputs
    ] == [{"index": index, "echo": "inline"} for index in range(2)]


@requires_macos
def test_concurrent_python_calls_keep_their_streams_separate(
    harness: Harness, tmp_path: Path
) -> None:
    call_count = 6
    callers_ready = threading.Barrier(call_count + 1)
    arrivals = tuple(
        Gate.create(tmp_path, f"python-arrival-{index}")
        for index in range(call_count)
    )
    releases = tuple(
        Gate.create(tmp_path, f"python-release-{index}")
        for index in range(call_count)
    )
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    payload = request["payload"]
    with open(payload["arrival"], "w") as gate:
        gate.write(payload["call"])
    open(payload["release"]).read()
    {emit_call("{'call': payload['call'], 'cwd': os.getcwd(), 'pid': os.getpid()}")}
"""

    def run_one(index: int, /) -> CompletedExecution:
        callers_ready.wait()
        request = build_identity_document(
            schema=REQUEST_SCHEMA,
            schema_version=1,
            payload={
                "arrival": str(arrivals[index].path),
                "call": f"call-{index}",
                "release": str(releases[index].path),
            },
        )
        return harness.run(
            driver,
            request=request,
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        )

    calls = start_threaded_calls(
        tuple(
            lambda index=index: run_one(index) for index in range(call_count)
        )
    )
    try:
        callers_ready.wait()
        assert tuple(gate.receive() for gate in arrivals) == tuple(
            f"call-{index}" for index in range(call_count)
        )
    finally:
        for gate in releases:
            gate.release()
        completions = finish_threaded_calls(calls)

    outputs = []
    for index, completed in enumerate(completions):
        assert completed.result.outcome == ExitedOutcome(exit_code=0)
        assert completed.result.attribution.owner is FailureOwner.NONE
        assert completed.result.payload_outputs.stdout.head == b""
        assert completed.result.payload_outputs.stderr.head == b""
        assert completed.result.measurements.protocol_bytes_received > 0
        payload = sole_mapping(completed)
        assert payload["call"] == f"call-{index}"
        outputs.append(payload)
    assert len({str(output) for output in outputs}) == call_count
    assert len({output["cwd"] for output in outputs}) == call_count
    assert len({output["pid"] for output in outputs}) == call_count
    assert len({record_dir_of(c) for c in completions}) == call_count
    assert (
        len(
            {
                completed.result.execution_id.attempt_id
                for completed in completions
            }
        )
        == call_count
    )
