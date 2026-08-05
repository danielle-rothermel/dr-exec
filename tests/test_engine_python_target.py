"""The single-run engine against real children, for the Python target.

Every case here spawns one real isolated interpreter through the engine's
own bootstrap, so what is exercised is the production path: the runtime's
fixed ``-I -c <wrapper>`` command, the canonical request on stdin through
EOF, the protected fd 3 stream, and the same containment, budget, teardown,
and recording lifecycle every other target follows.

Synchronization is on explicit gates, files, and terminal outcomes.
Nothing waits on elapsed time or treats its passage as evidence: a case
that must observe a child which is deliberately still running releases it
through a gate it created, and deadlines appear only as watchdogs.

macOS process semantics are what these cases rest on, so they are marked
and skipped off darwin; their passing on macOS is the qualification
evidence.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dr_serialize import IdentityDocument, build_identity_document

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
from dr_exec._bootstrap import DRIVER_ENTRYPOINT_NAME, PROTOCOL_DESCRIPTOR

if TYPE_CHECKING:
    from dr_exec.record import CompletedExecution

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)

# Watchdog only. It bounds a case that would otherwise hang the suite; no
# case asserts on it, and no case uses its non-expiry as evidence.
WATCHDOG_SECONDS = 60.0

# Watchdogs expressed as budgets, for cases whose child is deliberately
# immortal. The case asserts on the terminal outcome, never on timing.
WATCHDOG_WALL_TIME = FiniteDurationLimit(max_ns=10_000_000_000)
WATCHDOG_JOIN_TIME = FiniteDurationLimit(max_ns=10_000_000_000)

REQUEST_SCHEMA = "dr_exec.test_request"
OUTPUT_SCHEMA = "dr_exec.test_output"


def emit_call(payload: str, /) -> str:
    """Render one ``emit`` call whose payload expression is given."""
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
    """One temporary record root plus the executor that writes into it."""

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
def harness(tmp_path: Path) -> Harness:
    root = tmp_path / "records"
    root.mkdir()
    store = DirectoryRunStore(root=root)
    return Harness(
        executor=ProcessExecutor(
            runtime=IsolatedHostPythonRuntime(executable=Path(sys.executable)),
            run_store=store,
        ),
        store=store,
        root=root,
    )


@pytest.fixture(autouse=True)
def watchdog() -> object:
    """Fail a hung case instead of letting it hang the whole suite."""
    timer = threading.Timer(
        WATCHDOG_SECONDS,
        lambda: os.kill(os.getpid(), signal.SIGALRM),
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: the case did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


@dataclass(frozen=True, slots=True)
class Gate:
    """One FIFO the parent and child use to synchronize on an event.

    Opening a FIFO blocks in the kernel until the peer opens it, so a gate
    is real state synchronization with no spinning and no delay. The case's
    watchdog bounds a peer that never arrives.
    """

    path: Path

    @classmethod
    def create(cls, directory: Path, name: str, /) -> Gate:
        path = directory / name
        os.mkfifo(path)
        return cls(path=path)

    def receive(self) -> str:
        with self.path.open() as reader:
            return reader.read()

    def release(self, message: str = "go", /) -> None:
        with self.path.open("w") as writer:
            writer.write(message)


def record_dir_of(completed: CompletedExecution, /) -> Path:
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    return receipt.record_dir


def payloads_of(completed: CompletedExecution, /) -> list[object]:
    return [document.payload for document in completed.result.protocol_outputs]


def sole_mapping(completed: CompletedExecution, /) -> Mapping[str, object]:
    """Return the one output payload as a mapping the case can read.

    Drivers that report facts about their own process emit exactly one
    object payload; narrowing it once here keeps every such case reading
    named keys instead of repeating the same shape assertions.
    """
    (payload,) = payloads_of(completed)
    assert isinstance(payload, Mapping)
    return cast("Mapping[str, object]", payload)


def protocol_failure_of(
    completed: CompletedExecution, /
) -> ProtocolFailedOutcome:
    outcome = completed.result.outcome
    assert isinstance(outcome, ProtocolFailedOutcome)
    return outcome


# --- Complete streams ----------------------------------------------------


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
    """The prelude digest is computed by the child over the bytes it read.

    Accepting the stream therefore proves the canonical request arrived
    whole: a truncated or altered request could not produce a prelude that
    binds the parent's digest.
    """
    completed = harness.run(ECHO_DRIVER, count=1, echo="é中\U0001f600")

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert payloads_of(completed) == [{"index": 0, "echo": "é中\U0001f600"}]


@requires_macos
def test_recorded_input_bytes_are_the_canonical_request_length(
    harness: Harness,
) -> None:
    """Measurement is the canonical length, not a declared raw stdin."""
    from dr_exec._protocol import request_transport_bytes

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


# --- Isolation and containment -------------------------------------------


@requires_macos
def test_a_python_child_inherits_only_its_declared_transports(
    harness: Harness,
) -> None:
    """fd 3 is the one addition over a command target, and nothing else.

    The driver reports every descriptor it can stat, so this catches any
    parent descriptor that leaked past ``close_fds`` as well as the
    intended topology. Exactly four are live by the time domain code runs:
    the three payload streams plus the wrapper's own duplicate of the
    protected handle. The protected *number* is closed -- the wrapper
    duplicates fd 3 and closes the original before loading the driver --
    so the duplicate lands on the lowest free descriptor above the payload
    streams, and the payload can no longer reach the protected stream by
    the well-known number.
    """
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    live = []
    for fd in range(256):
        try:
            os.fstat(fd)
        except OSError:
            continue
        live.append(fd)
    {emit_call("{'live': live}")}
"""
    completed = harness.run(driver)

    live = sole_mapping(completed)["live"]
    assert isinstance(live, list)
    assert live[:3] == [0, 1, 2]
    assert len(live) == 4
    assert PROTOCOL_DESCRIPTOR not in live


@requires_macos
def test_the_payload_cannot_write_the_protected_stream_directly(
    harness: Harness,
) -> None:
    """The honest limit is stated in the design; this pins what holds.

    Domain code writing the well-known descriptor number cannot inject
    bytes into the protected stream, because the wrapper duplicated the
    handle and closed that number before loading the driver.
    """
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
def test_a_python_child_leads_a_fresh_session_and_process_group(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'pid': os.getpid(), 'pgid': os.getpgrp(), 'sid': os.getsid(0)}")}
"""
    completed = harness.run(driver)

    reported = sole_mapping(completed)
    assert reported["pid"] == reported["pgid"] == reported["sid"]
    assert reported["pgid"] != os.getpgrp()


@requires_macos
def test_a_python_child_runs_in_a_fresh_scratch_directory(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'cwd': os.getcwd()}")}
"""
    completed = harness.run(driver)

    assert not Path(str(sole_mapping(completed)["cwd"])).exists()


@requires_macos
def test_a_python_child_receives_only_the_granted_environment(
    harness: Harness,
) -> None:
    driver = f"""
import os


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    {emit_call("{'names': sorted(os.environ)}")}
"""
    os.environ["DR_EXEC_TEST_PYTHON_AMBIENT"] = "must not reach the child"
    try:
        completed = harness.run(
            driver, env=EnvGrant.fixed({"GRANTED": "value"})
        )
    finally:
        del os.environ["DR_EXEC_TEST_PYTHON_AMBIENT"]

    names = sole_mapping(completed)["names"]
    assert isinstance(names, list)
    assert "GRANTED" in names
    assert "DR_EXEC_TEST_PYTHON_AMBIENT" not in names


@requires_macos
def test_the_driver_source_never_reaches_argv_or_a_shell(
    harness: Harness,
) -> None:
    """Hostile source is embedded as data, so it cannot become syntax."""
    driver = f"""
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    marker = "$(echo pwned); `id`; '\\"\\\\"
    {emit_call("{'marker': marker, 'argv_len': len(sys.argv)}")}
"""
    completed = harness.run(driver)

    assert sole_mapping(completed)["marker"] == "$(echo pwned); `id`; '\"\\"


# --- Protocol failure taxonomy as outcome data ---------------------------


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
    """Payload-owned protocol failure is data, never a raise."""
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
    """The domain owns completeness; dr-exec discards nothing it accepted."""
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
    """The manifest carries the same outputs the result did, inline."""
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


# --- Protocol self-budget edges ------------------------------------------


@requires_macos
def test_an_output_count_budget_stops_the_stream_as_an_oversized_frame(
    harness: Harness,
) -> None:
    """An executor limit is attributed to the executor, not the payload.

    A self-budget stopping the stream is executor policy; classifying it
    as a payload fault would let an executor limit masquerade as a
    payload crash.
    """
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
    """The edge itself: the budget admits exactly what it declares."""
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
    """Explicitly unbudgeted means no executor cap, not a large one."""
    completed = harness.run(ECHO_DRIVER, count=64, echo="y" * 2048)

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert len(completed.result.protocol_outputs) == 64


# --- Workload budgets ----------------------------------------------------


@requires_macos
def test_an_over_budget_request_is_refused_before_any_spawn(
    harness: Harness,
) -> None:
    """The canonical request length is checked before a child exists."""
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
    """Pinned precedence: the budget names the failure, not its symptom.

    The engine's own teardown is what ends the stream, so reporting the
    resulting incomplete stream would blame the payload for the
    executor's deadline. The accepted output still survives.
    """
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


# --- Cancellation --------------------------------------------------------


@requires_macos
def test_pre_spawn_cancellation_records_without_launching_a_child(
    harness: Harness, tmp_path: Path
) -> None:
    marker = tmp_path / "the-driver-ran"
    token = CancelToken()
    token.cancel()

    driver = f"""
import pathlib


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    pathlib.Path({str(marker)!r}).write_text("ran")
"""
    completed = harness.run(driver, cancellation=token)

    assert completed.result.outcome == CancelledOutcome()
    assert completed.result.attribution.owner is FailureOwner.NONE
    assert not marker.exists()
    assert completed.result.protocol_outputs == ()
    record = harness.store.load(record_dir_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_post_spawn_cancellation_tears_down_and_returns_cancelled(
    harness: Harness, tmp_path: Path
) -> None:
    """The gate orders the two: the token is set while the child is alive.

    The canceller's read returns exactly when the driver announces itself,
    so cancellation is observed against a real running child rather than
    at some hoped-for moment.
    """
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


# --- Teardown ------------------------------------------------------------


@requires_macos
def test_teardown_reaches_a_descendant_the_driver_forked(
    harness: Harness, tmp_path: Path
) -> None:
    """A forked descendant in the group goes with the leader.

    The descendant announces its pid through a gate before either process
    blocks, so the pid checked here belongs to a process that provably
    existed and shared the group.
    """
    gate = Gate.create(tmp_path, "descendant")
    pids: list[int] = []
    collector = threading.Thread(
        target=lambda: pids.append(int(gate.receive())),
        daemon=True,
    )
    collector.start()

    driver = f"""
import os
import time


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    if os.fork() == 0:
        gate = open({str(gate.path)!r}, "w")
        gate.write(str(os.getpid()))
        gate.close()
        while True:
            time.sleep(3600)
    while True:
        time.sleep(3600)
"""
    completed = harness.run(
        driver,
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
        self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
    )
    collector.join()

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.WALL_TIME
    )
    (descendant,) = pids
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)


@requires_macos
def test_the_direct_child_is_reaped_so_no_zombie_remains(
    harness: Harness,
) -> None:
    completed = harness.run(ECHO_DRIVER)
    del completed

    # `ECHILD` is the kernel's own statement that every direct child of
    # this parent was reaped.
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


# --- Durable recording ---------------------------------------------------


@requires_macos
def test_the_record_carries_python_specific_durable_evidence(
    harness: Harness,
) -> None:
    """Request identity, containment profile, and runtime evidence."""
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
    """Secret-safe durable evidence: digests, never recoverable input."""
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
def test_the_python_target_finalizes_with_digest_matching_sidecars(
    harness: Harness,
) -> None:
    driver = f"""
import sys


def {DRIVER_ENTRYPOINT_NAME}(request, emit):
    sys.stdout.write("stdout evidence")
    sys.stderr.write("stderr evidence")
    {emit_call("{'ok': True}")}
"""
    completed = harness.run(driver)

    record_dir = record_dir_of(completed)
    record = harness.store.load(record_dir)
    assert isinstance(record, FinalizedRecord)
    stdout_path = record_dir / record.outputs.stdout.relative_path
    assert stdout_path.read_bytes() == b"stdout evidence"


# --- Concurrency ---------------------------------------------------------


@requires_macos
def test_concurrent_python_calls_keep_their_streams_separate(
    harness: Harness,
) -> None:
    """One executor, many threads: no stream crosses into another call."""
    call_count = 6
    completions: list[CompletedExecution] = []
    lock = threading.Lock()

    def run_one(index: int, /) -> None:
        completed = harness.run(ECHO_DRIVER, count=1, echo=f"call-{index}")
        with lock:
            completions.append(completed)

    threads = [
        threading.Thread(target=run_one, args=(index,))
        for index in range(call_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(completions) == call_count
    echoes = {sole_mapping(completed)["echo"] for completed in completions}
    assert echoes == {f"call-{index}" for index in range(call_count)}
    assert len({record_dir_of(c) for c in completions}) == call_count
