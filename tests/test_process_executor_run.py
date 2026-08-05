"""``ProcessExecutor.run()`` end to end, for every declared target kind.

These cases enter through the public executor rather than the private
engine entry point, so what they qualify is the cutover itself: full
validation, prepare, spawn, outcome, and a finalized record plus receipt,
returning one ``CompletedExecution`` for a trusted command, an untrusted
command, and an untrusted Python target against a real directory store.

The PR 3 surface -- ``run_many``, ``open_pool``, and the pool scheduler --
stays stubbed, and one case pins that it still raises rather than
half-working.

Synchronization is on terminal outcomes and committed store state; every
case carries a watchdog so a hung child cannot hang the suite.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompleteRecordReceipt,
    ContainmentProfile,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutionTarget,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FinalizedRecord,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
    RecordState,
    TrustedCommandTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)

if TYPE_CHECKING:
    from dr_exec.record import CompletedExecution

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)

WATCHDOG_SECONDS = 60.0

DRIVER = """
def dr_exec_main(request, emit):
    emit({
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {"echo": request["payload"]["echo"]},
    })
"""


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


@pytest.fixture
def store(tmp_path: Path) -> DirectoryRunStore:
    root = tmp_path / "records"
    root.mkdir()
    return DirectoryRunStore(root=root)


@pytest.fixture
def executor(store: DirectoryRunStore) -> ProcessExecutor:
    return ProcessExecutor(
        runtime=IsolatedHostPythonRuntime(executable=Path(sys.executable)),
        run_store=store,
    )


def announcing_command() -> tuple[str, ...]:
    return (
        sys.executable,
        "-I",
        "-c",
        "import sys; sys.stdout.write('ran')",
    )


def trusted_target() -> ExecutionTarget:
    return TrustedCommandTarget(argv=announcing_command())


def untrusted_command_target() -> ExecutionTarget:
    return UntrustedCommandTarget(
        argv=announcing_command(),
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )


def python_target(echo: str = "ran", /) -> ExecutionTarget:
    return UntrustedPythonTarget(
        driver_source=DRIVER,
        request=build_identity_document(
            schema="dr_exec.test_request",
            schema_version=1,
            payload={"echo": echo},
        ),
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )


def job_for(target: ExecutionTarget, /) -> ExecutionJob:
    return ExecutionJob(
        job_id=JobId(uuid4()),
        target=target,
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )


def record_dir_of(completed: CompletedExecution, /) -> Path:
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    return receipt.record_dir


# --- The cutover, for every declared target kind -------------------------


@requires_macos
@pytest.mark.parametrize(
    "target_factory",
    [
        pytest.param(trusted_target, id="trusted-command"),
        pytest.param(untrusted_command_target, id="untrusted-command"),
        pytest.param(python_target, id="untrusted-python"),
    ],
)
def test_run_returns_a_completion_and_a_finalized_record(
    executor: ProcessExecutor,
    store: DirectoryRunStore,
    target_factory: Callable[[], ExecutionTarget],
) -> None:
    """One sequence for every kind: validate, prepare, spawn, finalize."""
    completed = executor.run(job_for(target_factory()))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    assert receipt.latest_state is RecordState.FINALIZED
    assert receipt.execution_id == completed.result.execution_id
    record = store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert record.declaration.execution_id == completed.result.execution_id


@requires_macos
def test_run_carries_target_specific_evidence_into_the_result(
    executor: ProcessExecutor,
) -> None:
    """Commands deliver payload bytes; Python delivers protocol outputs."""
    command = executor.run(job_for(trusted_target()))
    python = executor.run(job_for(python_target("through-run")))

    assert command.result.payload_outputs.stdout.head == b"ran"
    assert command.result.protocol_outputs == ()
    assert [
        document.payload for document in python.result.protocol_outputs
    ] == [{"echo": "through-run"}]


@requires_macos
def test_run_accepts_a_cancellation_token_for_every_target_kind(
    executor: ProcessExecutor, store: DirectoryRunStore
) -> None:
    """Pre-spawn cancellation is the shared conformance case.

    A token already cancelled cannot race the spawn, so this is the one
    cancellation behavior that is deterministic for every kind.
    """
    for factory in (
        trusted_target,
        untrusted_command_target,
        python_target,
    ):
        token = CancelToken()
        token.cancel()

        completed = executor.run(job_for(factory()), cancellation=token)

        assert completed.result.outcome == CancelledOutcome()
        assert store.load(record_dir_of(completed)).state is (
            RecordState.FINALIZED
        )


@requires_macos
def test_run_defaults_to_unbudgeted_self_budgets(
    store: DirectoryRunStore,
) -> None:
    """The declared default is explicit unbudgeted, not an implicit cap."""
    executor = ProcessExecutor(
        runtime=IsolatedHostPythonRuntime(executable=Path(sys.executable)),
        run_store=store,
    )

    assert executor.self_budgets == ExecutorSelfBudgets.unbudgeted()
    completed = executor.run(job_for(python_target()))
    assert completed.result.outcome == ExitedOutcome(exit_code=0)


def test_run_validates_the_declaration_before_anything_durable(
    executor: ProcessExecutor, store: DirectoryRunStore
) -> None:
    """An invalid declaration raises rather than producing an outcome.

    Platform refusal holds off darwin too, so this case needs no mark.
    """
    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=TrustedCommandTarget(argv=("dr-exec-test-relative",)),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )

    with pytest.raises(DeclarationError):
        executor.run(job)

    assert list(store.root.iterdir()) == []
