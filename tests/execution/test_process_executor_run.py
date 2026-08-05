"""The unique production ``ProcessExecutor.run()`` integration evidence.

The shared executor conformance suite owns target dispatch, cancellation,
validation, and receipt behavior. This file retains only the production
manifest/result identity binding that no other execution test owns, plus
the constructor's no-spawn default-field contract.

Scheduling is not in scope here: ``run_many`` and ``open_pool`` are the
same executor under the pool's scheduler core, qualified in
``test_execution_pool.py`` and ``test_pool_real_engine.py``.

Synchronization is on terminal outcomes and committed store state; every
case carries a watchdog so a hung child cannot hang the suite.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from dr_exec import (
    Budgets,
    CompleteRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FinalizedRecord,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
    RecordState,
    TrustedCommandTarget,
)

if TYPE_CHECKING:
    from dr_exec.recording.models import CompletedExecution

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)


@pytest.fixture
def store(tmp_path: Path) -> DirectoryRunStore:
    root = tmp_path / "records"
    root.mkdir()
    return DirectoryRunStore(root=root)


@pytest.fixture
def executor(
    store: DirectoryRunStore,
    host_runtime: IsolatedHostPythonRuntime,
) -> ProcessExecutor:
    return ProcessExecutor(
        runtime=host_runtime,
        run_store=store,
    )


def announcing_command() -> tuple[str, ...]:
    return (
        sys.executable,
        "-I",
        "-c",
        "import sys; sys.stdout.write('ran')",
    )


def trusted_target() -> TrustedCommandTarget:
    return TrustedCommandTarget(argv=announcing_command())


def job_for(target: TrustedCommandTarget, /) -> ExecutionJob:
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


# --- Production identity binding ----------------------------------------


@requires_macos
@pytest.mark.integration
@pytest.mark.subprocess
@pytest.mark.platform_macos
def test_run_binds_the_finalized_manifest_to_the_returned_execution(
    executor: ProcessExecutor,
    store: DirectoryRunStore,
) -> None:
    """The durable declaration and returned result name one execution."""
    completed = executor.run(job_for(trusted_target()))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    assert receipt.latest_state is RecordState.FINALIZED
    assert receipt.execution_id == completed.result.execution_id
    record = store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert record.declaration.execution_id == completed.result.execution_id


def test_run_defaults_to_unbudgeted_self_budgets(
    store: DirectoryRunStore,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    """The declared default is explicit unbudgeted, not an implicit cap."""
    executor = ProcessExecutor(
        runtime=host_runtime,
        run_store=store,
    )

    assert executor.self_budgets == ExecutorSelfBudgets.unbudgeted()
