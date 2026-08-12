from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document

from dr_exec import (
    Budgets,
    CompleteRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutorFailure,
    ExecutorFailureCode,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FinalizedRecord,
    ImportableEntryPoint,
    InProcessImportableJsonTarget,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
    RecordState,
    RunRecordReference,
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


def reference_of(completed: CompletedExecution, /) -> RunRecordReference:
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    return receipt.reference


@requires_macos
@pytest.mark.integration
@pytest.mark.subprocess
@pytest.mark.platform_macos
def test_run_binds_the_finalized_manifest_to_the_returned_execution(
    executor: ProcessExecutor,
    store: DirectoryRunStore,
) -> None:
    completed = executor.run_blocking(job_for(trusted_target()))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt)
    assert receipt.latest_state is RecordState.FINALIZED
    assert receipt.execution_id == completed.result.execution_id
    record = store.load(reference_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert record.declaration.execution_id == completed.result.execution_id


def test_an_in_process_target_is_refused_before_declaration_validation(
    executor: ProcessExecutor,
) -> None:
    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=InProcessImportableJsonTarget(
            entry_point=ImportableEntryPoint(
                module_name="support.in_process_entry_points",
                attribute_name="echo",
            ),
            request=build_identity_document(
                schema="dr_exec.test_request",
                schema_version=1,
                payload={"echo": "ran"},
            ),
        ),
        # A grant the shared declaration gate refuses, so the reported code
        # shows which gate ran first.
        env=EnvGrant.fixed({"PATH": "/usr/bin"}),
        budgets=Budgets.unbudgeted(),
    )

    with pytest.raises(ExecutorFailure) as raised:
        executor.run_blocking(job)

    assert raised.value.code is ExecutorFailureCode.TARGET_NOT_SUPPORTED


def test_run_defaults_to_unbudgeted_self_budgets(
    store: DirectoryRunStore,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    executor = ProcessExecutor(
        runtime=host_runtime,
        run_store=store,
    )

    assert executor.self_budgets == ExecutorSelfBudgets.unbudgeted()
