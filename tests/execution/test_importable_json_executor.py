from __future__ import annotations

import asyncio
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from dr_serialize import Jsonable
from support.executor import job_for, run_thread_calls, trusted_python_target

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExecutorFailure,
    ExitedOutcome,
    FailureOwner,
    FiniteByteLimit,
    FiniteDurationLimit,
    FiniteOutput,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonExecutor,
    InProcessRecordReceipt,
    IsolatedHostPythonRuntime,
    JobId,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    ProcessExecutor,
    ProtocolFailedOutcome,
    RecordReceiptKind,
    StreamRetentionBudget,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.core.kinds import BudgetAxis
from dr_exec.recording.models import BudgetExceededOutcome

JOB_ID = JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))
ENTRY_POINT = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="echo",
)
MISSING_MODULE = ImportableEntryPoint(
    module_name="support.missing_in_process_entry_points",
    attribute_name="echo",
)
NOT_CALLABLE = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="NOT_CALLABLE",
)
RAISES = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="raise_error",
)
RETURN_NULL = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="return_null",
)
SLEEP_LONG = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="sleep_long",
)
IMPORT_FAIL = ImportableEntryPoint(
    module_name="support.in_process_import_fail",
    attribute_name="echo",
)
RAISE_SYSTEM_EXIT = ImportableEntryPoint(
    module_name="support.in_process_entry_points",
    attribute_name="raise_system_exit",
)

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)


def build_job(
    entry_point: ImportableEntryPoint = ENTRY_POINT,
    request: Jsonable | None = None,
    *,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    payload: Jsonable = {"question": 42} if request is None else request
    return build_in_process_importable_json_job(
        JOB_ID,
        entry_point,
        payload,
        budgets=budgets,
    )


def test_echo_round_trips_through_parse_importable_json_result() -> None:
    executor = ImportableJsonExecutor()
    completed = executor.run(build_job(request={"question": 42}))

    assert parse_importable_json_result(completed) == {
        "value": {"question": 42}
    }
    assert isinstance(completed.record_receipt, InProcessRecordReceipt)
    assert completed.record_receipt.kind is RecordReceiptKind.IN_PROCESS


def test_json_null_result_is_accepted() -> None:
    executor = ImportableJsonExecutor()
    completed = executor.run(
        build_job(entry_point=RETURN_NULL, request={"ignored": True})
    )

    assert parse_importable_json_result(completed) is None


def test_import_error_maps_to_protocol_failure_with_executor_owner() -> None:
    executor = ImportableJsonExecutor()
    completed = executor.run(build_job(entry_point=MISSING_MODULE))

    outcome = completed.result.outcome
    assert isinstance(outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_not_callable_maps_to_protocol_failure_with_executor_owner() -> None:
    executor = ImportableJsonExecutor()
    completed = executor.run(build_job(entry_point=NOT_CALLABLE))

    outcome = completed.result.outcome
    assert isinstance(outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_entry_point_exception_maps_to_nonzero_exit_with_payload_owner() -> (
    None
):
    executor = ImportableJsonExecutor()
    completed = executor.run(build_job(entry_point=RAISES))

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_wrong_target_raises_executor_failure() -> None:
    executor = ImportableJsonExecutor()

    with pytest.raises(ExecutorFailure):
        executor.run(job_for(trusted_python_target()))


@requires_macos
def test_process_executor_rejects_in_process_target(tmp_path: Path) -> None:
    runtime = IsolatedHostPythonRuntime(executable=Path(sys.executable))
    records = tmp_path / "records"
    records.mkdir()
    executor = ProcessExecutor(
        runtime=runtime,
        run_store=DirectoryRunStore(root=records),
    )

    with pytest.raises(ExecutorFailure):
        executor.run(build_job())


def test_non_empty_env_is_rejected_before_run() -> None:
    job = replace(
        build_in_process_importable_json_job(
            JOB_ID,
            ENTRY_POINT,
            {"ok": True},
        ),
        env=EnvGrant.fixed({"VISIBLE": "yes"}),
    )
    executor = ImportableJsonExecutor()

    with pytest.raises(DeclarationError):
        executor.run(job)


def test_finite_input_budget_is_enforced_before_run() -> None:
    job = build_in_process_importable_json_job(
        JOB_ID,
        ENTRY_POINT,
        {"too": "large"},
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=1)),
    )
    executor = ImportableJsonExecutor()

    with pytest.raises(DeclarationError):
        executor.run(job)


def test_pre_cancelled_token_returns_cancelled_outcome() -> None:
    executor = ImportableJsonExecutor()
    token = CancelToken()
    token.cancel()

    completed = executor.run(build_job(), cancellation=token)

    assert isinstance(completed.result.outcome, CancelledOutcome)
    assert completed.result.attribution.owner is FailureOwner.NONE


def test_wall_time_budget_exceeds_on_blocking_entry_point() -> None:
    job = build_job(
        entry_point=SLEEP_LONG,
        request={"ignored": True},
        budgets=Budgets(
            wall_time=FiniteDurationLimit(max_ns=50_000_000),
        ),
    )
    executor = ImportableJsonExecutor()
    completed = executor.run(job)

    assert isinstance(completed.result.outcome, BudgetExceededOutcome)
    assert completed.result.outcome.axis is BudgetAxis.WALL_TIME


def test_executor_is_safe_for_concurrent_calls() -> None:
    executor = ImportableJsonExecutor()
    jobs = tuple(
        build_in_process_importable_json_job(
            JobId(UUID(int=index + 10)),
            ENTRY_POINT,
            {"index": index},
        )
        for index in range(4)
    )
    results = run_thread_calls(
        (
            lambda job=job: parse_importable_json_result(executor.run(job))
            for job in jobs
        ),
        timeout=5.0,
    )

    assert results.errors == ()
    assert results.unfinished == 0
    assert sorted(
        cast("list[dict[str, dict[str, int]]]", list(results.values)),
        key=lambda value: value["value"]["index"],
    ) == [
        {"value": {"index": 0}},
        {"value": {"index": 1}},
        {"value": {"index": 2}},
        {"value": {"index": 3}},
    ]


def test_open_pool_streams_parsed_results() -> None:
    jobs = tuple(
        build_in_process_importable_json_job(
            JobId(UUID(int=index + 1)),
            ENTRY_POINT,
            {"index": index},
        )
        for index in range(2)
    )
    executor = ImportableJsonExecutor()

    async def run() -> list[tuple[str, object]]:
        async def submissions():
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=f"context-{index}")

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=2)
            )
        ) as pool:
            return [
                (
                    item.context,
                    parse_importable_json_result(item.completed_execution),
                )
                async for item in pool.run_stream(submissions())
            ]

    returned = asyncio.run(run())

    assert sorted(returned) == [
        ("context-0", {"value": {"index": 0}}),
        ("context-1", {"value": {"index": 1}}),
    ]


def test_system_exit_maps_to_exited_outcome_without_breaking_pool() -> None:
    exit_job = build_in_process_importable_json_job(
        JobId(UUID(int=1)),
        RAISE_SYSTEM_EXIT,
        {"ignored": True},
    )
    echo_job = build_in_process_importable_json_job(
        JobId(UUID(int=2)),
        ENTRY_POINT,
        {"index": 2},
    )
    executor = ImportableJsonExecutor()

    async def run() -> list[tuple[str, object, object | None]]:
        async def submissions():
            yield ExecutionSubmission(job=exit_job, context="exit")
            yield ExecutionSubmission(job=echo_job, context="echo")

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=1)
            )
        ) as pool:
            outcomes: list[tuple[str, object, object | None]] = []
            async for item in pool.run_stream(submissions()):
                outcomes.append(
                    (
                        item.context,
                        item.completed_execution.result.outcome,
                        parse_importable_json_result(item.completed_execution)
                        if item.context == "echo"
                        else None,
                    )
                )
            return outcomes

    returned = asyncio.run(run())

    assert len(returned) == 2
    exit_context, exit_outcome, _ = returned[0]
    echo_context, _, echo_value = returned[1]
    assert exit_context == "exit"
    assert echo_context == "echo"
    assert isinstance(exit_outcome, ExitedOutcome)
    assert exit_outcome.exit_code == 7
    assert echo_value == {"value": {"index": 2}}


def test_import_initialization_failure_maps_to_protocol_failure() -> None:
    executor = ImportableJsonExecutor()
    completed = executor.run(build_job(entry_point=IMPORT_FAIL))

    outcome = completed.result.outcome
    assert isinstance(outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_import_initialization_failure_does_not_break_pool() -> None:
    fail_job = build_in_process_importable_json_job(
        JobId(UUID(int=3)),
        IMPORT_FAIL,
        {"ignored": True},
    )
    echo_job = build_in_process_importable_json_job(
        JobId(UUID(int=4)),
        ENTRY_POINT,
        {"index": 4},
    )
    executor = ImportableJsonExecutor()

    async def run() -> list[tuple[str, object, object | None]]:
        async def submissions():
            yield ExecutionSubmission(job=fail_job, context="fail")
            yield ExecutionSubmission(job=echo_job, context="echo")

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=1)
            )
        ) as pool:
            values: list[tuple[str, object, object | None]] = []
            async for item in pool.run_stream(submissions()):
                values.append(
                    (
                        item.context,
                        item.completed_execution.result.outcome,
                        parse_importable_json_result(item.completed_execution)
                        if item.context == "echo"
                        else None,
                    )
                )
            return values

    returned = asyncio.run(run())

    fail_outcome = returned[0][1]
    echo_value = returned[1][2]
    assert isinstance(fail_outcome, ProtocolFailedOutcome)
    assert echo_value == {"value": {"index": 4}}


def test_caller_cancel_wins_over_wall_time_budget() -> None:
    job = build_job(
        entry_point=SLEEP_LONG,
        request={"seconds": 0.5},
        budgets=Budgets(
            wall_time=FiniteDurationLimit(max_ns=50_000_000),
        ),
    )
    executor = ImportableJsonExecutor()
    token = CancelToken()
    completed_holder: list[CompletedExecution] = []

    def run_job() -> None:
        completed_holder.append(
            executor.run(job, cancellation=token),
        )

    thread = threading.Thread(target=run_job)
    thread.start()
    time.sleep(0.01)
    token.cancel()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert len(completed_holder) == 1
    assert isinstance(completed_holder[0].result.outcome, CancelledOutcome)
    assert not isinstance(
        completed_holder[0].result.outcome, BudgetExceededOutcome
    )


def test_finite_payload_output_budget_is_rejected_before_run() -> None:
    job = build_in_process_importable_json_job(
        JOB_ID,
        ENTRY_POINT,
        {"ok": True},
        budgets=Budgets(
            payload_output=FiniteOutput(
                max_bytes=100,
                overflow_policy=OutputOverflowPolicy.MARKED_TRUNCATION,
                retention=PayloadRetentionBudget(
                    stdout=StreamRetentionBudget(head_bytes=100, tail_bytes=0),
                    stderr=StreamRetentionBudget(head_bytes=0, tail_bytes=0),
                ),
            ),
        ),
    )
    executor = ImportableJsonExecutor()

    with pytest.raises(DeclarationError, match="payload_output"):
        executor.run(job)
