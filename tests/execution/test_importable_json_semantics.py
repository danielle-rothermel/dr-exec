"""Semantic conformance shared by both trusted importable-JSON executors.

The shared `Executor` fixture in `tests/capabilities` is parameterized over
command and process-Python declarations, which an importable-JSON executor
cannot accept. This suite is the semantic conformance baseline in its place:
every case here must hold for the in-process executor and for the worker pool.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import cast
from uuid import UUID

import pytest
from dr_serialize import Jsonable
from support.executor import job_for, trusted_python_target
from support.importable_json import (
    ECHO,
    ECHO_OR_RAISE,
    HARNESSES,
    IMPORT_FAIL,
    MISSING_MODULE,
    NOT_CALLABLE,
    RAISES,
    RETURN_NON_JSON,
    RETURN_NULL,
    ExecutorHarness,
    PooledExecutor,
)

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    DeclarationError,
    EnvGrant,
    ExecutionJob,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExecutorFailure,
    ExitedOutcome,
    FailureOwner,
    FiniteByteLimit,
    FiniteOutput,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonExecutor,
    InProcessRecordReceipt,
    JobId,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    ProtocolFailedOutcome,
    StreamRetentionBudget,
    WorkerPoolImportableJsonExecutor,
    WorkerPoolRecordReceipt,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)

JOB_ID = JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))


@pytest.fixture(params=HARNESSES, ids=lambda harness: harness.name)
def harness(request: pytest.FixtureRequest) -> ExecutorHarness:
    return cast("ExecutorHarness", request.param)


def build_job(
    entry_point: ImportableEntryPoint = ECHO,
    request: Jsonable | None = None,
    *,
    job_id: JobId = JOB_ID,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    payload: Jsonable = {"question": 42} if request is None else request
    return build_in_process_importable_json_job(
        job_id,
        entry_point,
        payload,
        budgets=budgets,
    )


def run_one(
    harness: ExecutorHarness,
    entry_point: ImportableEntryPoint,
    /,
    *,
    request: Jsonable | None = None,
    budgets: Budgets | None = None,
    cancellation: CancelToken | None = None,
) -> CompletedExecution:
    with harness.open(entry_point, workers=1) as executor:
        return executor.run_blocking(
            build_job(entry_point, request, budgets=budgets),
            cancellation=cancellation,
        )


def test_echo_round_trips_through_parse_importable_json_result(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, ECHO, request={"question": 42})

    assert parse_importable_json_result(completed) == {
        "value": {"question": 42}
    }


def test_each_executor_stamps_its_own_receipt_kind(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, ECHO)

    assert harness.receipt_is_own(completed.record_receipt)
    assert (
        completed.record_receipt.execution_id == completed.result.execution_id
    )


def test_the_two_executors_stamp_the_two_record_less_receipts() -> None:
    # The shared completion builder takes the receipt already constructed, so
    # which executor built a completion stays readable from its receipt.
    in_process = ImportableJsonExecutor().run_blocking(build_job(ECHO, {}))
    with WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=1
    ) as pool:
        pooled = pool.run_blocking(build_job(ECHO, {}))

    assert isinstance(in_process.record_receipt, InProcessRecordReceipt)
    assert isinstance(pooled.record_receipt, WorkerPoolRecordReceipt)


def test_json_null_result_is_accepted(harness: ExecutorHarness) -> None:
    completed = run_one(harness, RETURN_NULL, request={"ignored": True})

    assert parse_importable_json_result(completed) is None


def test_import_error_maps_to_protocol_failure_with_executor_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, MISSING_MODULE)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_not_callable_maps_to_protocol_failure_with_executor_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, NOT_CALLABLE)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_import_initialization_failure_maps_to_protocol_failure(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, IMPORT_FAIL)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_entry_point_exception_maps_to_nonzero_exit_with_payload_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, RAISES)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_non_json_return_maps_to_protocol_failure_with_payload_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, RETURN_NON_JSON)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_wrong_target_raises_executor_failure(
    harness: ExecutorHarness,
) -> None:
    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(ExecutorFailure),
    ):
        executor.run_blocking(job_for(trusted_python_target()))


def test_non_empty_env_is_rejected_before_run(
    harness: ExecutorHarness,
) -> None:
    job = replace(build_job(), env=EnvGrant.fixed({"VISIBLE": "yes"}))

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError),
    ):
        executor.run_blocking(job)


def test_finite_input_budget_is_enforced_before_run(
    harness: ExecutorHarness,
) -> None:
    job = build_job(
        request={"too": "large"},
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=1)),
    )

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError),
    ):
        executor.run_blocking(job)


def test_finite_payload_output_budget_is_rejected_before_run(
    harness: ExecutorHarness,
) -> None:
    job = build_job(
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

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError, match="payload_output"),
    ):
        executor.run_blocking(job)


def test_pre_cancelled_token_returns_cancelled_outcome(
    harness: ExecutorHarness,
) -> None:
    token = CancelToken()
    token.cancel()

    completed = run_one(harness, ECHO, cancellation=token)

    assert isinstance(completed.result.outcome, CancelledOutcome)
    assert completed.result.attribution.owner is FailureOwner.NONE


def test_an_unbudgeted_job_installs_no_deadline(
    harness: ExecutorHarness,
) -> None:
    """An unbudgeted job is never stopped by the executor itself."""

    completed = run_one(harness, ECHO, budgets=Budgets.unbudgeted())

    assert parse_importable_json_result(completed) == {
        "value": {"question": 42}
    }


def test_concurrent_calls_each_return_their_own_result(
    harness: ExecutorHarness,
) -> None:
    jobs = tuple(
        build_job(ECHO, {"index": index}, job_id=JobId(UUID(int=index + 10)))
        for index in range(4)
    )

    with harness.open(ECHO, workers=4) as executor:
        returned = _run_pool(executor, jobs, capacity=4)

    assert sorted(
        cast("list[dict[str, dict[str, int]]]", returned),
        key=lambda value: value["value"]["index"],
    ) == [{"value": {"index": index}} for index in range(4)]


def test_open_pool_streams_parsed_results(harness: ExecutorHarness) -> None:
    jobs = tuple(
        build_job(ECHO, {"index": index}, job_id=JobId(UUID(int=index + 1)))
        for index in range(2)
    )

    with harness.open(ECHO, workers=2) as executor:
        returned = _run_pool(executor, jobs, capacity=2)

    assert sorted(
        cast("list[dict[str, dict[str, int]]]", returned),
        key=lambda value: value["value"]["index"],
    ) == [{"value": {"index": 0}}, {"value": {"index": 1}}]


def test_a_failing_job_leaves_the_pool_healthy(
    harness: ExecutorHarness,
) -> None:
    fail_job = build_job(
        ECHO_OR_RAISE, {"raise": True}, job_id=JobId(UUID(int=3))
    )
    echo_job = build_job(
        ECHO_OR_RAISE, {"index": 4}, job_id=JobId(UUID(int=4))
    )

    async def run() -> list[tuple[str, CompletedExecution]]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[str]]:
            yield ExecutionSubmission(job=fail_job, context="fail")
            yield ExecutionSubmission(job=echo_job, context="echo")

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=1)
            )
        ) as pool:
            return [
                (item.context, item.completed_execution)
                async for item in pool.run_stream(submissions())
            ]

    with harness.open(ECHO_OR_RAISE, workers=1) as executor:
        returned = dict(asyncio.run(run()))

    failed = returned["fail"].result.outcome
    assert isinstance(failed, ExitedOutcome)
    assert failed.exit_code == 1
    assert parse_importable_json_result(returned["echo"]) == {
        "value": {"index": 4}
    }


def _run_pool(
    executor: PooledExecutor,
    jobs: tuple[ExecutionJob, ...],
    /,
    *,
    capacity: int,
) -> list[Jsonable]:
    async def run() -> list[Jsonable]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[int]]:
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=index)

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=capacity)
            )
        ) as pool:
            return [
                parse_importable_json_result(item.completed_execution)
                async for item in pool.run_stream(submissions())
            ]

    return asyncio.run(run())
