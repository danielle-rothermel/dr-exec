from __future__ import annotations

import asyncio
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest
from dr_serialize import IdentityDocument, Jsonable
from support.importable_json import (
    ECHO,
    RAISE_SYSTEM_EXIT,
    SLEEP_LONG,
)
from support.process import requires_posix

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    DirectoryRunStore,
    ExecutionJob,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExecutorFailure,
    ExitedOutcome,
    FiniteDurationLimit,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonExecutor,
    InProcessRecordReceipt,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.core.kinds import BudgetAxis, FailureOwner
from dr_exec.execution import importable_json_executor
from dr_exec.execution.importable_json_executor import _in_process_target
from dr_exec.recording.models import BudgetExceededOutcome

JOB_ID = JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))


def build_job(
    entry_point: ImportableEntryPoint = ECHO,
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


@requires_posix
def test_process_executor_rejects_in_process_target(tmp_path: Path) -> None:
    runtime = IsolatedHostPythonRuntime(executable=Path(sys.executable))
    records = tmp_path / "records"
    records.mkdir()
    executor = ProcessExecutor(
        runtime=runtime,
        run_store=DirectoryRunStore(root=records),
    )

    with pytest.raises(ExecutorFailure):
        executor.run_blocking(build_job())


def test_wall_time_budget_exceeds_on_blocking_entry_point() -> None:
    job = build_job(
        entry_point=SLEEP_LONG,
        request={"ignored": True},
        budgets=Budgets(
            wall_time=FiniteDurationLimit(max_ns=50_000_000),
        ),
    )
    executor = ImportableJsonExecutor()
    completed = executor.run_blocking(job)

    assert isinstance(completed.result.outcome, BudgetExceededOutcome)
    assert completed.result.outcome.axis is BudgetAxis.WALL_TIME
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_system_exit_maps_to_exited_outcome_without_breaking_pool() -> None:
    exit_job = build_in_process_importable_json_job(
        JobId(UUID(int=1)),
        RAISE_SYSTEM_EXIT,
        {"ignored": True},
    )
    echo_job = build_in_process_importable_json_job(
        JobId(UUID(int=2)),
        ECHO,
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
            executor.run_blocking(job, cancellation=token),
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


def test_async_run_does_not_block_the_event_loop() -> None:
    executor = ImportableJsonExecutor()
    job = build_job()

    async def collect() -> tuple[CompletedExecution, object]:
        completed, tick = await asyncio.gather(
            executor.run(job),
            asyncio.sleep(0),
        )
        return completed, tick

    completed, tick = asyncio.run(collect())

    assert tick is None
    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert completed.result.outcome.exit_code == 0


async def _raise_keyboard_interrupt(*_args: object, **_kwargs: object) -> None:
    raise KeyboardInterrupt


async def _raise_cancelled_error(*_args: object, **_kwargs: object) -> None:
    raise asyncio.CancelledError


def test_async_run_maps_keyboard_interrupt_to_exited_outcome() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})

    async def collect() -> CompletedExecution:
        with patch(
            "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
            _raise_keyboard_interrupt,
        ):
            return await executor.run(job)

    completed = asyncio.run(collect())

    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert completed.result.outcome.exit_code == 1
    assert (
        completed.result.attribution.detail
        == "the importable JSON entry point terminated"
    )


def test_async_run_maps_cancelled_error_to_exited_outcome() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})

    async def collect() -> CompletedExecution:
        with patch(
            "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
            _raise_cancelled_error,
        ):
            return await executor.run(job)

    completed = asyncio.run(collect())

    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert completed.result.outcome.exit_code == 1
    assert (
        completed.result.attribution.detail
        == "the importable JSON entry point terminated"
    )


def test_an_interrupted_run_reports_the_elapsed_run_duration() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})
    # A controlled clock, not elapsed real time: the one clock is read twice,
    # once when the offloaded attempt stamps its facts into the handoff slot
    # and once when the interrupt completion is built.
    readings = iter((1_000, 5_000_000_000))

    async def offload_then_interrupt(
        call: object, *args: object, **kwargs: object
    ) -> CompletedExecution:
        # Fill the handoff slot the way a started attempt does, then interrupt:
        # the completion must report that attempt's elapsed run.
        handoff = kwargs["handoff"]
        assert isinstance(handoff, importable_json_executor._Handoff)
        handoff.execution = importable_json_executor._attempt(
            job, _in_process_target(job)
        )
        raise KeyboardInterrupt

    async def collect() -> CompletedExecution:
        with (
            patch("time.monotonic_ns", lambda: next(readings)),
            patch(
                "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
                offload_then_interrupt,
            ),
        ):
            return await executor.run(job)

    completed = asyncio.run(collect())

    assert completed.result.measurements.duration_ns == 4_999_999_000


def test_an_interrupt_before_the_attempt_starts_still_completes() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})

    async def collect() -> CompletedExecution:
        # The offload never invokes the callable, so the handoff slot stays
        # empty: the attempt never started and the facts are stamped here.
        with patch(
            "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
            _raise_keyboard_interrupt,
        ):
            return await executor.run(job)

    completed = asyncio.run(collect())

    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert completed.result.outcome.exit_code == 1
    assert isinstance(completed.record_receipt, InProcessRecordReceipt)


def test_run_serializes_the_request_off_the_event_loop() -> None:
    executor = ImportableJsonExecutor()
    job = build_job()
    real_transport_bytes = importable_json_executor.request_transport_bytes
    serializing_threads: list[int] = []

    def recording_transport_bytes(request: IdentityDocument, /) -> bytes:
        serializing_threads.append(threading.get_ident())
        return real_transport_bytes(request)

    async def collect() -> tuple[CompletedExecution, int]:
        with patch.object(
            importable_json_executor,
            "request_transport_bytes",
            recording_transport_bytes,
        ):
            completed = await executor.run(job)
        return completed, threading.get_ident()

    completed, loop_thread = asyncio.run(collect())

    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert serializing_threads
    assert all(ident != loop_thread for ident in serializing_threads)


def test_async_run_cancels_token_on_keyboard_interrupt() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})
    token = CancelToken()

    async def collect() -> CompletedExecution:
        with patch(
            "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
            _raise_keyboard_interrupt,
        ):
            return await executor.run(job, cancellation=token)

    asyncio.run(collect())

    assert token.cancelled


def test_async_run_cancels_token_on_cancelled_error() -> None:
    executor = ImportableJsonExecutor()
    job = build_job(entry_point=SLEEP_LONG, request={"seconds": 10})
    token = CancelToken()

    async def collect() -> CompletedExecution:
        with patch(
            "dr_exec.execution.importable_json_executor.offload_blocking_daemon",
            _raise_cancelled_error,
        ):
            return await executor.run(job, cancellation=token)

    asyncio.run(collect())

    assert token.cancelled
