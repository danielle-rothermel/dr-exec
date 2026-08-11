"""Behavior only the worker-pool executor has.

Semantics shared with the in-process executor live in
`test_importable_json_semantics.py`. What is proven here is what worker
processes buy and what they newly risk: real parallelism, one amortized
import per worker, worker death mid-job, enforced wall-time budgets, and an
envelope that survives payloads far larger than a pipe buffer.

Every wait here is on state — an arrival barrier, a FIFO gate, a marker file,
a terminal outcome — never on elapsed time. Watchdogs bound the session; they
are never the evidence a case passed.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from dr_serialize import Jsonable
from support.importable_json import (
    BLOCK_ON_BARRIER,
    BLOCK_ON_GATE,
    BURN_UNTIL_GATE,
    COUNT_IMPORTS,
    ECHO,
    EXIT_ABRUPTLY,
    IMPORT_FAIL,
)
from support.process import Gate

from dr_exec import (
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    ExecutionJob,
    ExecutionSubmission,
    ExitedOutcome,
    FailureOwner,
    FiniteDurationLimit,
    ImportableEntryPoint,
    JobId,
    ProtocolFailedOutcome,
    WorkerPoolImportableJsonExecutor,
    WorkerPoolRecordReceipt,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.core.kinds import BudgetAxis
from dr_exec.scheduling.scheduler import usable_cpu_count

pytestmark = [pytest.mark.integration, pytest.mark.subprocess]

WATCHDOG_SECONDS = 60.0


@pytest.fixture(autouse=True)
def _watchdog(process_watchdog: object) -> object:
    """Turn a hung case into a failure instead of a stalled session."""

    return process_watchdog


def job_for_entry_point(
    entry_point: ImportableEntryPoint,
    request: Jsonable,
    /,
    *,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    return build_in_process_importable_json_job(
        JobId(uuid4()),
        entry_point,
        request,
        budgets=budgets,
    )


def run_all(
    executor: WorkerPoolImportableJsonExecutor,
    jobs: tuple[ExecutionJob, ...],
    /,
) -> list[CompletedExecution]:
    """Run every job through the pool and return their completions."""

    async def run() -> list[CompletedExecution]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[int]]:
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=index)

        async with executor.open_pool() as pool:
            return [
                item.completed_execution
                async for item in pool.map_stream(submissions())
            ]

    return asyncio.run(run())


def test_jobs_run_in_genuine_parallel_across_workers(tmp_path: Path) -> None:
    """N jobs must be in flight at once for any of them to finish.

    Each call announces its arrival and then waits for every peer to arrive.
    Under one worker — or under GIL threading in one interpreter — no call
    could ever observe the full party count and the watchdog would fire.
    """

    parties = 4
    barrier = tmp_path / "barrier"
    barrier.mkdir()
    jobs = tuple(
        job_for_entry_point(
            BLOCK_ON_BARRIER,
            {
                "barrier_directory": str(barrier),
                "parties": parties,
                "identity": f"job-{index}",
            },
        )
        for index in range(parties)
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=BLOCK_ON_BARRIER, worker_count=parties
    ) as executor:
        completions = run_all(executor, jobs)

    identities = {
        cast("dict[str, object]", parse_importable_json_result(completed))[
            "identity"
        ]
        for completed in completions
    }
    pids = {
        cast("dict[str, object]", parse_importable_json_result(completed))[
            "pid"
        ]
        for completed in completions
    }
    assert identities == {f"job-{index}" for index in range(parties)}
    assert len(pids) == parties
    assert os.getpid() not in pids


def test_a_worker_imports_its_entry_point_once_for_every_job_it_serves(
    tmp_path: Path,
) -> None:
    """One worker's import is amortized: same import id, rising call count."""

    jobs = tuple(
        job_for_entry_point(COUNT_IMPORTS, {"index": index})
        for index in range(5)
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=COUNT_IMPORTS, worker_count=1
    ) as executor:
        results = [
            cast("dict[str, object]", parse_importable_json_result(completed))
            for completed in (executor.run(job) for job in jobs)
        ]

    assert len({result["import_id"] for result in results}) == 1
    assert [result["calls"] for result in results] == [1, 2, 3, 4, 5]
    assert len({result["pid"] for result in results}) == 1
    assert results[0]["pid"] != os.getpid()


def test_a_worker_death_mid_job_fails_that_job_with_payload_attribution(
    tmp_path: Path,
) -> None:
    """An abrupt exit is that job's failure, not the pool's."""

    with WorkerPoolImportableJsonExecutor(
        entry_point=EXIT_ABRUPTLY, worker_count=1
    ) as executor:
        completed = executor.run(
            job_for_entry_point(EXIT_ABRUPTLY, {"ignored": True})
        )

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 9
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    assert completed.result.attribution.detail is not None
    assert "worker" in completed.result.attribution.detail
    assert isinstance(completed.record_receipt, WorkerPoolRecordReceipt)


def test_the_pool_respawns_and_keeps_serving_after_a_worker_dies() -> None:
    """A killed worker costs its job, not the jobs behind it."""

    entry_point = ImportableEntryPoint(
        module_name=EXIT_ABRUPTLY.module_name,
        attribute_name="exit_abruptly_unless_asked_to_echo",
    )
    jobs = (
        job_for_entry_point(entry_point, {"die": True}),
        job_for_entry_point(entry_point, {"index": 1}),
        job_for_entry_point(entry_point, {"index": 2}),
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=entry_point, worker_count=1
    ) as executor:
        completions = [executor.run(job) for job in jobs]

    died = completions[0].result.outcome
    assert isinstance(died, ExitedOutcome)
    assert died.exit_code == 9
    assert parse_importable_json_result(completions[1]) == {
        "value": {"index": 1}
    }
    assert parse_importable_json_result(completions[2]) == {
        "value": {"index": 2}
    }


def test_a_worker_startup_import_failure_fails_jobs_loudly() -> None:
    """A worker that cannot import its entry point must not hang a job."""

    with WorkerPoolImportableJsonExecutor(
        entry_point=IMPORT_FAIL, worker_count=1
    ) as executor:
        completed = executor.run(
            job_for_entry_point(IMPORT_FAIL, {"ignored": True})
        )
        after = executor.run(
            job_for_entry_point(IMPORT_FAIL, {"ignored": True})
        )

    for result in (completed, after):
        assert isinstance(result.result.outcome, ProtocolFailedOutcome)
        assert result.result.attribution.owner is FailureOwner.EXECUTOR


def test_a_multi_megabyte_request_and_result_round_trip_without_deadlock() -> (
    None
):
    """No size cap and no pipe deadlock in either direction.

    The payload is far larger than any pipe buffer in both directions, so a
    parent that wrote the request before draining the result would wedge.
    """

    payload = "x" * (4 * 1024 * 1024)

    with WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=1
    ) as executor:
        completed = executor.run(job_for_entry_point(ECHO, {"blob": payload}))

    assert parse_importable_json_result(completed) == {
        "value": {"blob": payload}
    }
    assert completed.result.measurements.input_bytes > 4 * 1024 * 1024


def test_a_finite_wall_time_budget_kills_a_worker_that_ignores_cancellation(
    tmp_path: Path,
) -> None:
    """A declared wall-time budget is enforced by killing the worker.

    The entry point spins on CPU and observes no token, so only process
    termination can end it. The job is released from its gate only by the
    terminal outcome the executor produces.
    """

    ready = tmp_path / "ready"
    gate = tmp_path / "gate-never-opened"
    job = job_for_entry_point(
        BURN_UNTIL_GATE,
        {"ready_path": str(ready), "gate_path": str(gate)},
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=250_000_000)),
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=BURN_UNTIL_GATE, worker_count=1
    ) as executor:
        completed = executor.run(job)
        after = executor.run(
            job_for_entry_point(
                BURN_UNTIL_GATE,
                {
                    "ready_path": str(tmp_path / "ready-2"),
                    "gate_path": str(_opened_gate(tmp_path)),
                },
            )
        )

    outcome = completed.result.outcome
    assert isinstance(outcome, BudgetExceededOutcome)
    assert outcome.axis is BudgetAxis.WALL_TIME
    assert not gate.exists()
    assert parse_importable_json_result(after) == {"released": True}


def test_a_caller_cancel_kills_the_worker_running_the_job(
    tmp_path: Path,
) -> None:
    """Cancelling a running job terminates its worker and completes it."""

    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    token = CancelToken()
    job = job_for_entry_point(
        BLOCK_ON_GATE,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )

    async def run() -> CompletedExecution:
        with WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=1
        ) as executor:
            call = asyncio.create_task(
                asyncio.to_thread(executor.run, job, cancellation=token)
            )
            await asyncio.to_thread(_await_marker, ready)
            token.cancel()
            return await call

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert isinstance(completed.result.outcome, CancelledOutcome)
    assert completed.result.attribution.owner is FailureOwner.NONE


def test_an_unbudgeted_job_is_never_stopped_by_the_executor(
    tmp_path: Path,
) -> None:
    """No default deadline exists: only the caller's gate ends the job."""

    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    job = job_for_entry_point(
        BLOCK_ON_GATE,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )

    async def run() -> CompletedExecution:
        with WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=1
        ) as executor:
            call = asyncio.create_task(asyncio.to_thread(executor.run, job))
            await asyncio.to_thread(_await_marker, ready)
            await asyncio.to_thread(gate.release)
            return await call

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert parse_importable_json_result(completed) == {"released": True}


def test_the_worker_count_defaults_to_the_usable_cpu_count() -> None:
    with WorkerPoolImportableJsonExecutor(entry_point=ECHO) as executor:
        assert executor.width == usable_cpu_count()


def test_a_pool_serves_only_the_entry_point_it_was_opened_with() -> None:
    from dr_exec import ExecutorFailure

    with (
        WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=1
        ) as executor,
        pytest.raises(ExecutorFailure, match="entry point"),
    ):
        executor.run(job_for_entry_point(COUNT_IMPORTS, {"index": 0}))


def _opened_gate(directory: Path, /) -> Path:
    gate = directory / "gate-already-open"
    gate.touch()
    return gate


def _await_marker(marker: Path, /) -> None:
    """Block until the entry point has actually started running."""

    while not marker.exists():
        pass


def test_map_stream_yields_in_completion_order_not_submission_order(
    tmp_path: Path,
) -> None:
    """A slow job delays only itself; fast peers are yielded first."""

    directory = tmp_path / "gates"
    directory.mkdir()
    slow_ready = tmp_path / "slow-ready"
    slow_gate = Gate.create(directory, "slow-gate")
    entry_point = ImportableEntryPoint(
        module_name=BLOCK_ON_GATE.module_name,
        attribute_name="echo_or_block_on_gate",
    )
    slow_job = job_for_entry_point(
        entry_point,
        {"ready_path": str(slow_ready), "gate_path": str(slow_gate.path)},
    )
    fast_jobs = tuple(
        job_for_entry_point(entry_point, {"index": index})
        for index in range(3)
    )

    async def run() -> list[str]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[str]]:
            yield ExecutionSubmission(job=slow_job, context="slow")
            for index, job in enumerate(fast_jobs):
                yield ExecutionSubmission(job=job, context=f"fast-{index}")

        with WorkerPoolImportableJsonExecutor(
            entry_point=entry_point, worker_count=4
        ) as executor:
            async with executor.open_pool() as pool:
                order: list[str] = []
                stream = pool.map_stream(submissions())
                async for item in stream:
                    order.append(item.context)
                    if len(order) == len(fast_jobs):
                        await asyncio.to_thread(_await_marker, slow_ready)
                        await asyncio.to_thread(slow_gate.release)
                return order

    order = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert sorted(order) == ["fast-0", "fast-1", "fast-2", "slow"]
    assert order[-1] == "slow"


def test_map_stream_yields_exactly_one_completion_per_submission() -> None:
    """Failures are completions too: none is dropped and none is doubled."""

    entry_point = ImportableEntryPoint(
        module_name=EXIT_ABRUPTLY.module_name,
        attribute_name="exit_abruptly_unless_asked_to_echo",
    )
    jobs = (
        job_for_entry_point(entry_point, {"index": 0}),
        job_for_entry_point(entry_point, {"die": True}),
        job_for_entry_point(entry_point, {"index": 2}),
    )

    async def run() -> list[str]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[str]]:
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=f"job-{index}")

        with WorkerPoolImportableJsonExecutor(
            entry_point=entry_point, worker_count=2
        ) as executor:
            async with executor.open_pool() as pool:
                return [
                    item.context
                    async for item in pool.map_stream(submissions())
                ]

    contexts = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert sorted(contexts) == ["job-0", "job-1", "job-2"]


def test_map_stream_pulls_its_source_only_as_slots_free(
    tmp_path: Path,
) -> None:
    """An instrumented source is never advanced beyond the in-flight width."""

    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    entry_point = ImportableEntryPoint(
        module_name=BLOCK_ON_GATE.module_name,
        attribute_name="echo_or_block_on_gate",
    )
    blocking = job_for_entry_point(
        entry_point,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )
    pulled: list[int] = []

    async def run() -> list[int]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[int]]:
            index = 0
            while True:
                pulled.append(index)
                yield ExecutionSubmission(
                    job=blocking
                    if index == 0
                    else job_for_entry_point(entry_point, {"index": index}),
                    context=index,
                )
                index += 1

        with WorkerPoolImportableJsonExecutor(
            entry_point=entry_point, worker_count=2
        ) as executor:
            async with executor.open_pool() as pool:
                seen: list[int] = []
                async for item in pool.map_stream(
                    submissions(), concurrency=2
                ):
                    seen.append(item.context)
                    if len(seen) == 3:
                        break
                await asyncio.to_thread(_await_marker, ready)
                await asyncio.to_thread(gate.release)
                await pool.abort()
                return seen

    asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    # An infinite source stopped intake with the consumer: it advanced only
    # for slots that actually freed, never draining ahead of capacity.
    assert len(pulled) <= 6


def test_map_stream_accepts_a_plain_iterable_source() -> None:
    jobs = [
        ExecutionSubmission(
            job=job_for_entry_point(ECHO, {"index": index}),
            context=index,
        )
        for index in range(3)
    ]

    async def run() -> list[Jsonable]:
        with WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=2
        ) as executor:
            async with executor.open_pool() as pool:
                return [
                    parse_importable_json_result(item.completed_execution)
                    async for item in pool.map_stream(jobs)
                ]

    returned = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert sorted(
        cast("list[dict[str, dict[str, int]]]", returned),
        key=lambda value: value["value"]["index"],
    ) == [{"value": {"index": index}} for index in range(3)]


def test_execution_ids_are_unique_per_job(tmp_path: Path) -> None:
    jobs = tuple(
        job_for_entry_point(ECHO, {"index": index}) for index in range(4)
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=2
    ) as executor:
        completions = run_all(executor, jobs)

    identifiers = {completed.result.execution_id for completed in completions}
    assert len(identifiers) == len(jobs)
    assert all(
        completed.record_receipt.execution_id == completed.result.execution_id
        for completed in completions
    )
