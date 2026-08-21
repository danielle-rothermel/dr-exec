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
import io
import os
import queue
import signal
import subprocess
import sys
import threading
from collections.abc import AsyncIterator
from pathlib import Path
from time import monotonic
from typing import IO, cast
from unittest import mock
from uuid import uuid4

import pytest
from dr_serialize import Jsonable
from support import orphan_parent
from support.importable_json import (
    BLOCK_ON_BARRIER,
    BLOCK_ON_GATE,
    BURN_UNTIL_GATE,
    COUNT_IMPORTS,
    ECHO,
    ECHO_OR_BLOCK,
    EXIT_ABRUPTLY,
    FORK_CHILD,
    IMPORT_BLOCKS,
    IMPORT_FAIL,
    RAISE_SYSTEM_EXIT,
)
from support.process import Gate, cleanup_exact_pids, exact_pid_exists

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
    SignaledOutcome,
    WorkerPoolImportableJsonExecutor,
    WorkerPoolRecordReceipt,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.core.kinds import BudgetAxis
from dr_exec.execution import worker_pool, worker_pool_worker
from dr_exec.execution.worker_pool import _spawn_worker, _StopWatch
from dr_exec.scheduling.pool import usable_cpu_count

pytestmark = [pytest.mark.integration, pytest.mark.subprocess]

WATCHDOG_SECONDS = 60.0

ORPHAN_PARENT_SCRIPT = Path(orphan_parent.__file__)

# Only keeps the orphan poll from spinning; the pid probe is the evidence.
_ORPHAN_POLL_SECONDS = 0.05


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
            for completed in (executor.run_blocking(job) for job in jobs)
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
        completed = executor.run_blocking(
            job_for_entry_point(EXIT_ABRUPTLY, {"ignored": True})
        )

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 9
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    assert completed.result.attribution.detail is not None
    assert "worker" in completed.result.attribution.detail
    assert isinstance(completed.record_receipt, WorkerPoolRecordReceipt)


def test_system_exit_from_an_entry_point_reports_the_requested_exit_code(
    tmp_path: Path,
) -> None:
    """The worker's own exit status is what the job reports.

    ``SystemExit`` leaves the worker through interpreter shutdown, so its
    result pipe reaches end of file before the process finishes exiting.
    Describing the death without reaping the process first would report a
    kill by the pool instead of the code the entry point asked for.
    """

    with WorkerPoolImportableJsonExecutor(
        entry_point=RAISE_SYSTEM_EXIT, worker_count=1
    ) as executor:
        completed = executor.run_blocking(
            job_for_entry_point(RAISE_SYSTEM_EXIT, {"ignored": True})
        )

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 7
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


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
        completions = [executor.run_blocking(job) for job in jobs]

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
        completed = executor.run_blocking(
            job_for_entry_point(IMPORT_FAIL, {"ignored": True})
        )
        after = executor.run_blocking(
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

    The returned payload is the evidence. The watchdog bounds a wedge only;
    read throughput is pinned structurally by the buffered-framing test rather
    than by how long this one takes.
    """

    payload = "x" * (4 * 1024 * 1024)

    async def run() -> CompletedExecution:
        with WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=1
        ) as executor:
            return await executor.run(
                job_for_entry_point(ECHO, {"blob": payload})
            )

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert parse_importable_json_result(completed) == {
        "value": {"blob": payload}
    }
    assert completed.result.measurements.input_bytes > 4 * 1024 * 1024


def test_frames_are_read_over_a_buffered_reader() -> None:
    """Framing reads must not degrade into one syscall per byte.

    A newline-delimited read on an unbuffered stream reads a byte at a time,
    which costs about a second per megabyte — unusable for a mode that exists
    for throughput. This pins the mechanism rather than a duration: an
    elapsed-time threshold would make speed the property under test, and a
    byte-at-a-time reader still eventually returns the right bytes.
    """

    drained: list[IO[bytes]] = []
    real_drain = worker_pool._drain_frames

    def recording_drain(
        stream: IO[bytes], frames: queue.SimpleQueue[bytes | None], /
    ) -> None:
        drained.append(stream)
        real_drain(stream, frames)

    with mock.patch.object(worker_pool, "_drain_frames", recording_drain):
        worker = _spawn_worker(ECHO)
        try:
            worker.wait_for_ready(stop=_StopWatch(None, None))
        finally:
            worker.terminate()

    assert [type(stream) for stream in drained] == [io.BufferedReader]


def test_the_worker_reads_requests_over_a_buffered_reader() -> None:
    """The worker's half of the framing must be buffered for the same reason.

    Both directions carry whole frames, so an unbuffered reader on the worker
    side reintroduces the byte-at-a-time cost for requests. The worker opens
    its pipes in another interpreter before any job arrives, so the reader it
    builds is checked by running that same call here.
    """

    request_read, request_write = os.pipe()
    try:
        requests = worker_pool_worker.open_request_reader(request_read)
        try:
            assert isinstance(requests, io.BufferedReader)
        finally:
            requests.close()
    finally:
        os.close(request_write)


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
        completed = executor.run_blocking(job)
        after = executor.run_blocking(
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
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR
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
            call = asyncio.create_task(executor.run(job, cancellation=token))
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
            call = asyncio.create_task(executor.run(job))
            await asyncio.to_thread(_await_marker, ready)
            await asyncio.to_thread(gate.release)
            return await call

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert parse_importable_json_result(completed) == {"released": True}


@pytest.mark.parametrize("stopper", ["budget", "cancel"])
def test_a_declared_stop_condition_reaches_a_job_waiting_for_a_slot(
    tmp_path: Path, stopper: str
) -> None:
    """Waiting for a worker slot is inside the caller's stop condition.

    Every slot is held by an unbudgeted job the test never releases, so the
    second job can only ever end through the condition it declared. The
    terminal outcome is the evidence; nothing here waits on elapsed time.
    """

    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    holder = job_for_entry_point(
        BLOCK_ON_GATE,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )
    token = CancelToken() if stopper == "cancel" else None
    queued = job_for_entry_point(
        BLOCK_ON_GATE,
        {"ready_path": str(tmp_path / "unused"), "gate_path": str(gate.path)},
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=250_000_000))
        if stopper == "budget"
        else None,
    )

    async def run() -> CompletedExecution:
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=1
        )
        held = asyncio.create_task(executor.run(holder))
        # The only slot is occupied once the payload announces itself.
        await asyncio.to_thread(_await_marker, ready)
        waiting = asyncio.create_task(executor.run(queued, cancellation=token))
        if token is not None:
            token.cancel()
        completed = await waiting
        await executor.close()
        await held
        return completed

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    outcome = completed.result.outcome
    if stopper == "budget":
        assert isinstance(outcome, BudgetExceededOutcome)
        assert outcome.axis is BudgetAxis.WALL_TIME
        assert completed.result.attribution.owner is FailureOwner.EXECUTOR
    else:
        assert isinstance(outcome, CancelledOutcome)


def test_a_worker_spawned_as_the_pool_closes_does_not_outlive_it(
    tmp_path: Path,
) -> None:
    """close() must reach a worker whose spawn it raced.

    The spawn is held open until close() has taken its snapshot of live
    workers, so registration lands afterwards. The worker must still be gone
    and the job must fail loudly rather than run where nobody can reach it.
    """

    directory = tmp_path / "gates"
    directory.mkdir()
    gate = Gate.create(directory, "gate")
    job = job_for_entry_point(
        BLOCK_ON_GATE,
        {
            "ready_path": str(tmp_path / "ready"),
            "gate_path": str(gate.path),
        },
    )
    spawning = threading.Event()
    proceed = threading.Event()
    spawned: list[object] = []
    real_spawn = worker_pool._spawn_worker

    def held_spawn(entry_point: ImportableEntryPoint, /) -> object:
        spawning.set()
        proceed.wait(WATCHDOG_SECONDS)
        worker = real_spawn(entry_point)
        spawned.append(worker)
        return worker

    async def run() -> CompletedExecution:
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=1
        )
        call = asyncio.create_task(executor.run(job))
        # close() runs while the spawn is parked inside held_spawn.
        await asyncio.to_thread(spawning.wait, WATCHDOG_SECONDS)
        await executor.close()
        proceed.set()
        return await call

    with mock.patch.object(worker_pool, "_spawn_worker", held_spawn):
        completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    worker = cast("worker_pool._Worker", spawned[0])
    _await_pid_gone(worker.process.pid)
    assert not exact_pid_exists(worker.process.pid)


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
        executor.run_blocking(job_for_entry_point(COUNT_IMPORTS, {"index": 0}))


def _opened_gate(directory: Path, /) -> Path:
    gate = directory / "gate-already-open"
    gate.touch()
    return gate


def _await_marker(marker: Path, /) -> None:
    """Block until the entry point has actually started running."""

    while not marker.exists():
        pass


def _await_pid_file(path: Path, /) -> int:
    """Block until a child has written a parseable pid."""

    while True:
        if path.exists():
            text = path.read_text(encoding="utf-8").strip()
            if text:
                return int(text)


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


def test_a_wall_time_budget_stops_a_job_waiting_on_a_blocking_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Startup is inside the budget, not a window the budget cannot reach.

    The worker's entry-point import blocks on a gate the test never opens, so
    the job is waiting for the worker to become ready. Only the declared
    wall-time budget can end it; the terminal outcome is the evidence.
    """

    directory = tmp_path / "gates"
    directory.mkdir()
    gate = Gate.create(directory, "import-gate")
    monkeypatch.setenv("DR_EXEC_TEST_IMPORT_GATE", str(gate.path))
    job = job_for_entry_point(
        IMPORT_BLOCKS,
        {"ignored": True},
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=250_000_000)),
    )

    with WorkerPoolImportableJsonExecutor(
        entry_point=IMPORT_BLOCKS, worker_count=1
    ) as executor:
        completed = executor.run_blocking(job)

    outcome = completed.result.outcome
    assert isinstance(outcome, BudgetExceededOutcome)
    assert outcome.axis is BudgetAxis.WALL_TIME
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_a_caller_cancel_stops_a_job_waiting_on_a_blocking_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel token reaches a job still waiting for its worker to start."""

    directory = tmp_path / "gates"
    directory.mkdir()
    gate = Gate.create(directory, "import-gate")
    monkeypatch.setenv("DR_EXEC_TEST_IMPORT_GATE", str(gate.path))
    token = CancelToken()
    job = job_for_entry_point(IMPORT_BLOCKS, {"ignored": True})

    async def run() -> CompletedExecution:
        with WorkerPoolImportableJsonExecutor(
            entry_point=IMPORT_BLOCKS, worker_count=1
        ) as executor:
            call = asyncio.create_task(executor.run(job, cancellation=token))
            # The token is the only thing that can end this job: the worker
            # never becomes ready, so no result frame will ever arrive.
            token.cancel()
            return await call

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert isinstance(completed.result.outcome, CancelledOutcome)


def test_close_ends_an_unbudgeted_job_in_flight_rather_than_waiting(
    tmp_path: Path,
) -> None:
    """Closing must not wait on a slot a running job may never return.

    The job declares no budget, so nothing bounds it but the caller's gate,
    which is never opened. Closing terminates the worker; the job completes
    loudly through worker death instead of hanging its caller.
    """

    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    job = job_for_entry_point(
        BLOCK_ON_GATE,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )

    async def run() -> CompletedExecution:
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=2
        )
        call = asyncio.create_task(executor.run(job))
        await asyncio.to_thread(_await_marker, ready)
        await executor.close()
        return await call

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert isinstance(completed.result.outcome, SignaledOutcome)
    assert completed.result.attribution.detail is not None
    assert "worker" in completed.result.attribution.detail


def test_awaitable_close_does_not_block_the_event_loop() -> None:
    job = job_for_entry_point(ECHO, {"value": 1})

    async def run() -> None:
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=1
        )
        await executor.run(job)
        _closed, tick = await asyncio.gather(
            executor.close(),
            asyncio.sleep(0),
        )
        assert tick is None

    asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))


def test_awaitable_close_is_idempotent_like_close_blocking() -> None:
    job = job_for_entry_point(ECHO, {"value": 1})

    async def run_async_close() -> None:
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=1
        )
        await executor.run(job)
        await executor.close()
        await executor.close()

    asyncio.run(asyncio.wait_for(run_async_close(), WATCHDOG_SECONDS))

    executor = WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=1
    )
    executor.run_blocking(job)
    executor.close_blocking()
    executor.close_blocking()


def test_awaitable_close_matches_close_blocking_effect(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "gates"
    directory.mkdir()

    async def closed_via_async() -> CompletedExecution:
        ready = tmp_path / "ready-async"
        gate = Gate.create(directory, "gate-async")
        job = job_for_entry_point(
            BLOCK_ON_GATE,
            {"ready_path": str(ready), "gate_path": str(gate.path)},
        )
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=2
        )
        call = asyncio.create_task(executor.run(job))
        await asyncio.to_thread(_await_marker, ready)
        await executor.close()
        return await call

    async def closed_via_blocking() -> CompletedExecution:
        ready = tmp_path / "ready-blocking"
        gate = Gate.create(directory, "gate-blocking")
        job = job_for_entry_point(
            BLOCK_ON_GATE,
            {"ready_path": str(ready), "gate_path": str(gate.path)},
        )
        executor = WorkerPoolImportableJsonExecutor(
            entry_point=BLOCK_ON_GATE, worker_count=2
        )
        call = asyncio.create_task(executor.run(job))
        await asyncio.to_thread(_await_marker, ready)
        executor.close_blocking()
        return await call

    async def collect() -> tuple[CompletedExecution, CompletedExecution]:
        async_closed = await closed_via_async()
        blocking_closed = await closed_via_blocking()
        return async_closed, blocking_closed

    async_closed, blocking_closed = asyncio.run(
        asyncio.wait_for(collect(), WATCHDOG_SECONDS)
    )

    for completed in (async_closed, blocking_closed):
        assert isinstance(completed.result.outcome, SignaledOutcome)
        assert completed.result.attribution.detail is not None
        assert "worker" in completed.result.attribution.detail


def test_async_context_manager_closes_workers() -> None:
    job = job_for_entry_point(ECHO, {"value": 1})

    async def run() -> CompletedExecution:
        async with WorkerPoolImportableJsonExecutor(
            entry_point=ECHO, worker_count=1
        ) as executor:
            completed = await executor.run(job)
            assert parse_importable_json_result(completed) == {
                "value": {"value": 1}
            }
        return await executor.run(job)

    completed = asyncio.run(asyncio.wait_for(run(), WATCHDOG_SECONDS))

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.detail == (
        "the worker pool closed while this job was starting a worker"
    )


def test_a_closed_idle_pool_rejects_new_jobs_cleanly() -> None:
    job = job_for_entry_point(ECHO, {"value": 1})
    executor = WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=1
    )
    executor.run_blocking(job)
    executor.close_blocking()
    completed = executor.run_blocking(job)
    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.detail == (
        "the worker pool closed while this job was starting a worker"
    )


@pytest.mark.parametrize("mode", [orphan_parent.IDLE, orphan_parent.BUSY])
def test_a_worker_does_not_outlive_a_parent_that_died_abnormally(
    mode: str,
) -> None:
    """A worker whose pool died must not keep running with no one to answer.

    The parent is killed with SIGKILL, so nothing it owns gets to clean up:
    whatever ends the worker has to come from the worker itself. An idle
    worker ends at the request pipe's end of file; a worker inside a job that
    never returns is not reading that pipe, so its parent-liveness watchdog is
    the only thing that can. The terminal state -- the worker pid gone -- is
    the assertion, and elapsed time appears only as the watchdog bound on a
    hang.
    """

    parent = subprocess.Popen(
        (sys.executable, str(ORPHAN_PARENT_SCRIPT), mode),
        stdout=subprocess.PIPE,
        env=_tests_on_path(),
    )
    with cleanup_exact_pids() as registered:
        registered.append(parent.pid)
        assert parent.stdout is not None
        # The parent prints only after the worker is ready and, for the busy
        # case, after its endless job has been dispatched.
        worker_pid = int(parent.stdout.readline())
        registered.append(worker_pid)
        assert exact_pid_exists(worker_pid)

        parent.kill()
        parent.wait()

        _await_pid_gone(worker_pid)

    assert not exact_pid_exists(worker_pid)


def test_an_orphaned_worker_kills_a_grandchild_it_forked(
    tmp_path: Path,
) -> None:
    """Orphan cleanup must reach descendants that stayed in the worker group."""

    grandchild_path = tmp_path / "grandchild"
    parent = subprocess.Popen(
        (
            sys.executable,
            str(ORPHAN_PARENT_SCRIPT),
            orphan_parent.FORK,
            str(grandchild_path),
        ),
        stdout=subprocess.PIPE,
        env=_tests_on_path(),
    )
    with cleanup_exact_pids() as registered:
        registered.append(parent.pid)
        assert parent.stdout is not None
        worker_pid, grandchild_pid = (
            int(part) for part in parent.stdout.readline().split()
        )
        registered.extend((worker_pid, grandchild_pid))
        assert exact_pid_exists(worker_pid)
        assert exact_pid_exists(grandchild_pid)

        parent.kill()
        parent.wait()

        _await_pid_gone(worker_pid)
        _await_pid_gone(grandchild_pid)

    assert not exact_pid_exists(worker_pid)
    assert not exact_pid_exists(grandchild_pid)


def test_kill_own_process_group_is_a_no_op_when_this_process_is_not_leader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "getpid", lambda: 10)
    monkeypatch.setattr(os, "getpgrp", lambda: 1)
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(
        os, "killpg", lambda pid, number: killed.append((pid, number))
    )

    worker_pool_worker.kill_own_process_group()

    assert killed == []


def test_kill_own_process_group_signals_only_the_group_this_process_leads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "getpid", lambda: 10)
    monkeypatch.setattr(os, "getpgrp", lambda: 10)
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(
        os, "killpg", lambda pid, number: killed.append((pid, number))
    )

    worker_pool_worker.kill_own_process_group()

    assert killed == [(10, signal.SIGKILL)]


@pytest.mark.parametrize("stopper", ["budget", "cancel"])
def test_a_declared_stop_kills_a_grandchild_the_worker_forked(
    tmp_path: Path, stopper: str
) -> None:
    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    grandchild_path = tmp_path / "grandchild"
    token = CancelToken() if stopper == "cancel" else None
    job = job_for_entry_point(
        FORK_CHILD,
        {
            "ready_path": str(ready),
            "gate_path": str(gate.path),
            "grandchild_pid_path": str(grandchild_path),
        },
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=250_000_000))
        if stopper == "budget"
        else None,
    )
    completed_holder: list[CompletedExecution] = []

    def run() -> None:
        with WorkerPoolImportableJsonExecutor(
            entry_point=FORK_CHILD, worker_count=1
        ) as executor:
            completed_holder.append(
                executor.run_blocking(job, cancellation=token)
            )

    with cleanup_exact_pids() as registered:
        driver = threading.Thread(target=run)
        driver.start()
        _await_marker(ready)
        grandchild_pid = _await_pid_file(grandchild_path)
        registered.append(grandchild_pid)
        if token is not None:
            token.cancel()
        driver.join(WATCHDOG_SECONDS)
        assert not driver.is_alive()
        _await_pid_gone(grandchild_pid)

    outcome = completed_holder[0].result.outcome
    if stopper == "budget":
        assert isinstance(outcome, BudgetExceededOutcome)
        assert outcome.axis is BudgetAxis.WALL_TIME
    else:
        assert isinstance(outcome, CancelledOutcome)


def test_run_many_batch_wall_time_cancels_work_and_leaves_the_pool_reusable(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "gates"
    directory.mkdir()
    ready = tmp_path / "ready"
    gate = Gate.create(directory, "gate")
    holder = job_for_entry_point(
        ECHO_OR_BLOCK,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )
    queued = job_for_entry_point(ECHO_OR_BLOCK, {"value": 1})

    with WorkerPoolImportableJsonExecutor(
        entry_point=ECHO_OR_BLOCK, worker_count=1
    ) as executor:
        completed = list(
            executor.run_many(
                (holder, queued),
                wall_time=FiniteDurationLimit(max_ns=500_000_000),
            )
        )
        after = executor.run_blocking(
            job_for_entry_point(ECHO_OR_BLOCK, {"value": 2})
        )

    assert len(completed) == 2
    assert all(
        isinstance(one.result.outcome, CancelledOutcome) for one in completed
    )
    assert parse_importable_json_result(after) == {"value": {"value": 2}}


def _await_pid_gone(pid: int, /) -> None:
    """Block until the process is gone, failing if it never is."""

    deadline = monotonic() + WATCHDOG_SECONDS
    poll_gate = threading.Event()
    while exact_pid_exists(pid):
        if monotonic() >= deadline:
            pytest.fail(f"worker {pid} outlived its parent")
        # The state probe is the evidence; this only avoids a busy loop.
        poll_gate.wait(_ORPHAN_POLL_SECONDS)


def _tests_on_path() -> dict[str, str]:
    """Let the disposable parent import the shared test support modules."""

    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).parent.parent)
    return environment
