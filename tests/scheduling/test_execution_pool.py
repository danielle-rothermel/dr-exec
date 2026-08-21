from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import (
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
)
from typing import TYPE_CHECKING

import pytest
from support.executor import completion_for, job_for, trusted_target
from support.pool import WATCHDOG_SECONDS, GatedResponder, wait_for

from dr_exec import (
    AutoPoolCapacity,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    ExecutionCompletion,
    ExecutionJob,
    ExecutionPool,
    ExecutionPoolConfig,
    ExecutionPoolState,
    ExecutionSubmission,
    ExecutorFailure,
    FakeExecutor,
    FiniteDurationLimit,
    FixedPoolCapacity,
    JobId,
)
from dr_exec.core.kinds import CapacitySource, ExecutorFailureCode
from dr_exec.scheduling.pool import _OwnedContext, _StreamOwner, _unowned
from dr_exec.scheduling.scheduler import (
    AdmissionResult,
    ExecutionScheduler,
    SchedulerBroken,
    _Admitted,
    _Completion,
    run_batch,
)

if TYPE_CHECKING:
    from dr_exec.capabilities.protocols import Executor


_QUEUED_TICKET = 9_000


def jobs(count: int, /) -> list[ExecutionJob]:
    return [job_for(trusted_target(("/usr/bin/true",))) for _ in range(count)]


def gated_executor(
    *, cancellation_aware: bool = False
) -> tuple[FakeExecutor, GatedResponder]:
    responder = GatedResponder(cancellation_aware=cancellation_aware)
    return FakeExecutor(responder=responder), responder


def immediate_executor() -> FakeExecutor:
    return FakeExecutor(
        responder=lambda job, _token: completion_for(job.job_id)
    )


def fixed_pool(executor: Executor, slots: int, /) -> ExecutionPool:
    return ExecutionPool(
        executor=executor,
        config=ExecutionPoolConfig(
            capacity=FixedPoolCapacity(max_active_jobs=slots)
        ),
    )


def batch_of(
    executor: Executor,
    batch: Iterable[ExecutionJob],
    /,
    *,
    slots: int,
    wall_time: FiniteDurationLimit | None = None,
) -> Generator[CompletedExecution]:
    return run_batch(executor, batch, capacity=slots, wall_time=wall_time)


class RecordingSource:
    def __init__(
        self,
        submissions: Iterable[ExecutionSubmission[int]],
        /,
        *,
        pull_gates: Mapping[int, threading.Event] | None = None,
    ) -> None:
        self._items = list(submissions)
        self.pulled = 0
        self._pull_events = [threading.Event() for _ in self._items]
        self._pull_gates = {} if pull_gates is None else dict(pull_gates)

    async def __aiter__(self) -> AsyncIterator[ExecutionSubmission[int]]:
        for index, item in enumerate(self._items):
            self.pulled += 1
            self._pull_events[index].set()
            pull_number = index + 1
            if gate := self._pull_gates.get(pull_number):
                await asyncio.to_thread(
                    wait_for,
                    gate,
                    what=f"source pull {pull_number} to yield",
                )
            yield item

    def await_pulled(self, count: int, /) -> None:
        wait_for(
            self._pull_events[count - 1],
            what=f"the source to pull submission {count}",
        )


def recording_source(
    batch: list[ExecutionJob],
    /,
    *,
    pull_gates: Mapping[int, threading.Event] | None = None,
) -> RecordingSource:
    return RecordingSource(
        [
            ExecutionSubmission(job=job, context=index)
            for index, job in enumerate(batch)
        ],
        pull_gates=pull_gates,
    )


async def submissions_of(
    batch: Iterable[ExecutionJob], /
) -> AsyncIterator[ExecutionSubmission[int]]:
    for index, job in enumerate(batch):
        yield ExecutionSubmission(job=job, context=index)


async def consume(stream: AsyncIterator[object], /) -> None:
    async for _ in stream:
        pass


def in_thread(work: object, /) -> threading.Thread:
    thread = threading.Thread(target=work)  # ty: ignore[invalid-argument-type]
    thread.start()
    return thread


def join(thread: threading.Thread, /) -> None:
    thread.join(WATCHDOG_SECONDS)
    assert not thread.is_alive(), "watchdog fired joining a driver thread"


def test_automatic_capacity_resolves_once_from_the_usable_cpu_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved = iter((3, 7))
    calls = 0

    def resolve_usable_cpus() -> int:
        nonlocal calls
        calls += 1
        return next(resolved)

    monkeypatch.setattr(
        "dr_exec.scheduling.pool.usable_cpu_count", resolve_usable_cpus
    )
    pool = ExecutionPool(
        executor=immediate_executor(),
        config=ExecutionPoolConfig(capacity=AutoPoolCapacity()),
    )

    async def open_and_read() -> tuple[object, object]:
        async with pool:
            return pool.effective_capacity, pool.effective_capacity

    first, second = asyncio.run(open_and_read())

    assert calls == 1
    assert first == second
    assert first.source is CapacitySource.AUTO  # ty: ignore[unresolved-attribute]
    assert first.cpu_count == 3  # ty: ignore[unresolved-attribute]
    assert first.max_active_jobs == 3  # ty: ignore[unresolved-attribute]


def test_state_reports_each_lifecycle_stage_through_the_public_surface() -> (
    None
):
    pool = fixed_pool(immediate_executor(), 1)
    assert pool.state is ExecutionPoolState.CREATED

    async def open_observe_close() -> ExecutionPoolState:
        async with pool:
            return pool.state

    assert asyncio.run(open_observe_close()) is ExecutionPoolState.RUNNING
    assert pool.state is ExecutionPoolState.CLOSED


def test_fixed_capacity_uses_the_selected_slot_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("dr_exec.scheduling.pool.usable_cpu_count", lambda: 5)
    pool = fixed_pool(immediate_executor(), 3)

    async def open_and_read() -> object:
        async with pool:
            return pool.effective_capacity

    capacity = asyncio.run(open_and_read())

    assert capacity.source is CapacitySource.FIXED  # ty: ignore[unresolved-attribute]
    assert capacity.max_active_jobs == 3  # ty: ignore[unresolved-attribute]
    assert capacity.cpu_count == 5  # ty: ignore[unresolved-attribute]


@pytest.mark.parametrize("slots", [0, -1])
def test_fixed_capacity_refuses_a_non_positive_slot_count(slots: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        FixedPoolCapacity(max_active_jobs=slots)


def test_effective_capacity_is_unavailable_before_the_pool_opens() -> None:
    pool = fixed_pool(immediate_executor(), 2)

    with pytest.raises(ExecutorFailure, match="resolved when the pool"):
        _ = pool.effective_capacity


def test_a_completed_result_keeps_its_slot_until_it_is_delivered() -> None:
    executor, responder = gated_executor()
    batch = jobs(3)
    first, second, third = batch
    scheduler: ExecutionScheduler[None] = ExecutionScheduler(
        executor=executor, capacity=2
    )
    try:
        assert scheduler.admit(first, None) is AdmissionResult.ADMITTED
        assert scheduler.admit(second, None) is AdmissionResult.ADMITTED
        responder.await_arrival_count(2)
        assert set(responder.started) == {first.job_id, second.job_id}
        assert not scheduler.can_admit()

        responder.release(first.job_id)
        responder.await_executor_returned(first.job_id)
        assert not scheduler.can_admit()
        assert third.job_id not in responder.started

        completion = scheduler.take_completion()
        assert completion is not None
        assert completion.completed_execution.result.execution_id.job_id == (
            first.job_id
        )
        assert scheduler.can_admit()

        assert scheduler.admit(third, None) is AdmissionResult.ADMITTED
        responder.await_arrival(third.job_id)
    finally:
        responder.release_all(batch)
        scheduler.close_intake()
        scheduler.wait_for_quiescence()
        scheduler.shutdown()


def test_intake_advances_only_while_the_resident_bound_has_room() -> None:
    executor, responder = gated_executor()
    batch = jobs(5)
    source = recording_source(batch)
    pool = fixed_pool(executor, 2)
    collected: list[int] = []

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(source):
                collected.append(completion.context)

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(2)
    assert source.pulled == 2

    responder.release_all(batch)
    join(streamer)

    assert source.pulled == len(batch)
    assert sorted(collected) == list(range(len(batch)))


def test_a_stalled_consumer_stops_the_source_advancing() -> None:
    executor, responder = gated_executor()
    batch = jobs(6)
    third_may_yield = threading.Event()
    source = recording_source(batch, pull_gates={3: third_may_yield})
    pool = fixed_pool(executor, 2)
    stalled = threading.Event()
    resume = threading.Event()
    collected: list[int] = []

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(source):
                collected.append(completion.context)
                if len(collected) == 2:
                    stalled.set()
                    await asyncio.to_thread(
                        wait_for, resume, what="the stalled consumer to resume"
                    )

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(2)
    responder.release(batch[0].job_id, batch[1].job_id)
    wait_for(stalled, what="the consumer to stall holding two completions")
    source.await_pulled(3)

    try:
        assert source.pulled == 3
    finally:
        third_may_yield.set()
        resume.set()
        responder.release_all(batch)
        join(streamer)

    assert sorted(collected) == list(range(len(batch)))
    assert source.pulled == len(batch)


def test_a_source_can_wait_for_its_prior_completion_before_yielding_more() -> (
    None
):
    batch = jobs(2)
    first_delivered = asyncio.Event()
    pool = fixed_pool(immediate_executor(), 2)
    collected: list[int] = []

    async def dependent_source() -> AsyncIterator[ExecutionSubmission[int]]:
        yield ExecutionSubmission(job=batch[0], context=0)
        await first_delivered.wait()
        yield ExecutionSubmission(job=batch[1], context=1)

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(dependent_source()):
                collected.append(completion.context)
                if completion.context == 0:
                    first_delivered.set()

    asyncio.run(asyncio.wait_for(stream(), WATCHDOG_SECONDS))

    assert collected == [0, 1]


def test_cancelling_a_waiting_stream_does_not_take_its_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiter_entered = threading.Event()
    blocking_waiter_entered = threading.Event()
    blocking_waiter_returned = threading.Event()

    class _ObservedScheduler(ExecutionScheduler[object]):
        def take_completion(self) -> _Completion[object] | None:
            blocking_waiter_entered.set()
            waiter_entered.set()
            completion = super().take_completion()
            blocking_waiter_returned.set()
            return completion

        def take_completion_nowait(
            self,
            /,
            *,
            owned_by: Callable[[object], bool] | None = None,
        ) -> _Completion[object] | None:
            completion = super().take_completion_nowait(owned_by=owned_by)
            if completion is None and self.has_residents():
                waiter_entered.set()
            return completion

    monkeypatch.setattr(
        "dr_exec.scheduling.pool.ExecutionScheduler", _ObservedScheduler
    )
    executor, responder = gated_executor()
    only = jobs(1)[0]
    pool = fixed_pool(executor, 1)
    collected: list[int] = []

    async def cancel_then_recover() -> None:
        async with pool:
            abandoned = asyncio.create_task(
                consume(pool.run_stream(submissions_of([only])))
            )
            await asyncio.to_thread(responder.await_arrival, only.job_id)
            await asyncio.to_thread(
                wait_for,
                waiter_entered,
                what="the stream to wait for its completion",
            )
            abandoned.cancel()
            with pytest.raises(asyncio.CancelledError):
                await abandoned

            responder.release(only.job_id)
            scheduler = pool._scheduler
            assert scheduler is not None
            if blocking_waiter_entered.is_set():
                await asyncio.to_thread(
                    wait_for,
                    blocking_waiter_returned,
                    what="the cancelled blocking waiter to return",
                )
            else:
                await asyncio.to_thread(
                    _await_scheduler_publication, scheduler, only.job_id
                )

            await _collect_contexts(
                pool.run_stream(submissions_of([])), collected
            )

    asyncio.run(cancel_then_recover())

    assert collected == [0]


def test_capacity_is_reached_by_genuinely_overlapping_executor_calls() -> None:
    slots = 2
    executor, responder = gated_executor()
    batch = jobs(4)
    completed: list[CompletedExecution] = []

    driver = in_thread(
        lambda: completed.extend(batch_of(executor, batch, slots=slots))
    )
    responder.await_arrival_count(slots)
    assert responder.peak_active == slots
    responder.release_all(batch)
    join(driver)

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }


@pytest.mark.parametrize(
    "finish_order",
    [
        pytest.param((2, 0, 1), id="last-first"),
        pytest.param((1, 2, 0), id="middle-first"),
        pytest.param((0, 1, 2), id="submission-order"),
    ],
)
def test_results_are_delivered_in_completion_order(
    finish_order: tuple[int, ...],
) -> None:
    executor, responder = gated_executor()
    batch = jobs(3)
    pool = fixed_pool(executor, 4)
    collected: list[int] = []
    delivered = [threading.Event() for _ in batch]

    parked = threading.Event()
    may_finish_source = threading.Event()

    async def parking_source() -> AsyncIterator[ExecutionSubmission[int]]:
        async for submission in submissions_of(batch):
            yield submission
        parked.set()
        await asyncio.to_thread(
            wait_for,
            may_finish_source,
            what="the completion-order source to finish",
        )

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(parking_source()):
                collected.append(completion.context)
                delivered[completion.context].set()

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(3)
    wait_for(parked, what="the completion-order source to park")
    for index in finish_order:
        responder.release(batch[index].job_id)
        responder.await_executor_returned(batch[index].job_id)
        wait_for(
            delivered[index],
            what=f"completion {index} to reach the stream consumer",
        )

    may_finish_source.set()
    join(streamer)

    assert collected == list(finish_order)


def test_every_completion_carries_exactly_its_submissions_context() -> None:
    batch = jobs(4)
    contexts = {job.job_id: _UnserializableContext() for job in batch}
    pool = fixed_pool(immediate_executor(), 2)

    async def source() -> AsyncIterator[ExecutionSubmission[object]]:
        for job in batch:
            yield ExecutionSubmission(job=job, context=contexts[job.job_id])

    async def collect() -> list[tuple[JobId, object]]:
        async with pool:
            return [
                (
                    completion.completed_execution.result.execution_id.job_id,
                    completion.context,
                )
                async for completion in pool.run_stream(source())
            ]

    paired = asyncio.run(collect())

    assert len(paired) == len(batch)
    for job_id, context in paired:
        assert context is contexts[job_id]


class _UnserializableContext:
    __slots__ = ("lock",)

    def __init__(self) -> None:
        self.lock = threading.Lock()


def test_abort_cancels_work_in_flight_and_awaits_its_teardown() -> None:
    executor, responder = gated_executor(cancellation_aware=True)
    batch = jobs(2)
    pool = fixed_pool(executor, 2)

    async def stream_then_abort() -> None:
        async with pool:
            consumer = asyncio.create_task(
                consume(pool.run_stream(submissions_of(batch)))
            )
            await asyncio.to_thread(responder.await_arrival_count, len(batch))
            await pool.abort()
            consumer.cancel()

    asyncio.run(stream_then_abort())

    assert set(responder.cancelled) == {job.job_id for job in batch}
    for job in batch:
        assert responder.executor_returned_gate(job.job_id).is_set()
    responder.assert_no_watchers()
    assert pool.state is ExecutionPoolState.CLOSED


def test_abort_stops_intake_before_the_next_submission_is_admitted() -> None:
    executor, responder = gated_executor(cancellation_aware=True)
    batch = jobs(2)
    first, second = batch
    pool = fixed_pool(executor, 1)

    async def abort_mid_queue() -> None:
        async with pool:
            consumer = asyncio.create_task(
                consume(pool.run_stream(submissions_of(batch)))
            )
            await asyncio.to_thread(responder.await_arrival, first.job_id)
            await pool.abort()
            consumer.cancel()

    asyncio.run(abort_mid_queue())

    assert first.job_id in responder.cancelled
    assert second.job_id not in responder.started
    responder.assert_no_watchers()


def test_abort_cancels_a_genuinely_admitted_pending_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_waiting = threading.Event()
    may_enter_scheduler = threading.Event()

    class _EntryGatedThread(threading.Thread):
        def run(self) -> None:
            worker_waiting.set()
            wait_for(
                may_enter_scheduler,
                what="the admitted worker to enter the scheduler",
            )
            super().run()

    monkeypatch.setattr(
        "dr_exec.scheduling.scheduler.Thread", _EntryGatedThread
    )
    executor, responder = gated_executor(cancellation_aware=True)
    only = jobs(1)[0]
    pool = fixed_pool(executor, 1)
    token: CancelToken | None = None

    async def abort_while_admitted() -> None:
        nonlocal token
        async with pool:
            consumer = asyncio.create_task(
                consume(pool.run_stream(submissions_of([only])))
            )
            await asyncio.to_thread(
                wait_for,
                worker_waiting,
                what="the worker to wait before entry",
            )
            scheduler = pool._scheduler
            assert scheduler is not None
            with scheduler._condition:
                assert len(scheduler._pending) == 1
                token = scheduler._pending[0].cancellation

            aborting = asyncio.create_task(pool.abort())
            await asyncio.to_thread(_await_closed_intake, scheduler)
            assert token.cancelled
            assert only.job_id not in responder.started
            may_enter_scheduler.set()
            await asyncio.wait_for(aborting, WATCHDOG_SECONDS)
            consumer.cancel()

    try:
        asyncio.run(abort_while_admitted())
    finally:
        may_enter_scheduler.set()

    assert token is not None and token.cancelled
    assert responder.started == (only.job_id,)
    assert responder.cancelled == (only.job_id,)
    assert responder.executor_returned_gate(only.job_id).is_set()
    responder.assert_no_watchers()
    assert pool.state is ExecutionPoolState.CLOSED


def test_a_cancelled_call_is_delivered_as_completion_data() -> None:
    batch = jobs(2)
    cancelled_job, plain_job = batch

    def respond(
        job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        completed = completion_for(job.job_id)
        if job.job_id != cancelled_job.job_id:
            return completed
        return CompletedExecution(
            result=completed.result.model_copy(
                update={"outcome": CancelledOutcome()}
            ),
            record_receipt=completed.record_receipt,
        )

    pool = fixed_pool(FakeExecutor(responder=respond), 2)
    collected: list[CompletedExecution] = []

    async def drain_all() -> None:
        async with pool:
            async for completion in pool.run_stream(submissions_of(batch)):
                collected.append(completion.completed_execution)

    asyncio.run(drain_all())

    outcomes = {
        one.result.execution_id.job_id: one.result.outcome for one in collected
    }
    assert outcomes[cancelled_job.job_id] == CancelledOutcome()
    assert outcomes[plain_job.job_id] != CancelledOutcome()


def test_drain_lets_admitted_work_finish_uncancelled() -> None:
    executor, responder = gated_executor()
    batch = jobs(1)
    only = batch[0]
    pool = fixed_pool(executor, 1)

    async def drain_while_running() -> None:
        async with pool:
            scheduler = pool._scheduler
            assert scheduler is not None
            assert scheduler.admit(only, None) is AdmissionResult.ADMITTED
            await asyncio.to_thread(responder.await_arrival, only.job_id)
            draining = asyncio.create_task(pool.drain())
            await asyncio.to_thread(_await_closed_intake, scheduler)
            responder.release(only.job_id)
            await asyncio.wait_for(draining, WATCHDOG_SECONDS)

    asyncio.run(drain_while_running())

    assert responder.cancelled == ()
    assert responder.executor_returned_gate(only.job_id).is_set()
    responder.assert_no_watchers()
    assert pool.state is ExecutionPoolState.CLOSED


@pytest.mark.parametrize("close_kind", ["drain", "abort"])
def test_cancelling_a_close_still_finishes_pool_cleanup(
    close_kind: str,
) -> None:
    executor, responder = gated_executor(
        cancellation_aware=close_kind == "abort"
    )
    only = jobs(1)[0]
    pool = fixed_pool(executor, 1)

    async def cancel_close() -> None:
        await pool.__aenter__()
        scheduler = pool._scheduler
        assert scheduler is not None
        assert scheduler.admit(only, None) is AdmissionResult.ADMITTED
        await asyncio.to_thread(responder.await_arrival, only.job_id)

        close = pool.drain if close_kind == "drain" else pool.abort
        closing = asyncio.create_task(close())
        await asyncio.to_thread(_await_closed_intake, scheduler)
        closing.cancel()
        if close_kind == "drain":
            responder.release(only.job_id)
        with pytest.raises(asyncio.CancelledError):
            await closing

    asyncio.run(cancel_close())

    assert responder.executor_returned_gate(only.job_id).is_set()
    responder.assert_no_watchers()
    assert pool.state is ExecutionPoolState.CLOSED
    assert pool._closed
    assert pool._shutdown_complete


def test_drain_preserves_completions_for_an_active_stream() -> None:
    executor, responder = gated_executor()
    batch = jobs(2)
    pool = fixed_pool(executor, 2)
    first_delivered = asyncio.Event()
    resume = asyncio.Event()
    collected: list[int] = []

    async def collect_slowly() -> None:
        async for completion in pool.run_stream(submissions_of(batch)):
            collected.append(completion.context)
            if len(collected) == 1:
                first_delivered.set()
                await resume.wait()

    async def drain_around_the_stream() -> None:
        async with pool:
            consumer = asyncio.create_task(collect_slowly())
            await asyncio.to_thread(responder.await_arrival_count, 2)
            responder.release_all(batch)
            await asyncio.wait_for(first_delivered.wait(), WATCHDOG_SECONDS)
            scheduler = pool._scheduler
            assert scheduler is not None
            await asyncio.to_thread(
                _await_ready_count,
                scheduler,
                1,
            )

            await pool.drain()
            with scheduler._condition:
                assert len(scheduler._ready) == 1
            resume.set()
            await asyncio.wait_for(consumer, WATCHDOG_SECONDS)

    asyncio.run(drain_around_the_stream())

    assert sorted(collected) == [0, 1]
    assert pool.state is ExecutionPoolState.CLOSED


def test_exceptional_context_exit_preserves_the_error_and_awaits_abort() -> (
    None
):

    class _BodyFailure(Exception):
        pass

    failure = _BodyFailure("body failed")
    executor, responder = gated_executor(cancellation_aware=True)
    only = jobs(1)[0]
    pool = fixed_pool(executor, 1)

    async def fail_from_the_body() -> None:
        consumer: asyncio.Task[None] | None = None
        try:
            async with pool:
                consumer = asyncio.create_task(
                    consume(pool.run_stream(submissions_of([only])))
                )
                await asyncio.to_thread(responder.await_arrival, only.job_id)
                raise failure
        finally:
            if consumer is not None:
                await asyncio.wait_for(consumer, WATCHDOG_SECONDS)

    with pytest.raises(_BodyFailure) as raised:
        asyncio.run(fail_from_the_body())

    assert raised.value is failure
    assert responder.cancelled == (only.job_id,)
    assert responder.executor_returned_gate(only.job_id).is_set()
    responder.assert_no_watchers()
    assert pool.state is ExecutionPoolState.CLOSED


def test_drain_to_empty_delivers_every_admitted_submission() -> None:
    batch = jobs(5)
    pool = fixed_pool(immediate_executor(), 2)
    collected: list[int] = []

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(submissions_of(batch)):
                collected.append(completion.context)

    asyncio.run(stream())

    assert sorted(collected) == list(range(len(batch)))
    assert pool.state is ExecutionPoolState.CLOSED


def test_a_closed_pool_cannot_reopen_or_stream() -> None:
    pool = fixed_pool(immediate_executor(), 1)

    async def close_then_reuse() -> None:
        async with pool:
            pass
        with pytest.raises(ExecutorFailure, match="cannot be opened"):
            await pool.__aenter__()
        with pytest.raises(ExecutorFailure, match="cannot stream"):
            await consume(pool.run_stream(submissions_of(jobs(1))))

    asyncio.run(close_then_reuse())

    assert pool.state is ExecutionPoolState.CLOSED


def test_a_pool_rejects_lifecycle_calls_from_another_loop() -> None:
    pool = fixed_pool(immediate_executor(), 1)
    opened = threading.Event()
    intruded = threading.Event()
    rejections: list[str] = []

    def intrude() -> None:
        wait_for(opened, what="the pool to open on its owning loop")
        try:

            async def from_another_loop() -> None:
                for call in (
                    pool.drain(),
                    pool.abort(),
                    pool.__aexit__(None, None, None),
                    consume(pool.run_stream(submissions_of(jobs(1)))),
                ):
                    with pytest.raises(ExecutorFailure) as rejected:
                        await call
                    rejections.append(str(rejected.value))

            asyncio.run(from_another_loop())
        finally:
            intruded.set()

    async def own_the_pool() -> None:
        async with pool:
            intruder = in_thread(intrude)
            opened.set()
            await asyncio.to_thread(wait_for, intruded, what="the intruder")
            join(intruder)

    asyncio.run(own_the_pool())

    assert len(rejections) == 4
    assert all("opened it" in rejection for rejection in rejections)
    # The owning loop's own close still runs: rejection refused the
    # foreign calls without leaving the pool wedged.
    assert pool.state is ExecutionPoolState.CLOSED


def test_a_failing_executor_call_breaks_the_pool() -> None:

    def explode(
        _job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        raise ExecutorFailure(
            "machinery failed",
            code=ExecutorFailureCode.RECORDING_OPERATION_FAILED,
        )

    pool = fixed_pool(FakeExecutor(responder=explode), 1)

    async def stream() -> None:
        async with pool:
            await consume(pool.run_stream(submissions_of(jobs(2))))

    with pytest.raises(ExecutorFailure, match="the execution pool broke") as e:
        asyncio.run(stream())

    assert isinstance(e.value.__cause__, ExecutorFailure)
    assert pool.state is ExecutionPoolState.BROKEN


@pytest.mark.parametrize("close", ["drain", "abort"])
def test_a_break_landing_during_a_close_is_the_state_the_close_lands_in(
    close: str,
) -> None:
    arrived = threading.Event()
    release = threading.Event()

    def explode_when_released(
        _job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        arrived.set()
        wait_for(release, what="the failing call to be released")
        raise ExecutorFailure(
            "machinery failed",
            code=ExecutorFailureCode.RECORDING_OPERATION_FAILED,
        )

    pool = fixed_pool(FakeExecutor(responder=explode_when_released), 1)

    async def close_over_a_breaking_call() -> None:
        async with pool:
            scheduler = pool._scheduler
            assert scheduler is not None
            assert (
                scheduler.admit(jobs(1)[0], None) is AdmissionResult.ADMITTED
            )
            await asyncio.to_thread(
                wait_for, arrived, what="the failing call to start"
            )
            closing = asyncio.create_task(
                pool.drain() if close == "drain" else pool.abort()
            )
            # Intake closing is the state that says the close is past
            # its snapshot and into its wait, which is the window the
            # break must still be reported from.
            await asyncio.to_thread(_await_closed_intake, scheduler)
            release.set()
            await asyncio.wait_for(closing, WATCHDOG_SECONDS)

    asyncio.run(close_over_a_breaking_call())

    assert pool.state is ExecutionPoolState.BROKEN


def _await_closed_intake(scheduler: ExecutionScheduler[object], /) -> None:
    with scheduler._condition:
        if not scheduler._condition.wait_for(
            lambda: scheduler._intake_closed, WATCHDOG_SECONDS
        ):
            raise AssertionError("watchdog fired waiting for closed intake")


def _await_scheduler_publication(
    scheduler: ExecutionScheduler[object], *job_ids: JobId
) -> None:
    expected = set(job_ids)
    with scheduler._condition:
        if not scheduler._condition.wait_for(
            lambda: (
                expected
                <= {
                    completion.completed_execution.result.execution_id.job_id
                    for completion in scheduler._ready
                }
            ),
            WATCHDOG_SECONDS,
        ):
            raise AssertionError(
                "watchdog fired waiting for scheduler publication of "
                f"{sorted(map(str, expected))}"
            )


def _await_ready_count(
    scheduler: ExecutionScheduler[object], count: int, /
) -> None:
    with scheduler._condition:
        if not scheduler._condition.wait_for(
            lambda: len(scheduler._ready) == count, WATCHDOG_SECONDS
        ):
            raise AssertionError(
                f"watchdog fired waiting for {count} buffered completions"
            )


def _await_scheduler_break[T](scheduler: ExecutionScheduler[T], /) -> None:
    with scheduler._condition:
        if not scheduler._condition.wait_for(
            lambda: scheduler._broken is not None, WATCHDOG_SECONDS
        ):
            raise AssertionError("watchdog fired waiting for scheduler break")


def test_a_job_queued_behind_a_failing_call_is_never_started() -> None:
    failing, queued = jobs(2)
    started: list[JobId] = []
    failing_arrived = threading.Event()
    release_failing = threading.Event()

    def explode_on_the_failing_call(
        job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        started.append(job.job_id)
        failing_arrived.set()
        wait_for(release_failing, what="the failing call to be released")
        raise ExecutorFailure(
            "machinery failed",
            code=ExecutorFailureCode.RECORDING_OPERATION_FAILED,
        )

    scheduler: ExecutionScheduler[None] = ExecutionScheduler(
        executor=FakeExecutor(responder=explode_on_the_failing_call),
        capacity=1,
    )
    queued_token = CancelToken()
    try:
        assert scheduler.admit(failing, None) is AdmissionResult.ADMITTED
        wait_for(failing_arrived, what="the failing call to start")
        # The bound is full and the one worker is inside the failing
        # call, so this is the window a break must not dispatch out of.
        with scheduler._condition:
            scheduler._tokens[_QUEUED_TICKET] = queued_token
            scheduler._pending.append(
                _Admitted(_QUEUED_TICKET, queued, None, queued_token)
            )

        release_failing.set()
        with pytest.raises(SchedulerBroken):
            scheduler.take_completion()
    finally:
        release_failing.set()
        scheduler.shutdown()

    assert started == [failing.job_id]
    assert queued_token.cancelled


class _BreakAfterBuffering:
    def __init__(self, failing: JobId, /) -> None:
        self._failing = failing
        self._arrived: dict[JobId, threading.Event] = {}
        self._release: dict[JobId, threading.Event] = {}
        self._executor_returned: dict[JobId, threading.Event] = {}
        self._lock = threading.Lock()

    def __call__(
        self, job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        self.arrived(job.job_id).set()
        wait_for(self.gate(job.job_id), what=f"job {job.job_id} to release")
        try:
            if job.job_id == self._failing:
                raise ExecutorFailure(
                    "machinery failed",
                    code=ExecutorFailureCode.RECORDING_OPERATION_FAILED,
                )
            return completion_for(job.job_id)
        finally:
            self.executor_returned(job.job_id).set()

    def arrived(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._arrived.setdefault(job_id, threading.Event())

    def gate(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._release.setdefault(job_id, threading.Event())

    def executor_returned(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._executor_returned.setdefault(
                job_id, threading.Event()
            )

    def await_arrivals(self, *job_ids: JobId) -> None:
        for job_id in job_ids:
            wait_for(self.arrived(job_id), what=f"job {job_id} to start")

    def release_successes(self, *job_ids: JobId) -> None:
        for job_id in job_ids:
            self.gate(job_id).set()
        for job_id in job_ids:
            wait_for(
                self.executor_returned(job_id),
                what=f"job {job_id} to return from its executor responder",
            )

    def break_the_pool(self) -> None:
        self.await_arrivals(self._failing)
        self.gate(self._failing).set()
        wait_for(
            self.executor_returned(self._failing),
            what="the failing executor responder to return",
        )


def test_a_break_delivers_the_buffered_tail_before_it_raises() -> None:
    first, second, failing = jobs(3)
    responder = _BreakAfterBuffering(failing.job_id)
    pool = fixed_pool(FakeExecutor(responder=responder), 4)
    parked = threading.Event()
    may_proceed = threading.Event()
    delivered: list[int] = []

    async def parking_source() -> AsyncIterator[ExecutionSubmission[int]]:
        for index, job in enumerate((first, second, failing)):
            yield ExecutionSubmission(job=job, context=index)
        parked.set()
        await asyncio.to_thread(
            wait_for, may_proceed, what="the parked pull to be released"
        )

    async def stream() -> None:
        async with pool:
            consumer = asyncio.create_task(
                _collect_contexts(pool.run_stream(parking_source()), delivered)
            )
            await asyncio.to_thread(
                wait_for, parked, what="the source to park past its last job"
            )
            scheduler = pool._scheduler
            assert scheduler is not None
            scheduler._notify_change = None
            try:
                # Hold the owning loop while worker-thread gates advance so
                # the consumer cannot take the completions being buffered.
                responder.release_successes(first.job_id, second.job_id)
                _await_scheduler_publication(
                    scheduler,
                    first.job_id,
                    second.job_id,
                )
                responder.break_the_pool()
                _await_scheduler_break(scheduler)
            finally:
                scheduler._notify_change = pool._notify_scheduler_change
                pool._wake_streams()
                may_proceed.set()
            await asyncio.wait_for(consumer, WATCHDOG_SECONDS)

    with pytest.raises(ExecutorFailure, match="the execution pool broke") as e:
        asyncio.run(stream())

    # Both buffered completions were handed over, each with its own
    # submission's context, and only then did the break surface. Which of
    # the two came first is completion order, pinned elsewhere; what is
    # pinned here is that neither was dropped and the raise came last.
    assert sorted(delivered) == [0, 1]
    assert isinstance(e.value.__cause__, ExecutorFailure)
    assert pool.state is ExecutionPoolState.BROKEN


def test_a_batch_break_delivers_the_buffered_tail_before_it_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, second, failing = jobs(3)
    responder = _BreakAfterBuffering(failing.job_id)
    captured: list[ExecutionScheduler[object]] = []

    class _CapturingScheduler(ExecutionScheduler[object]):
        def __init__(self, *, executor: Executor, capacity: int) -> None:
            super().__init__(executor=executor, capacity=capacity)
            captured.append(self)

    monkeypatch.setattr(
        "dr_exec.scheduling.scheduler.ExecutionScheduler", _CapturingScheduler
    )
    parked = threading.Event()
    may_proceed = threading.Event()

    def parking_source() -> Iterator[ExecutionJob]:
        yield from (first, second, failing)
        parked.set()
        wait_for(may_proceed, what="the parked pull to be released")

    driver = batch_of(
        FakeExecutor(responder=responder), parking_source(), slots=4
    )
    delivered: list[JobId] = []
    failure: BaseException | None = None

    def consume_the_batch() -> None:
        nonlocal failure
        try:
            for completed in driver:
                delivered.append(completed.result.execution_id.job_id)
        except BaseException as raised:  # noqa: BLE001
            failure = raised
        finally:
            driver.close()

    consumer = in_thread(consume_the_batch)
    wait_for(parked, what="the source to park past its last job")
    assert len(captured) == 1
    scheduler = captured[0]
    responder.release_successes(first.job_id, second.job_id)
    _await_scheduler_publication(scheduler, first.job_id, second.job_id)
    responder.break_the_pool()
    _await_scheduler_break(scheduler)
    may_proceed.set()
    join(consumer)

    assert sorted(delivered) == sorted([first.job_id, second.job_id])
    assert isinstance(failure, SchedulerBroken)
    assert isinstance(failure.__cause__, ExecutorFailure)


@pytest.mark.parametrize("surface", ["batch", "stream"])
def test_a_worker_that_cannot_start_breaks_the_pool_rather_than_hanging(
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    class _UnstartableThread(threading.Thread):
        def start(self) -> None:
            raise RuntimeError("can't start new thread")

    # Patched at the scheduler's own module reference rather than on
    # `threading.Thread`: `asyncio.to_thread` builds threads too, and
    # breaking those would wedge the pool for an unrelated reason and
    # invalidate the case.
    monkeypatch.setattr(
        "dr_exec.scheduling.scheduler.Thread", _UnstartableThread
    )

    batch = jobs(2)
    failure: BaseException | None = None

    def drive_the_batch() -> None:
        nonlocal failure
        generator = batch_of(immediate_executor(), batch, slots=2)
        try:
            list(generator)
        except BaseException as raised:  # noqa: BLE001
            failure = raised
        finally:
            generator.close()

    def drive_the_stream() -> None:
        nonlocal failure

        async def stream() -> None:
            pool = fixed_pool(immediate_executor(), 2)
            async with pool:
                await consume(pool.run_stream(submissions_of(batch)))

        try:
            asyncio.run(stream())
        except BaseException as raised:  # noqa: BLE001
            failure = raised

    join(
        in_thread(drive_the_batch if surface == "batch" else drive_the_stream)
    )

    assert isinstance(failure, SchedulerBroken)
    assert isinstance(failure.__cause__, RuntimeError)


@pytest.mark.parametrize("close_kind", ["drain", "abort"])
def test_a_close_landing_mid_pull_ends_the_stream_without_a_failure(
    close_kind: str,
) -> None:
    parked = asyncio.Event()
    may_proceed = asyncio.Event()

    async def parking_source() -> AsyncIterator[ExecutionSubmission[int]]:
        yield ExecutionSubmission(job=jobs(1)[0], context=0)
        parked.set()
        await may_proceed.wait()
        yield ExecutionSubmission(job=jobs(1)[0], context=1)

    pool = fixed_pool(immediate_executor(), 2)
    delivered: list[int] = []

    async def close_mid_pull() -> None:
        async with pool:
            scheduler = pool._scheduler
            assert scheduler is not None
            stream = pool.run_stream(parking_source())
            consumer = asyncio.create_task(
                _collect_contexts(stream, delivered)
            )
            await asyncio.wait_for(parked.wait(), WATCHDOG_SECONDS)
            close = pool.drain if close_kind == "drain" else pool.abort
            closing = asyncio.create_task(close())
            await asyncio.to_thread(_await_closed_intake, scheduler)
            may_proceed.set()
            await asyncio.wait_for(consumer, WATCHDOG_SECONDS)
            await asyncio.wait_for(closing, WATCHDOG_SECONDS)

    asyncio.run(close_mid_pull())

    assert 1 not in delivered
    assert pool.state is ExecutionPoolState.CLOSED


async def _collect_contexts(
    stream: AsyncIterator[ExecutionCompletion[int]],
    into: list[int],
    /,
) -> None:
    async for completion in stream:
        into.append(completion.context)


def test_concurrent_feeders_racing_the_last_slot_never_exceed_the_bound() -> (
    None
):
    feeders = 4
    capacity = 1
    all_parked = asyncio.Event()
    may_admit = asyncio.Event()
    parked = 0

    class _PeakRecordingScheduler(ExecutionScheduler[object]):
        peak_residents = 0

        def admit(
            self, job: ExecutionJob, context: object, /
        ) -> AdmissionResult:
            result = super().admit(job, context)
            with self._condition:
                type(self).peak_residents = max(
                    type(self).peak_residents, self._residents
                )
            return result

    async def racing_source(
        which: int, /
    ) -> AsyncIterator[ExecutionSubmission[int]]:
        nonlocal parked
        parked += 1
        if parked == feeders:
            all_parked.set()
        await may_admit.wait()
        yield ExecutionSubmission(job=jobs(1)[0], context=which)

    pool = fixed_pool(immediate_executor(), capacity)
    delivered: list[int] = []

    async def race_for_the_last_slot() -> None:
        async with pool:
            pool._scheduler = _PeakRecordingScheduler(
                executor=immediate_executor(),
                capacity=capacity,
                notify_change=pool._notify_scheduler_change,
            )
            streams = [
                asyncio.create_task(
                    _collect_contexts(
                        pool.run_stream(racing_source(which)), delivered
                    )
                )
                for which in range(feeders)
            ]
            await asyncio.wait_for(all_parked.wait(), WATCHDOG_SECONDS)
            # Every feeder has now observed room and is one resumption
            # away from admitting. Releasing them together is the exact
            # interleaving that over-admits if admission trusts that
            # stale observation.
            may_admit.set()
            await asyncio.wait_for(asyncio.gather(*streams), WATCHDOG_SECONDS)

    asyncio.run(race_for_the_last_slot())

    assert _PeakRecordingScheduler.peak_residents <= capacity
    assert sorted(delivered) == list(range(feeders))


def test_cancellation_tokens_are_bounded_by_capacity_not_by_history() -> None:
    capacity = 2
    delivered = 0
    peak_tokens = 0
    batch = jobs(40)
    scheduler: ExecutionScheduler[None] = ExecutionScheduler(
        executor=immediate_executor(), capacity=capacity
    )
    source = iter(batch)
    try:
        while True:
            while scheduler.can_admit():
                job = next(source, None)
                if job is None:
                    break
                scheduler.admit(job, None)
            if scheduler.take_completion() is None:
                break
            delivered += 1
            peak_tokens = max(peak_tokens, len(scheduler._tokens))
    finally:
        scheduler.close_intake()
        scheduler.wait_for_quiescence()
        scheduler.shutdown()

    assert delivered == len(batch)
    assert peak_tokens <= capacity


def test_a_finite_batch_yields_every_one_of_its_completions() -> None:
    batch = jobs(7)

    completed = list(batch_of(immediate_executor(), batch, slots=2))

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }


def test_a_batch_wall_time_cancels_inflight_queued_and_remaining_jobs() -> (
    None
):
    executor, responder = gated_executor(cancellation_aware=True)
    batch = jobs(3)
    collected: list[CompletedExecution] = []

    def drain() -> None:
        collected.extend(
            run_batch(
                executor,
                batch,
                capacity=1,
                wall_time=FiniteDurationLimit(max_ns=250_000_000),
            )
        )

    driver = in_thread(drain)
    responder.await_arrival(batch[0].job_id)
    join(driver)

    assert {one.result.execution_id.job_id for one in collected} == {
        job.job_id for job in batch
    }
    assert all(
        isinstance(one.result.outcome, CancelledOutcome) for one in collected
    )
    responder.assert_no_watchers()


def test_a_batch_that_finishes_before_its_wall_time_is_not_cancelled() -> None:
    batch = jobs(3)

    completed = list(
        batch_of(
            immediate_executor(),
            batch,
            slots=2,
            wall_time=FiniteDurationLimit(max_ns=30_000_000_000),
        )
    )

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }
    assert all(
        not isinstance(one.result.outcome, CancelledOutcome)
        for one in completed
    )


def test_a_finite_batch_consumes_its_input_lazily() -> None:
    batch = jobs(2)
    pulled = 0

    def counting() -> Iterator[ExecutionJob]:
        nonlocal pulled
        for job in batch:
            pulled += 1
            yield job
        raise AssertionError("the batch pulled beyond resident capacity")

    stream = batch_of(immediate_executor(), counting(), slots=2)
    next(stream)
    observed = pulled
    stream.close()

    assert observed <= 2


def test_a_batch_wall_time_still_cancels_during_early_close_drain() -> None:
    batch = jobs(2)
    first, held = batch
    responder = GatedResponder(cancellation_aware=True)

    def respond(
        job: ExecutionJob, token: CancelToken | None, /
    ) -> CompletedExecution:
        if job.job_id == first.job_id:
            return completion_for(job.job_id)
        return responder(job, token)

    stream = run_batch(
        FakeExecutor(responder=respond),
        batch,
        capacity=2,
        wall_time=FiniteDurationLimit(max_ns=250_000_000),
    )

    primer = in_thread(lambda: next(stream, None))
    responder.await_arrival(held.job_id)
    join(primer)

    closer = in_thread(stream.close)
    join(closer)

    assert responder.cancelled == (held.job_id,)
    assert responder.executor_returned_gate(held.job_id).is_set()
    responder.assert_no_watchers()


def test_an_abandoned_batch_still_drains_its_admitted_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor, responder = gated_executor()
    batch = jobs(2)
    first, held = batch
    captured: list[ExecutionScheduler[object]] = []

    class _CapturingScheduler(ExecutionScheduler[object]):
        def __init__(self, *, executor: Executor, capacity: int) -> None:
            super().__init__(executor=executor, capacity=capacity)
            captured.append(self)

    monkeypatch.setattr(
        "dr_exec.scheduling.scheduler.ExecutionScheduler", _CapturingScheduler
    )
    stream = batch_of(executor, batch, slots=2)

    primer = in_thread(lambda: next(stream, None))
    responder.await_arrival_count(2)
    responder.release(first.job_id)
    join(primer)
    assert len(captured) == 1
    scheduler = captured[0]

    closer = in_thread(stream.close)
    _await_closed_intake(scheduler)
    assert not responder.executor_returned_gate(held.job_id).is_set()
    responder.release(held.job_id)
    join(closer)

    for job in batch:
        assert responder.executor_returned_gate(job.job_id).is_set()
    responder.assert_no_watchers()


@pytest.mark.parametrize("surface", ["batch", "stream"])
def test_each_driver_handles_an_empty_source(surface: str) -> None:
    if surface == "batch":
        assert list(batch_of(immediate_executor(), [], slots=1)) == []
        return

    pool = fixed_pool(immediate_executor(), 1)

    async def stream() -> list[object]:
        async with pool:
            return [
                completion
                async for completion in pool.run_stream(submissions_of([]))
            ]

    assert asyncio.run(stream()) == []


def test_map_stream_defaults_its_width_to_the_pool_capacity() -> None:
    executor, responder = gated_executor()
    batch = jobs(5)
    pool = fixed_pool(executor, 2)
    source = recording_source(batch)

    async def stream() -> list[int]:
        async with pool:
            return [
                completion.context
                async for completion in pool.map_stream(source)
            ]

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(2)
    assert source.pulled == 2
    responder.release_all(batch)
    join(streamer)


def test_map_stream_holds_a_narrower_width_than_the_pool_capacity() -> None:
    executor, responder = gated_executor()
    batch = jobs(4)
    pool = fixed_pool(executor, 4)
    source = recording_source(batch)

    async def stream() -> list[int]:
        async with pool:
            return [
                completion.context
                async for completion in pool.map_stream(source, concurrency=1)
            ]

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival(batch[0].job_id)
    source.await_pulled(1)
    assert source.pulled == 1
    assert responder.peak_active == 1
    responder.release_all(batch)
    join(streamer)

    assert responder.peak_active == 1


def test_map_stream_yields_every_submission_exactly_once() -> None:
    batch = jobs(6)
    pool = fixed_pool(immediate_executor(), 3)

    async def stream() -> list[int]:
        async with pool:
            return [
                completion.context
                async for completion in pool.map_stream(submissions_of(batch))
            ]

    assert sorted(asyncio.run(stream())) == list(range(6))


def test_map_stream_accepts_a_synchronous_iterable() -> None:
    batch = jobs(3)
    pool = fixed_pool(immediate_executor(), 2)
    submissions = [
        ExecutionSubmission(job=job, context=index)
        for index, job in enumerate(batch)
    ]

    async def stream() -> list[int]:
        async with pool:
            return [
                completion.context
                async for completion in pool.map_stream(submissions)
            ]

    assert sorted(asyncio.run(stream())) == [0, 1, 2]


def test_map_stream_refuses_a_non_positive_width() -> None:
    pool = fixed_pool(immediate_executor(), 1)

    async def stream() -> None:
        async with pool:
            stream_iterator = pool.map_stream(
                submissions_of(jobs(1)), concurrency=0
            )
            await anext(stream_iterator)

    with pytest.raises(ValueError, match="concurrency"):
        asyncio.run(stream())


def test_map_stream_delivers_in_completion_order() -> None:
    executor, responder = gated_executor()
    batch = jobs(3)
    pool = fixed_pool(executor, 3)
    delivered: queue.SimpleQueue[int] = queue.SimpleQueue()

    async def stream() -> list[int]:
        async with pool:
            collected_contexts: list[int] = []
            async for completion in pool.map_stream(submissions_of(batch)):
                collected_contexts.append(completion.context)
                delivered.put(completion.context)
            return collected_contexts

    collected: list[list[int]] = []
    streamer = in_thread(lambda: collected.append(asyncio.run(stream())))

    responder.await_arrival_count(3)
    for index in (2, 0, 1):
        responder.release(batch[index].job_id)
        # The order under test is delivery order, so each job must be handed
        # to the caller before the next is released. Synchronizing on what the
        # stream delivered -- rather than on the scheduler's ready queue, which
        # this very consumer drains -- is state the test cannot miss.
        assert delivered.get(timeout=WATCHDOG_SECONDS) == index
    join(streamer)

    assert collected == [[2, 0, 1]]


def test_two_map_streams_on_one_pool_each_yield_only_their_own() -> None:
    """A shared pool must not let one map stream consume another's work.

    Both streams are driven to completion; each submission is owed exactly one
    delivery to the stream that made it. Before completions were routed by
    submitting stream, permits and completions migrated between streams and
    one of them could never finish.
    """

    executor, responder = gated_executor()
    first = jobs(3)
    second = jobs(3)
    pool = fixed_pool(executor, 6)

    async def stream() -> tuple[list[str], list[str]]:
        async def submissions(
            batch: list[ExecutionJob], tag: str, /
        ) -> AsyncIterator[ExecutionSubmission[str]]:
            for index, job in enumerate(batch):
                yield ExecutionSubmission(job=job, context=f"{tag}-{index}")

        async def drain(
            source: AsyncIterator[ExecutionCompletion[str]], /
        ) -> list[str]:
            return [completion.context async for completion in source]

        async with pool:
            return await asyncio.gather(
                drain(pool.map_stream(submissions(first, "first"))),
                drain(pool.map_stream(submissions(second, "second"))),
            )

    collected: list[tuple[list[str], list[str]]] = []
    streamer = in_thread(lambda: collected.append(asyncio.run(stream())))

    responder.await_arrival_count(len(first) + len(second))
    for job in (*first, *second):
        responder.release(job.job_id)
    join(streamer)

    first_contexts, second_contexts = collected[0]
    assert sorted(first_contexts) == [f"first-{index}" for index in range(3)]
    assert sorted(second_contexts) == [f"second-{index}" for index in range(3)]


def test_releasing_a_departed_owners_completions_frees_their_capacity() -> (
    None
):
    """A completion no surviving driver can claim must not hold capacity.

    An owned completion is claimable by exactly one driver, so once that
    driver leaves it is unreachable while still counting against the shared
    resident bound — and a plain stream, which ends only once no resident
    remains, would wait on it forever.
    """

    scheduler: ExecutionScheduler[object] = ExecutionScheduler(
        executor=immediate_executor(), capacity=3
    )
    owner = _StreamOwner()
    stranded: _Completion[object] = _Completion(
        _QUEUED_TICKET,
        completion_for(jobs(1)[0].job_id),
        _OwnedContext(owner=owner, context="stranded"),
    )
    scheduler._ready.append(stranded)
    scheduler._residents += 1

    # No surviving driver can claim it: a plain stream skips owned work, and
    # every other map stream owns a different tag.
    assert scheduler.take_completion_nowait(owned_by=_unowned) is None
    assert scheduler.has_residents()

    # Departing is what must release it.
    scheduler.release_owned(owner.owns)

    assert not scheduler.has_residents()


def test_a_map_stream_releases_its_ownership_when_it_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The release must be wired to the end of every map stream."""

    releases: list[object] = []

    class _RecordingScheduler(ExecutionScheduler[object]):
        def release_owned(self, owned_by: Callable[[object], bool], /) -> None:
            releases.append(owned_by)
            super().release_owned(owned_by)

    monkeypatch.setattr(
        "dr_exec.scheduling.pool.ExecutionScheduler", _RecordingScheduler
    )
    pool = fixed_pool(immediate_executor(), 2)

    async def stream() -> None:
        async with pool:
            async for _ in pool.map_stream(submissions_of(jobs(2))):
                pass

    asyncio.run(asyncio.wait_for(stream(), WATCHDOG_SECONDS))

    assert len(releases) == 1


def test_a_break_reaches_a_map_stream_holding_no_completion_of_its_own() -> (
    None
):
    """Another stream's buffered work must not mask a scheduler break.

    The broken scheduler can never hand this stream its own completions, so a
    stream that waits for them instead of raising would wait forever.
    """

    scheduler: ExecutionScheduler[object] = ExecutionScheduler(
        executor=FakeExecutor(), capacity=4
    )
    foreign: _Completion[object] = _Completion(
        _QUEUED_TICKET, completion_for(jobs(1)[0].job_id), "foreign"
    )
    scheduler._ready.append(foreign)
    scheduler._residents += 1
    scheduler._break(RuntimeError("injected"))

    with pytest.raises(SchedulerBroken):
        scheduler.take_completion_nowait(owned_by=lambda _: False)


def test_a_plain_stream_never_consumes_a_map_streams_completions() -> None:
    """A map stream's submissions are owed to it, not to a sharing stream."""

    executor, responder = gated_executor()
    mapped = jobs(3)
    plain = jobs(3)
    pool = fixed_pool(executor, 6)

    async def stream() -> tuple[list[str], list[int]]:
        async def mapped_submissions() -> AsyncIterator[
            ExecutionSubmission[str]
        ]:
            for index, job in enumerate(mapped):
                yield ExecutionSubmission(job=job, context=f"mapped-{index}")

        async def drain_mapped(
            source: AsyncIterator[ExecutionCompletion[str]], /
        ) -> list[str]:
            return [completion.context async for completion in source]

        async def drain_plain(
            source: AsyncIterator[ExecutionCompletion[int]], /
        ) -> list[int]:
            return [completion.context async for completion in source]

        async with pool:
            return await asyncio.gather(
                drain_mapped(pool.map_stream(mapped_submissions())),
                drain_plain(pool.run_stream(submissions_of(plain))),
            )

    collected: list[tuple[list[str], list[int]]] = []
    streamer = in_thread(lambda: collected.append(asyncio.run(stream())))

    responder.await_arrival_count(len(mapped) + len(plain))
    for job in (*mapped, *plain):
        responder.release(job.job_id)
    join(streamer)

    mapped_contexts, plain_contexts = collected[0]
    assert sorted(mapped_contexts) == [f"mapped-{i}" for i in range(3)]
    assert sorted(plain_contexts) == [0, 1, 2]
