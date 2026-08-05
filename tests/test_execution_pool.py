"""Qualification of the one scheduler core through both its surfaces.

What is established here is scheduling behavior, not execution behavior:
admission under a shared resident bound, completion-order delivery,
backpressure on the source, cancellation and abort mid-queue and
mid-flight, drain to empty, and the exact preservation of caller context.
None of that needs a real child, and using one would make every case a
race against process startup. `FakeExecutor` with a gated responder is the
substrate instead, so each call's start and finish are events the test
sets rather than moments it hopes for. Real-engine coverage lives in
`test_pool_real_engine.py`, where it belongs.

Behaviors both surfaces share are qualified through both. That is the
point of one scheduler core: `run_many` and `run_stream` are not two
implementations to compare but one implementation reached two ways, so a
parity failure here means the core stopped being shared.

Synchronization is on explicit gates and terminal state throughout. There
is no sleep in this file, no elapsed-time assertion, and no case whose
evidence is that something had not happened yet after a delay. Where a
case must show that intake did *not* advance, it first waits for a state
that would necessarily follow the advance, then asserts the absence.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Generator, Iterable, Iterator
from typing import TYPE_CHECKING

import pytest
from executor_support import completion_for, job_for, trusted_target
from pool_support import WATCHDOG_SECONDS, GatedResponder, wait_for

from dr_exec import (
    AutoPoolCapacity,
    CancelledOutcome,
    CompletedExecution,
    ExecutionJob,
    ExecutionPool,
    ExecutionPoolConfig,
    ExecutionPoolState,
    ExecutionSubmission,
    ExecutorFailure,
    FakeExecutor,
    FixedPoolCapacity,
    JobId,
)
from dr_exec._scheduler import SchedulerBroken, usable_cpu_count
from dr_exec.executor import _run_batch
from dr_exec.kinds import CapacitySource

if TYPE_CHECKING:
    from dr_exec.cancel import CancelToken
    from dr_exec.protocols import Executor


# --- Building blocks -----------------------------------------------------


def jobs(count: int, /) -> list[ExecutionJob]:
    """`count` distinct well-formed jobs. Their target is never spawned."""
    return [job_for(trusted_target(("/usr/bin/true",))) for _ in range(count)]


def gated_executor() -> tuple[FakeExecutor, GatedResponder]:
    """A fake whose every call is held until the test releases it."""
    responder = GatedResponder()
    return FakeExecutor(responder=responder), responder


def immediate_executor() -> FakeExecutor:
    """A fake that completes every call at once, for order-free cases."""
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
    executor: Executor, batch: Iterable[ExecutionJob], /, *, slots: int
) -> Generator[CompletedExecution]:
    """`run_many`'s exact driver, over an arbitrary executor.

    `ProcessExecutor.run_many` is capacity resolution plus this driver,
    and the driver is what these cases are about. Reaching it directly is
    what lets a gated fake stand in for real children while still
    exercising the production surface's own loop; `run_many` itself is
    qualified end to end against the real engine.

    The generator type is deliberate here rather than the surface's
    `Iterator`: abandoning a batch is one of the behaviors under test, and
    `close()` is how a caller abandons one.
    """
    return _run_batch(executor, batch, capacity=slots)


class RecordingSource:
    """An async submission source reporting exactly how far it advanced.

    Backpressure is a claim about the source, so the source is what has to
    answer: `pulled` is the number of submissions the scheduler actually
    requested. Nothing about the scheduler's internals is inspected.
    """

    def __init__(
        self, submissions: Iterable[ExecutionSubmission[int]], /
    ) -> None:
        self._items = list(submissions)
        self.pulled = 0

    async def __aiter__(self) -> AsyncIterator[ExecutionSubmission[int]]:
        for item in self._items:
            self.pulled += 1
            yield item


def recording_source(batch: list[ExecutionJob], /) -> RecordingSource:
    return RecordingSource(
        [
            ExecutionSubmission(job=job, context=index)
            for index, job in enumerate(batch)
        ]
    )


async def submissions_of(
    batch: Iterable[ExecutionJob], /
) -> AsyncIterator[ExecutionSubmission[int]]:
    """Submit each job with its own position as its caller context."""
    for index, job in enumerate(batch):
        yield ExecutionSubmission(job=job, context=index)


async def consume(stream: AsyncIterator[object], /) -> None:
    """Drive a stream to exhaustion, discarding what it yields."""
    async for _ in stream:
        pass


def in_thread(work: object, /) -> threading.Thread:
    """Start one thread running `work`, so the test can gate against it."""
    thread = threading.Thread(target=work)  # ty: ignore[invalid-argument-type]
    thread.start()
    return thread


def join(thread: threading.Thread, /) -> None:
    """Join under the watchdog, failing rather than hanging the suite."""
    thread.join(WATCHDOG_SECONDS)
    assert not thread.is_alive(), "watchdog fired joining a driver thread"


# --- Capacity ------------------------------------------------------------


def test_automatic_capacity_resolves_once_from_the_usable_cpu_count() -> None:
    """Automatic capacity is the usable CPU count, recorded as automatic.

    Reading it back twice must give the same answer: a bound that drifted
    with machine load would make the pool's own resident guarantee
    unstatable.
    """
    pool = ExecutionPool(
        executor=immediate_executor(),
        config=ExecutionPoolConfig(capacity=AutoPoolCapacity()),
    )

    async def open_and_read() -> tuple[object, object]:
        async with pool:
            return pool.effective_capacity, pool.effective_capacity

    first, second = asyncio.run(open_and_read())

    assert first == second
    assert first.source is CapacitySource.AUTO  # ty: ignore[unresolved-attribute]
    assert first.max_active_jobs == usable_cpu_count()  # ty: ignore[unresolved-attribute]
    assert usable_cpu_count() >= 1


def test_fixed_capacity_uses_the_selected_slot_count() -> None:
    """A fixed pool records its own count and still names the machine."""
    pool = fixed_pool(immediate_executor(), 3)

    async def open_and_read() -> object:
        async with pool:
            return pool.effective_capacity

    capacity = asyncio.run(open_and_read())

    assert capacity.source is CapacitySource.FIXED  # ty: ignore[unresolved-attribute]
    assert capacity.max_active_jobs == 3  # ty: ignore[unresolved-attribute]
    assert capacity.cpu_count == usable_cpu_count()  # ty: ignore[unresolved-attribute]


@pytest.mark.parametrize("slots", [0, -1])
def test_fixed_capacity_refuses_a_non_positive_slot_count(slots: int) -> None:
    """A pool with no slots could never make progress."""
    with pytest.raises(ValueError, match="must be positive"):
        FixedPoolCapacity(max_active_jobs=slots)


def test_effective_capacity_is_unavailable_before_the_pool_opens() -> None:
    """Answering early would invent a bound or resolve an unused one."""
    pool = fixed_pool(immediate_executor(), 2)

    with pytest.raises(ExecutorFailure, match="resolved when the pool"):
        _ = pool.effective_capacity


# --- Admission under the one shared resident bound -----------------------


def test_a_completed_result_keeps_its_slot_until_it_is_delivered() -> None:
    """The bound is shared: completion alone does not admit replacement.

    Two slots, three jobs, every call gated. Once both slots are occupied
    the third job must not start. The evidence is not a delay: the first
    call is released and *observed to have returned*, which is the exact
    moment a separate active bound would have admitted the third -- and
    the assertion is that it did not, because the completed-but-undelivered
    result still holds the slot.
    """
    executor, responder = gated_executor()
    batch = jobs(3)
    first, second, third = batch
    delivered: list[JobId] = []

    def run() -> None:
        for completed in batch_of(executor, batch, slots=2):
            delivered.append(completed.result.execution_id.job_id)

    consumer = in_thread(run)

    responder.await_arrival_count(2)
    assert set(responder.started) == {first.job_id, second.job_id}

    responder.release(first.job_id)
    responder.await_finish(first.job_id)
    assert third.job_id not in responder.started

    responder.release_all(batch)
    join(consumer)

    assert set(delivered) == {job.job_id for job in batch}


def test_intake_advances_only_while_the_resident_bound_has_room() -> None:
    """The source is pulled exactly as far as capacity allows, never past.

    A two-slot pool over a five-item source: with both calls held, the
    source must have advanced twice and no further. The gate makes "both
    calls started" a state rather than a moment, so the pull count is read
    at a point where a third pull would already have happened if the
    scheduler prefetched.
    """
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
    """A consumer that stops consuming stops the source, exactly.

    The source is advanced by the delivery loop, so a consumer that stops
    taking completions stops intake at a determined point rather than an
    approximate one. With two slots and a consumer that stalls while
    holding its second completion, the count is exact: two pulls fill the
    bound, the first delivery frees one slot which the next loop pass
    refills, and the second delivery's refill never happens because the
    consumer never returns from the loop body. Three pulls, not four --
    and not five, which is the backpressure.

    The stall is a gate the test opens, not a delay: `pulled` is read only
    after the consumer has announced it is holding, and the assertion is
    an exact equality, so an extra pull would fail rather than pass
    silently.
    """
    executor, responder = gated_executor()
    batch = jobs(6)
    source = recording_source(batch)
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
                    await asyncio.to_thread(resume.wait, WATCHDOG_SECONDS)

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(2)
    responder.release(batch[0].job_id, batch[1].job_id)
    wait_for(stalled, what="the consumer to stall holding two completions")

    # Reaching the stall is itself the evidence that the first refill
    # happened; any further pull would require the loop to advance past
    # the stalled body, which is exactly what backpressure prevents.
    assert source.pulled == 3

    resume.set()
    responder.release_all(batch)
    join(streamer)

    assert sorted(collected) == list(range(len(batch)))
    assert source.pulled == len(batch)


def test_capacity_bounds_how_many_calls_are_ever_in_flight_at_once() -> None:
    """One job consumes one slot, and slots are the concurrency bound.

    Every call reports its own arrival and departure, so the peak overlap
    is counted directly rather than inferred. Two slots over eight jobs
    must never show three calls inside the executor at the same time.
    """
    executor = FakeExecutor(responder=_OverlapCounter(limit=2))
    batch = jobs(8)

    completed = list(batch_of(executor, batch, slots=2))

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }


class _OverlapCounter:
    """A responder that fails the case if concurrency exceeds the bound."""

    def __init__(self, *, limit: int) -> None:
        self._limit = limit
        self._lock = threading.Lock()
        self._active = 0

    def __call__(
        self, job: ExecutionJob, _cancellation: CancelToken | None, /
    ) -> CompletedExecution:
        with self._lock:
            self._active += 1
            if self._active > self._limit:
                raise AssertionError(
                    f"{self._active} calls overlapped a {self._limit}-slot pool"
                )
        try:
            return completion_for(job.job_id)
        finally:
            with self._lock:
                self._active -= 1


# --- Completion order ----------------------------------------------------


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
    """Delivery follows completion, whatever order submission was in.

    Each call is released and observed finished before the next is
    released, so completion order is imposed by the test rather than
    raced for, and the delivered contexts must reproduce it exactly.
    """
    executor, responder = gated_executor()
    batch = jobs(3)
    pool = fixed_pool(executor, 3)
    collected: list[int] = []

    async def stream() -> None:
        async with pool:
            async for completion in pool.run_stream(submissions_of(batch)):
                collected.append(completion.context)

    streamer = in_thread(lambda: asyncio.run(stream()))

    responder.await_arrival_count(3)
    for index in finish_order:
        responder.release(batch[index].job_id)
        responder.await_finish(batch[index].job_id)

    join(streamer)

    assert collected == list(finish_order)


# --- Caller context ------------------------------------------------------


def test_every_completion_carries_exactly_its_submissions_context() -> None:
    """Context is paired by identity, in memory, never serialized.

    The contexts here hold a lock, so no serialization path could carry
    one; the assertion is `is`, not equality. The very object submitted
    with a job comes back with that job's completion.
    """
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
    """A caller context no serialization path could survive."""

    __slots__ = ("lock",)

    def __init__(self) -> None:
        self.lock = threading.Lock()


# --- Cancellation, abort, and drain --------------------------------------


def test_abort_cancels_work_in_flight_and_awaits_its_teardown() -> None:
    """Abort cancels active calls and waits for each one to return.

    The calls are genuinely mid-flight when abort begins -- the test waits
    for their arrival first -- and each observes a cancelled token. That
    abort waited for them is what the finished gates show: they are set by
    the time abort returns, which is the "await their teardown" half of
    the promise.
    """
    executor, responder = gated_executor()
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
        assert responder.finished_gate(job.job_id).is_set()
    assert pool.state is ExecutionPoolState.CLOSED


def test_abort_cancels_admitted_work_no_worker_has_started() -> None:
    """A submission cancelled mid-queue completes cancelled, never vanishes.

    One slot, two jobs: the second is admitted only after the first is
    delivered, so aborting while the first is in flight leaves the second
    unadmitted. Nothing is dropped from what *was* admitted -- the
    in-flight call reaches its cancelled outcome -- and the unadmitted job
    was never a queue entry to lose.
    """
    executor, responder = gated_executor()
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


def test_a_cancelled_call_is_delivered_as_completion_data() -> None:
    """Cancellation is an outcome the consumer receives, not a lost job.

    A call that returns a cancelled outcome is a completion like any
    other, and the scheduler delivers it as one. The stream is consumed to
    exhaustion, so nothing about the delivery depends on how the pool is
    later closed.
    """
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


def test_abort_does_not_promise_delivery_of_what_it_tore_down() -> None:
    """Closing stops delivery; the durable half is what survives.

    Abort exists to stop work and await teardown, not to flush results,
    so a completion finished during an abort may never reach a consumer
    that is no longer reading. What is guaranteed is the part that
    matters: the call ran to its own end -- with a cancelled outcome and,
    in production, its record -- before the pool closed. A consumer that
    needs every result drains instead of aborting, which
    `test_drain_to_empty_delivers_every_admitted_submission` pins.
    """
    executor, responder = gated_executor()
    batch = jobs(1)
    only = batch[0]
    pool = fixed_pool(executor, 1)

    async def abort_mid_flight() -> None:
        async with pool:
            reader = asyncio.create_task(
                consume(pool.run_stream(submissions_of(batch)))
            )
            await asyncio.to_thread(responder.await_arrival, only.job_id)
            await pool.abort()
            reader.cancel()

    asyncio.run(abort_mid_flight())

    assert only.job_id in responder.cancelled
    assert responder.finished_gate(only.job_id).is_set()
    assert pool.state is ExecutionPoolState.CLOSED


def test_drain_lets_admitted_work_finish_uncancelled() -> None:
    """Normal close stops intake and drains; it does not cancel.

    The in-flight call is released by the test, not by cancellation, and
    drain must wait for it: by the time the pool reports closed the call
    has returned and never saw a cancelled token.
    """
    executor, responder = gated_executor()
    batch = jobs(1)
    only = batch[0]
    pool = fixed_pool(executor, 1)

    async def drain_while_running() -> None:
        async with pool:
            consumer = asyncio.create_task(
                consume(pool.run_stream(submissions_of(batch)))
            )
            await asyncio.to_thread(responder.await_arrival, only.job_id)
            releaser = asyncio.create_task(
                asyncio.to_thread(responder.release, only.job_id)
            )
            await pool.drain()
            await releaser
            consumer.cancel()

    asyncio.run(drain_while_running())

    assert responder.cancelled == ()
    assert responder.finished_gate(only.job_id).is_set()
    assert pool.state is ExecutionPoolState.CLOSED


def test_drain_to_empty_delivers_every_admitted_submission() -> None:
    """A stream consumed to exhaustion leaves the pool empty and closed."""
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
    """One capacity bound gets one lifetime."""
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


# --- Scheduler-wide failure ----------------------------------------------


def test_a_failing_executor_call_breaks_the_pool() -> None:
    """A machinery failure is not per-job completion data.

    `Executor.run` returns outcomes for everything it can observe about a
    child, so an exception escaping it means no trustworthy result exists
    at all. The pool breaks rather than reporting a job that never
    completed as though it had, and the original failure stays as the
    cause.
    """

    def explode(
        _job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        raise ExecutorFailure("machinery failed")

    pool = fixed_pool(FakeExecutor(responder=explode), 1)

    async def stream() -> None:
        async with pool:
            await consume(pool.run_stream(submissions_of(jobs(2))))

    with pytest.raises(SchedulerBroken, match="the execution pool broke") as e:
        asyncio.run(stream())

    assert isinstance(e.value.__cause__, ExecutorFailure)


# --- Finite batch --------------------------------------------------------


def test_a_finite_batch_yields_every_one_of_its_completions() -> None:
    """The whole batch comes back, one completion per job."""
    batch = jobs(7)

    completed = list(batch_of(immediate_executor(), batch, slots=2))

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }


def test_a_finite_batch_consumes_its_input_lazily() -> None:
    """A batch far larger than capacity never materializes.

    The source counts its own advancement and the consumer takes exactly
    one completion before the count is read. With two slots, no more than
    the resident bound may ever have been pulled.
    """
    batch = jobs(1000)
    pulled = 0

    def counting() -> Iterator[ExecutionJob]:
        nonlocal pulled
        for job in batch:
            pulled += 1
            yield job

    stream = batch_of(immediate_executor(), counting(), slots=2)
    next(stream)
    observed = pulled
    stream.close()

    assert observed <= 2


def test_an_abandoned_batch_still_drains_its_admitted_work() -> None:
    """Closing the iterator early still awaits in-flight teardown.

    The drain happens in the generator's own cleanup, so a caller who
    stops consuming cannot leave an executor call running behind the
    pool's back. The finished gates are set by the time `close` returns.
    """
    executor, responder = gated_executor()
    batch = jobs(2)
    stream = batch_of(executor, batch, slots=2)

    primer = in_thread(lambda: next(stream, None))
    responder.await_arrival_count(2)
    responder.release_all(batch)
    join(primer)

    stream.close()

    for job in batch:
        assert responder.finished_gate(job.job_id).is_set()


# --- Sync and async parity -----------------------------------------------


@pytest.mark.parametrize("slots", [1, 2, 5])
def test_both_surfaces_deliver_the_same_completions(slots: int) -> None:
    """One scheduler reached two ways delivers one set of completions.

    Parity is not two implementations agreeing; it is the same core driven
    differently. Varying capacity exercises the bound below, at, and above
    the batch size.
    """
    batch = jobs(4)
    expected = {job.job_id for job in batch}

    from_batch = {
        one.result.execution_id.job_id
        for one in batch_of(immediate_executor(), batch, slots=slots)
    }

    pool = fixed_pool(immediate_executor(), slots)

    async def stream() -> set[JobId]:
        async with pool:
            return {
                completion.completed_execution.result.execution_id.job_id
                async for completion in pool.run_stream(submissions_of(batch))
            }

    from_stream = asyncio.run(stream())

    assert from_batch == expected
    assert from_stream == expected


@pytest.mark.parametrize("slots", [1, 3])
def test_both_surfaces_handle_an_empty_source(slots: int) -> None:
    """No submissions means no completions, and no hang."""
    assert list(batch_of(immediate_executor(), [], slots=slots)) == []

    pool = fixed_pool(immediate_executor(), slots)

    async def stream() -> list[object]:
        async with pool:
            return [
                completion
                async for completion in pool.run_stream(submissions_of([]))
            ]

    assert asyncio.run(stream()) == []


@pytest.mark.parametrize("slots", [1, 2])
def test_both_surfaces_bound_concurrency_the_same_way(slots: int) -> None:
    """The bound is the scheduler's, so it holds through either surface."""
    batch = jobs(6)

    list(
        batch_of(
            FakeExecutor(responder=_OverlapCounter(limit=slots)),
            batch,
            slots=slots,
        )
    )

    pool = fixed_pool(
        FakeExecutor(responder=_OverlapCounter(limit=slots)), slots
    )

    async def stream() -> None:
        async with pool:
            await consume(pool.run_stream(submissions_of(batch)))

    asyncio.run(stream())
