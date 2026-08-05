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
    FixedPoolCapacity,
    JobId,
)
from dr_exec._scheduler import (
    SchedulerBroken,
    _AdmissionResult,
    _Admitted,
    _ExecutionScheduler,
    usable_cpu_count,
)
from dr_exec.executor import _run_batch
from dr_exec.kinds import CapacitySource

if TYPE_CHECKING:
    from dr_exec.protocols import Executor


# The ticket the break case loads its queue under. Any value distinct
# from the scheduler's own counter works; naming it keeps the constructed
# state readable where it is asserted on.
_QUEUED_TICKET = 9_000


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


def test_state_reports_each_lifecycle_stage_through_the_public_surface() -> None:
    """`state` snapshots the lifecycle: created, running, closed.

    The property is observational: each read here happens after the
    transition's own call has returned, never as a wait target. The
    broken stage is pinned where breaks are constructed, alongside the
    drain-before-raise cases.
    """
    pool = fixed_pool(immediate_executor(), 1)
    assert pool.state is ExecutionPoolState.CREATED

    async def open_observe_close() -> ExecutionPoolState:
        async with pool:
            return pool.state

    assert asyncio.run(open_observe_close()) is ExecutionPoolState.RUNNING
    assert pool.state is ExecutionPoolState.CLOSED


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

    Two slots, three jobs, every call gated. The case drives the scheduler
    directly because delivery is exactly what must not happen: no
    completion is taken while the assertion is made, so the first call's
    finished result provably still occupies its slot rather than merely
    not having been delivered yet.

    That construction is what makes the evidence a held gate instead of a
    won race. `can_admit` is the one predicate every surface's intake is
    gated on, and it is read at a point where a separate active bound
    would already have freed the first slot -- the first call is released
    and *observed to have returned*. Only afterwards is the completion
    taken, and only then does the third job become admissible, which pins
    delivery rather than completion as the moment the slot frees.
    """
    executor, responder = gated_executor()
    batch = jobs(3)
    first, second, third = batch
    scheduler: _ExecutionScheduler[None] = _ExecutionScheduler(
        executor=executor, capacity=2
    )
    try:
        assert scheduler.admit(first, None) is _AdmissionResult.ADMITTED
        assert scheduler.admit(second, None) is _AdmissionResult.ADMITTED
        responder.await_arrival_count(2)
        assert set(responder.started) == {first.job_id, second.job_id}
        assert not scheduler.can_admit()

        responder.release(first.job_id)
        responder.await_finish(first.job_id)
        assert not scheduler.can_admit()
        assert third.job_id not in responder.started

        completion = scheduler.take_completion()
        assert completion is not None
        assert completion.completed_execution.result.execution_id.job_id == (
            first.job_id
        )
        assert scheduler.can_admit()

        assert scheduler.admit(third, None) is _AdmissionResult.ADMITTED
        responder.await_arrival(third.job_id)
    finally:
        responder.release_all(batch)
        scheduler.close_intake()
        scheduler.wait_for_quiescence()
        scheduler.shutdown()


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


def test_a_pool_rejects_lifecycle_calls_from_another_loop() -> None:
    """The loop that opened the pool is the only loop that may drive it.

    The scheduler core is condition-guarded, but the pool's own lifecycle
    attributes are not: a `drain` on a second loop would race the state a
    live stream is reading and could tear down completions it was about
    to deliver. Provenance is therefore checked, not assumed.
    """
    pool = fixed_pool(immediate_executor(), 1)
    opened = threading.Event()
    intruded = threading.Event()
    rejections: list[str] = []

    def intrude() -> None:
        """Drive each lifecycle entry point from a distinct second loop."""
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

    with pytest.raises(ExecutorFailure, match="the execution pool broke") as e:
        asyncio.run(stream())

    assert isinstance(e.value.__cause__, ExecutorFailure)
    assert pool.state is ExecutionPoolState.BROKEN


def test_a_break_survives_the_close_that_follows_it() -> None:
    """A closed pool and a broken one are different answers, so it survives.

    Leaving the context manager on the raise aborts the pool, which is a
    real close: the scheduler shuts down and its workers are joined. What
    the pool must not do is overwrite the break with an ordinary CLOSED,
    because then a consumer could no longer tell a pool that delivered
    everything from one that stopped being able to deliver at all.
    """

    def explode(
        _job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        raise ExecutorFailure("machinery failed")

    pool = fixed_pool(FakeExecutor(responder=explode), 1)

    async def stream() -> None:
        async with pool:
            await consume(pool.run_stream(submissions_of(jobs(1))))

    with pytest.raises(ExecutorFailure, match="the execution pool broke"):
        asyncio.run(stream())

    assert pool.state is ExecutionPoolState.BROKEN


@pytest.mark.parametrize("close", ["drain", "abort"])
def test_a_break_landing_during_a_close_is_the_state_the_close_lands_in(
    close: str,
) -> None:
    """A close waits for running calls, so a break can land inside it.

    Closing is not instantaneous: it stops intake and then *waits* for
    every call still running to finish its teardown. Any of those calls
    may be the one whose machinery fails, so the break can land after
    the close began and before it returned. A close that decided its
    terminal state from a snapshot taken before that wait would report
    an ordinary CLOSED for a pool whose scheduler is broken -- exactly
    the distinction a consumer reads the state to make.

    The window is held open rather than raced for: the failing call is
    parked at a gate, the close is started and waited on until it has
    provably closed intake, and only then is the call released to raise.
    Driving the scheduler directly is deliberate -- with no live stream
    there is nothing that could observe the break first and set BROKEN
    by the other path, so the state under test is the close's own.

    Both close paths are covered, because both take the same snapshot.
    """
    arrived = threading.Event()
    release = threading.Event()

    def explode_when_released(
        _job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        arrived.set()
        wait_for(release, what="the failing call to be released")
        raise ExecutorFailure("machinery failed")

    pool = fixed_pool(FakeExecutor(responder=explode_when_released), 1)

    async def close_over_a_breaking_call() -> None:
        async with pool:
            scheduler = pool._scheduler
            assert scheduler is not None
            assert (
                scheduler.admit(jobs(1)[0], None) is _AdmissionResult.ADMITTED
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


def _await_closed_intake(scheduler: _ExecutionScheduler[object], /) -> None:
    """Block until the scheduler's intake is closed, under the watchdog.

    This is a state gate, not a delay: it waits on the scheduler's own
    condition for the flag a close sets, so a case can act at a point the
    close has provably reached rather than one it probably has.
    """
    with scheduler._condition:
        if not scheduler._condition.wait_for(
            lambda: scheduler._intake_closed, WATCHDOG_SECONDS
        ):
            raise AssertionError("watchdog fired waiting for closed intake")


def test_a_job_queued_behind_a_failing_call_is_never_started() -> None:
    """A broken scheduler stops dispatching, it does not drain the queue.

    What a break bounds is dispatch, not flight. A call another worker
    already entered runs to its own end -- it is cancelled, but its
    teardown completes and its result is discarded. So the behavior under
    test is only observable on a submission that is *genuinely queued*
    when the break lands.

    That state is not reachable by admitting, and the case is explicit
    about constructing it directly rather than pretending otherwise.
    Admission enforces the resident bound, and the worker cap is the same
    number, so every submission the bound accepts has a worker free to
    take it: a queue that outlives its dispatch exists only in the window
    between a worker returning from one call and re-entering the wait for
    the next. The queue is therefore loaded through the scheduler's own
    state, with the only worker provably inside the failing call.

    What is pinned is `_finish`'s guarantee about that queue: a break
    drops it rather than starting it. The queued submission's completion
    could never reach a consumer, because delivery raises before it
    inspects the ready queue, so starting it would spawn a child and
    write a record for a result guaranteed to be discarded. Its token is
    cancelled and dropped rather than left resident for the pool's life.

    The evidence is a state, not a delay: the scheduler is driven to its
    terminal break and fully shut down, which joins every worker, before
    the arrival record is read. Any dispatch that was going to happen has
    necessarily already happened by then.
    """
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
        raise ExecutorFailure("machinery failed")

    scheduler: _ExecutionScheduler[None] = _ExecutionScheduler(
        executor=FakeExecutor(responder=explode_on_the_failing_call),
        capacity=1,
    )
    queued_token = CancelToken()
    try:
        assert scheduler.admit(failing, None) is _AdmissionResult.ADMITTED
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
    """Complete every job but one, and fail that one on release.

    This is the substrate for the drain-before-raise cases. Two jobs are
    ordinary gated calls whose completions the scheduler buffers; the
    third raises, which breaks the scheduler with results already sitting
    in the ready queue -- the exact state the behavior is about.
    """

    def __init__(self, failing: JobId, /) -> None:
        self._failing = failing
        self._arrived: dict[JobId, threading.Event] = {}
        self._release: dict[JobId, threading.Event] = {}
        self._finished: dict[JobId, threading.Event] = {}
        self._lock = threading.Lock()

    def __call__(
        self, job: ExecutionJob, _token: CancelToken | None, /
    ) -> CompletedExecution:
        self.arrived(job.job_id).set()
        wait_for(self.gate(job.job_id), what=f"job {job.job_id} to release")
        try:
            if job.job_id == self._failing:
                raise ExecutorFailure("machinery failed")
            return completion_for(job.job_id)
        finally:
            self.finished(job.job_id).set()

    def arrived(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._arrived.setdefault(job_id, threading.Event())

    def gate(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._release.setdefault(job_id, threading.Event())

    def finished(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._finished.setdefault(job_id, threading.Event())

    def await_arrivals(self, *job_ids: JobId) -> None:
        for job_id in job_ids:
            wait_for(self.arrived(job_id), what=f"job {job_id} to start")

    def buffer(self, *job_ids: JobId) -> None:
        """Let the named calls return, and wait until they have.

        Waiting for the *finished* gate is what makes these completions
        buffered rather than merely released: each call has returned, so
        any result it will ever produce is already on its way to the ready
        queue, and the consumer is parked in its source pull, so none of
        them has been delivered.
        """
        for job_id in job_ids:
            self.gate(job_id).set()
        for job_id in job_ids:
            wait_for(self.finished(job_id), what=f"job {job_id} to finish")

    def break_the_pool(self) -> None:
        """Release the failing call and wait until it has raised."""
        self.await_arrivals(self._failing)
        self.gate(self._failing).set()
        wait_for(self.finished(self._failing), what="the failing call to end")


def test_a_break_delivers_the_buffered_tail_before_it_raises() -> None:
    """A break ends delivery after the buffer, not instead of it.

    A completion sitting in the ready queue is a call that genuinely ran
    and recorded its own result. A machinery failure on some *other* job
    says nothing about it, so it is delivered rather than discarded, and
    the break is raised only once nothing is left to hand over.

    Capacity is above one because that is the only place the behavior
    exists: with one slot there is never a buffered completion for a
    break to reach past. Four slots take all three jobs at once, two of
    which finish and buffer while the third fails, and the two must still
    arrive -- each carrying exactly its own submission's context -- before
    the failure surfaces.

    The ordering is constructed, not raced. The source parks after its
    last job, so the consumer is held inside a pull rather than inside a
    delivery for the whole setup: the two ordinary calls are released and
    *observed to have returned*, then the failing one is released and
    observed to have raised. At the moment the break lands, both
    completions are provably buffered and provably undelivered, which is
    the state the behavior is about. Only then is the pull let go, and
    the source ends there, so nothing admitted after the break can blur
    what the drain hands over.
    """
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
            await asyncio.to_thread(
                responder.buffer, first.job_id, second.job_id
            )
            await asyncio.to_thread(responder.break_the_pool)
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


def test_a_batch_break_delivers_the_buffered_tail_before_it_raises() -> None:
    """The same drain-before-raise through the synchronous surface.

    One scheduler core reached two ways must end a break the same way, so
    the batch driver is held to the identical rule: every buffered
    completion first, the failure last. The construction is the batch
    equivalent of the stream case, gate for gate -- the source parks after
    its last job, so the driver is held inside its pull while the two
    completions buffer and the break lands, and nothing has been
    delivered at that point.
    """
    first, second, failing = jobs(3)
    responder = _BreakAfterBuffering(failing.job_id)
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
    responder.buffer(first.job_id, second.job_id)
    responder.break_the_pool()
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
    """A resource failure the caller could handle must not become a wait.

    Thread creation fails on a loaded host, and the submission that
    triggered it is already queued for the worker that does not exist.
    Nothing can ever run it, so every wait for quiescence -- which is
    what both surfaces close through -- would block forever on work with
    no runner. An unbounded hang is the one outcome a caller cannot
    handle; a break is one they can, so the failure surfaces as
    `SchedulerBroken` carrying the resource error as its cause.

    Both surfaces are covered because both close through that same wait,
    and the assertion is that each *finishes*: the watchdog on the
    driving thread is what distinguishes a reported break from the hang
    this pins, since a wedged close fails the case rather than passing
    it slowly.
    """

    class _UnstartableThread(threading.Thread):
        """A worker thread the host refuses to start."""

        def start(self) -> None:
            raise RuntimeError("can't start new thread")

    # Patched at the scheduler's own module reference rather than on
    # `threading.Thread`: `asyncio.to_thread` builds threads too, and
    # breaking those would wedge the pool for an unrelated reason and
    # invalidate the case.
    monkeypatch.setattr("dr_exec._scheduler.Thread", _UnstartableThread)

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


# --- Requested close during intake ---------------------------------------


def test_a_drain_landing_mid_pull_ends_the_stream_without_a_failure() -> None:
    """A requested close is not a scheduler-wide failure.

    Every realistic source awaits between items -- a lease from a
    workflow queue is a network round trip -- so a `drain` can always
    land between the admission check and the admission itself. The
    consumer must see that as the end of the stream, because nothing
    about the scheduler's ability to produce trustworthy completions has
    changed; it was simply asked to stop taking new work.

    The window is not hoped for, it is held open: the source parks on an
    event that the test sets only after the drain has been requested, so
    the pull is provably in flight across the close.

    What is pinned is the shape of the ending, not a delivery count. The
    consumer reaches the end of its stream and the pool reports a clean
    close; which completions a concurrent drain still hands over is not
    promised, because closing stops delivery -- the same limit
    `test_abort_does_not_promise_delivery_of_what_it_tore_down` states.
    What must never arrive is the submission from beyond the close.
    """
    parked = asyncio.Event()
    may_proceed = asyncio.Event()

    async def parking_source() -> AsyncIterator[ExecutionSubmission[int]]:
        yield ExecutionSubmission(job=jobs(1)[0], context=0)
        parked.set()
        await may_proceed.wait()
        yield ExecutionSubmission(job=jobs(1)[0], context=1)

    pool = fixed_pool(immediate_executor(), 2)
    delivered: list[int] = []

    async def drain_mid_pull() -> None:
        async with pool:
            stream = pool.run_stream(parking_source())
            consumer = asyncio.create_task(
                _collect_contexts(stream, delivered)
            )
            await asyncio.wait_for(parked.wait(), WATCHDOG_SECONDS)
            drainer = asyncio.create_task(pool.drain())
            may_proceed.set()
            await asyncio.wait_for(consumer, WATCHDOG_SECONDS)
            await asyncio.wait_for(drainer, WATCHDOG_SECONDS)

    asyncio.run(drain_mid_pull())

    assert 1 not in delivered
    assert pool.state is ExecutionPoolState.CLOSED


def test_an_abort_landing_mid_pull_ends_the_stream_without_a_failure() -> None:
    """Abort is the same requested close, one step stronger.

    It cancels what is in flight rather than letting it finish, but it is
    still a lifecycle operation the caller asked for, so the consumer
    reaches the end of its stream instead of catching a failure that
    would say the pool could not be trusted.
    """
    parked = asyncio.Event()
    may_proceed = asyncio.Event()

    async def parking_source() -> AsyncIterator[ExecutionSubmission[int]]:
        yield ExecutionSubmission(job=jobs(1)[0], context=0)
        parked.set()
        await may_proceed.wait()
        yield ExecutionSubmission(job=jobs(1)[0], context=1)

    pool = fixed_pool(immediate_executor(), 2)
    delivered: list[int] = []

    async def abort_mid_pull() -> None:
        async with pool:
            stream = pool.run_stream(parking_source())
            consumer = asyncio.create_task(
                _collect_contexts(stream, delivered)
            )
            await asyncio.wait_for(parked.wait(), WATCHDOG_SECONDS)
            aborter = asyncio.create_task(pool.abort())
            may_proceed.set()
            await asyncio.wait_for(consumer, WATCHDOG_SECONDS)
            await asyncio.wait_for(aborter, WATCHDOG_SECONDS)

    asyncio.run(abort_mid_pull())

    assert 1 not in delivered
    assert pool.state is ExecutionPoolState.CLOSED


async def _collect_contexts(
    stream: AsyncIterator[ExecutionCompletion[int]],
    into: list[int],
    /,
) -> None:
    """Consume a stream to its end, recording each context delivered."""
    async for completion in stream:
        into.append(completion.context)


# --- Several sources, one pool -------------------------------------------


def test_several_source_loops_feed_one_pool_under_its_one_bound() -> None:
    """The host shape: many sources, one bound, one scheduler.

    Two streams run concurrently on one open pool. Both are served, and
    the bound that admits is the pool's own rather than one per stream:
    capacity two against two sources of two means exactly two calls are in
    flight, and the rest start only as delivery frees resident slots.
    """
    executor, responder = gated_executor()
    both = (jobs(2), jobs(2))
    pool = fixed_pool(executor, 2)
    delivered: list[int] = []

    async def two_streams() -> None:
        async with pool:
            streams = [
                asyncio.create_task(
                    _collect_contexts(
                        pool.run_stream(submissions_of(batch)), delivered
                    )
                )
                for batch in both
            ]
            # Two arrivals is the whole bound; a third would mean a
            # per-stream bound, so the count is the assertion.
            await asyncio.to_thread(responder.await_arrival_count, 2)
            assert len(responder.started) == 2
            for batch in both:
                await asyncio.to_thread(responder.release_all, batch)
            await asyncio.wait_for(asyncio.gather(*streams), WATCHDOG_SECONDS)

    asyncio.run(two_streams())

    assert sorted(delivered) == [0, 0, 1, 1]


def test_concurrent_feeders_racing_the_last_slot_never_exceed_the_bound() -> (
    None
):
    """The admission check is a hint; the bound holds under a real race.

    Every stream checks for room and then *awaits* its source before
    admitting, and that await releases the scheduler's lock. So several
    streams on one pool can each observe the same last free slot and all
    arrive at admission believing it is theirs. The check cannot be what
    enforces the bound, because it is stale by the time it is acted on.

    The race is constructed rather than hoped for: every feeder parks
    inside its pull on a gate the test opens only once *all* of them are
    parked, so each one provably observed room before any of them
    admitted. Releasing them together is the exact interleaving that
    over-admits if admission trusts the check.

    Two things are asserted, because a bound held by dropping work is not
    held. Occupancy never exceeds capacity, and every submission is still
    delivered exactly once: the stream that loses the race carries its
    already-pulled submission and retries it rather than discarding it.

    The occupancy evidence is recorded at the moment of each admission,
    not sampled from outside. A poll between admissions can miss the
    breach entirely -- it is repaired by the very next delivery -- so the
    peak is taken under the scheduler's own lock on the admitting path,
    where an over-admission is unmissable by construction.

    Residents are the right observable here, not arrivals. Worker threads
    are capped separately, so the in-flight count stays correct even when
    the shared bound is breached -- an arrival-count assertion would pass
    against the very defect this pins.
    """
    feeders = 4
    capacity = 1
    all_parked = asyncio.Event()
    may_admit = asyncio.Event()
    parked = 0

    class _PeakRecordingScheduler(_ExecutionScheduler[object]):
        """Records the bound's occupancy as each admission takes it."""

        peak_residents = 0

        def admit(
            self, job: ExecutionJob, context: object, /
        ) -> _AdmissionResult:
            result = super().admit(job, context)
            with self._condition:
                type(self).peak_residents = max(
                    type(self).peak_residents, self._residents
                )
            return result

    async def racing_source(
        which: int, /
    ) -> AsyncIterator[ExecutionSubmission[int]]:
        """Park inside the pull until every feeder is parked too."""
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
                executor=immediate_executor(), capacity=capacity
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


# --- Bounded retention ---------------------------------------------------


def test_cancellation_tokens_are_bounded_by_capacity_not_by_history() -> None:
    """The long-lived pool shape must not accumulate one token per job.

    A durable streaming worker is one pool over a hundred thousand
    samples, so any structure that grows per admitted job rather than per
    resident is a leak -- and a `CancelToken` holds an OS-level lock, not
    just a Python object. Every other scheduler structure is already
    sized by the bound; this pins that the token map is too.

    The check runs against delivered work, not a clock: after each
    delivery the scheduler has provably retired that submission, so the
    live token count may never exceed the capacity that bounds it.
    """
    capacity = 2
    delivered = 0
    peak_tokens = 0
    batch = jobs(40)
    scheduler: _ExecutionScheduler[None] = _ExecutionScheduler(
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
