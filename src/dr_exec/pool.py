"""The host-local bounded execution pool.

`ExecutionPool` is the one concurrency owner in dr-exec. It is concrete
policy rather than a capability Protocol: there is exactly one scheduling
model, and a consumer that needs a different one owns its own backlog
above this boundary instead of substituting a different scheduler beneath
it.

The pool holds no scheduling logic of its own. It owns an async lifecycle
-- open, run streams, drain or abort, close -- and delegates every
admission, capacity, ordering, and cancellation decision to the one
scheduler core, which `ProcessExecutor.run_many` drives synchronously from
the same code. That is deliberate: sync and async surfaces sharing
semantics is a property of there being one implementation, not of two
implementations agreeing.

Capacity is one shared resident bound. Running submissions and
completed-but-undelivered submissions occupy it together, so a consumer
that stops consuming stops the source: intake pulls the next submission
only while the bound has room, and a completed result keeps its slot until
delivery. There is no separate completion buffer to size and no prefetch.

Every blocking scheduler wait is offloaded to a worker thread through
`asyncio.to_thread`, so the event loop stays free while the pool waits on
a condition rather than spinning or polling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from types import TracebackType
from typing import Generic, TypeVar, cast

from pydantic import PositiveInt

from dr_exec._model import ContractModel
from dr_exec._scheduler import (
    SchedulerBroken,
    _AdmissionResult,
    _ExecutionScheduler,
    usable_cpu_count,
)
from dr_exec.declare import ExecutionJob
from dr_exec.errors import ExecutorFailure
from dr_exec.kinds import (
    CapacitySource,
    ExecutionPoolState,
)
from dr_exec.protocols import Executor
from dr_exec.record import CompletedExecution


@dataclass(frozen=True, slots=True)
class AutoPoolCapacity:
    """Resolve capacity once, at pool open, from the usable CPU count."""


@dataclass(frozen=True, slots=True)
class FixedPoolCapacity:
    max_active_jobs: int

    def __post_init__(self) -> None:
        if self.max_active_jobs < 1:
            raise ValueError("max_active_jobs must be positive")


type PoolCapacity = AutoPoolCapacity | FixedPoolCapacity


@dataclass(frozen=True, slots=True)
class ExecutionPoolConfig:
    capacity: PoolCapacity = field(default_factory=AutoPoolCapacity)


class EffectivePoolCapacity(ContractModel):
    """The capacity a pool actually resolved, recorded for the caller.

    Both the source and the observed CPU count are carried, not only the
    resulting slot count: a fixed pool on a four-CPU machine and an
    automatic pool that resolved to the same number are different
    decisions, and reading back only `max_active_jobs` would erase which
    one a run used.
    """

    source: CapacitySource
    cpu_count: PositiveInt
    max_active_jobs: PositiveInt


ContextT = TypeVar("ContextT")


@dataclass(frozen=True, slots=True)
class ExecutionSubmission(Generic[ContextT]):  # noqa: UP046
    job: ExecutionJob
    context: ContextT


@dataclass(frozen=True, slots=True)
class ExecutionCompletion(Generic[ContextT]):  # noqa: UP046
    completed_execution: CompletedExecution
    context: ContextT


def _resolve_capacity(capacity: PoolCapacity, /) -> EffectivePoolCapacity:
    """Resolve one capacity declaration into the pool's effective bound.

    Automatic capacity resolves exactly once, here, at pool open: a pool
    whose bound drifted with machine load would make its own resident
    guarantee unstatable. The usable CPU count is recorded either way, so
    a fixed pool's record still says what machine it bounded.
    """
    cpus = usable_cpu_count()
    match capacity:
        case AutoPoolCapacity():
            return EffectivePoolCapacity(
                source=CapacitySource.AUTO,
                cpu_count=cpus,
                max_active_jobs=cpus,
            )
        case FixedPoolCapacity():
            return EffectivePoolCapacity(
                source=CapacitySource.FIXED,
                cpu_count=cpus,
                max_active_jobs=capacity.max_active_jobs,
            )


class ExecutionPool:
    """One bounded scheduler with an owning async lifecycle.

    The pool is entered as an async context manager, streams submissions
    through `run_stream`, and closes by draining admitted work or by
    aborting it. A closed pool cannot reopen: reopening would give one
    capacity bound two disjoint lifetimes, and the durable records of the
    second would be indistinguishable from the first's.

    Several source loops may feed one open pool, which is the host shape:
    many sources, one host bound. The scheduler's ready queue is a single
    completion-ordered queue with no per-stream identity, so completions
    are shared out among live streams in completion order rather than
    partitioned by which stream admitted them. Each completion still
    carries exactly its own submission's context; what concurrent streams
    give up is which stream receives it, not whether the pairing holds.

    That is why the caller context of one pool is one type. Every stream
    delivers whatever finished first, so a pool fed two context types
    would hand each stream the other's objects -- correctly paired, and
    still not what that stream declared. A consumer needing two context
    types wraps them in one union or tags them itself.

    A second pool is not the remedy for that -- it is a separate
    host-capacity decision. Capacity here is host-level, so two pools
    must be given explicit non-overlapping fixed capacity; two automatic
    pools would each resolve to the full usable CPU count and together
    claim the host twice.

    Those several sources are several loops *on one event loop*: the pool
    is owned by the loop that entered it, and lifecycle operations from
    any other loop are rejected. The scheduler core is properly locked,
    but the pool's own lifecycle attributes are plain attributes, correct
    only because one loop touches them. Rejecting is what makes that
    true rather than assumed -- a `drain` racing a live `run_stream` from
    another loop would tear down state that stream is mid-read of.
    """

    _executor: Executor
    _config: ExecutionPoolConfig
    _effective_capacity: EffectivePoolCapacity | None
    _state: ExecutionPoolState
    _closed: bool
    _scheduler: _ExecutionScheduler[object] | None
    _owning_loop: asyncio.AbstractEventLoop | None

    def __init__(
        self,
        *,
        executor: Executor,
        config: ExecutionPoolConfig,
    ) -> None:
        self._executor = executor
        self._config = config
        self._effective_capacity = None
        self._state = ExecutionPoolState.CREATED
        self._closed = False
        self._scheduler = None
        self._owning_loop = None

    @property
    def effective_capacity(self) -> EffectivePoolCapacity:
        """The capacity this pool resolved when it opened.

        Only meaningful once open: automatic capacity resolves at open,
        so answering before that would either invent a number or resolve
        one the pool will not use.
        """
        if self._effective_capacity is None:
            raise ExecutorFailure(
                "effective capacity is resolved when the pool is entered"
            )
        return self._effective_capacity

    async def __aenter__(self) -> ExecutionPool:  # noqa: PYI034
        if self._state is not ExecutionPoolState.CREATED:
            raise ExecutorFailure(
                f"an execution pool in state {self._state} cannot be opened"
            )
        capacity = _resolve_capacity(self._config.capacity)
        self._effective_capacity = capacity
        self._scheduler = _ExecutionScheduler(
            executor=self._executor,
            capacity=capacity.max_active_jobs,
        )
        self._owning_loop = asyncio.get_running_loop()
        self._state = ExecutionPoolState.RUNNING
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the pool: drain normally, abort when leaving on an error.

        An exception propagating out of the body means the consumer is no
        longer waiting for results, so draining admitted work would block
        the raise behind runs nobody will read. Aborting instead cancels
        those calls and still awaits their teardown, which is the part
        that is never optional.
        """
        self._require_owning_loop()
        if self._closed:
            return
        if self._state is ExecutionPoolState.CREATED:
            self._closed = True
            self._state = ExecutionPoolState.CLOSED
            return
        if exc is None:
            await self.drain()
        else:
            await self.abort()

    async def run_stream(
        self,
        submissions: AsyncIterable[ExecutionSubmission[ContextT]],
        /,
    ) -> AsyncIterator[ExecutionCompletion[ContextT]]:
        """Stream completions in completion order under one resident bound.

        The loop is the whole scheduling policy made visible: pull from
        the source only while the shared bound has room, then deliver one
        completion, which is what frees the slot the next pull needs. A
        consumer that stops consuming stops pulling -- backpressure is
        structural here rather than a buffer size chosen somewhere else.

        The room this loop checks is shared, so it may be gone by the
        time the awaited pull returns: several streams on one pool can
        each see the last slot and race for it, and only one wins. The
        loser holds its already-pulled submission in a carry slot and
        retries it after the next delivery rather than dropping it. That
        is what makes the shared bound safe to check optimistically --
        the check is a hint, `admit` is the enforcement, and a lost race
        costs a retry rather than a lost run.

        Per-job failures arrive as completion data and the stream
        continues; only a scheduler-wide failure ends it, by breaking the
        pool. A break ends the stream with the tail before the raise:
        completions already buffered when it landed are yielded first, and
        the failure is raised once there is nothing left to hand over. So
        a consumer sees every result the pool actually produced and then
        learns the pool stopped being able to produce more. What a break
        loses is the undelivered work admitted before it -- queued
        submissions dropped unstarted, and calls still in flight whose
        results arrive after the buffer emptied.

        The context cast is the one place a type is restored rather than
        checked. The scheduler stores contexts opaquely -- `ContextT` is
        this method's parameter, not the pool's, so one pool's scheduler
        is typed at `object` -- and its pairing invariant is what the cast
        rests on: a completion carries the very object its own submission
        carried, moved through in memory and never serialized. Nothing
        else can appear in a completion's context slot.

        The invariant pairs a completion with its submission; it does not
        say which of a pool's live streams receives it. Sharing one ready
        queue is what makes several source loops one host bound, and the
        price is that a pool's caller context is one type -- feed a pool
        two, and a stream is handed the other's objects: correctly paired,
        wrongly typed, and the cast would not catch it. That is the
        caller's shape to choose, not a refusal this pool enforces.
        """
        self._require_owning_loop()
        scheduler = self._running_scheduler()
        source = aiter(submissions)
        exhausted = False
        carried: ExecutionSubmission[ContextT] | None = None
        try:
            while True:
                while not exhausted and scheduler.can_admit():
                    submission = (
                        carried
                        if carried is not None
                        else await _next_submission(source)
                    )
                    carried = None
                    if submission is None:
                        exhausted = True
                        break
                    match scheduler.admit(submission.job, submission.context):
                        case _AdmissionResult.ADMITTED:
                            pass
                        case _AdmissionResult.INTAKE_CLOSED:
                            # A concurrent `drain` or `abort` closed
                            # intake while this pull was awaiting. That
                            # is a requested close, not a failure, so
                            # intake ends here exactly as an exhausted
                            # source would end it and the stream still
                            # delivers what was already admitted.
                            exhausted = True
                            break
                        case _AdmissionResult.NO_ROOM:
                            # Another stream on this pool filled the
                            # shared bound while this pull was awaiting.
                            # The submission is good and this stream
                            # still owns it, so it is held rather than
                            # dropped and retried after the delivery
                            # below frees a slot -- the bound is the
                            # pool's, so any stream's delivery makes the
                            # room this one is waiting for.
                            carried = submission
                            break
                if (
                    exhausted
                    and carried is None
                    and not await asyncio.to_thread(scheduler.has_residents)
                ):
                    return
                completion = await asyncio.to_thread(scheduler.take_completion)
                if completion is None:
                    # Nothing ready, nothing running, nothing admitted.
                    # A carry held here is not stranded, it is the
                    # opposite: an empty scheduler is an empty bound, so
                    # the retry above is now certain to fit. Returning
                    # here instead would drop a pulled submission that
                    # was never delivered, which is the one thing the
                    # carry exists to prevent.
                    if carried is None:
                        return
                    continue
                yield ExecutionCompletion(
                    completed_execution=completion.completed_execution,
                    context=cast("ContextT", completion.context),
                )
        except SchedulerBroken:
            # Only a scheduler-wide failure breaks the pool. A consumer
            # that stops iterating, a cancelled task, or a source that
            # raises all end this stream without saying anything about
            # whether the scheduler can still produce trustworthy
            # completions -- and a pool already closed stays closed.
            if self._state is ExecutionPoolState.RUNNING:
                self._state = ExecutionPoolState.BROKEN
            raise

    async def drain(self) -> None:
        """Stop intake and let admitted work finish, then close.

        Draining delivers nothing: a consumer that wants results consumes
        the stream. What drain guarantees is that every admitted call ran
        to completion, including its teardown, before the pool closed.
        """
        self._require_owning_loop()
        scheduler = self._closing_scheduler()
        if scheduler is None:
            return
        broke = self._state is ExecutionPoolState.BROKEN
        self._state = ExecutionPoolState.DRAINING
        scheduler.close_intake()
        await asyncio.to_thread(scheduler.wait_for_quiescence)
        await asyncio.to_thread(scheduler.shutdown)
        self._closed = True
        self._state = _closed_state(broke or scheduler.is_broken())

    async def abort(self) -> None:
        """Stop intake, cancel every active call, and await their teardown.

        Abort is not abandonment. Each cancelled call still performs its
        own group-targeted teardown, reaps its child, and finalizes its
        record; the pool waits for exactly that before closing, because a
        pool that returned first would leave the containment claim
        unenforced at the one moment it matters most.
        """
        self._require_owning_loop()
        scheduler = self._closing_scheduler()
        if scheduler is None:
            return
        broke = self._state is ExecutionPoolState.BROKEN
        self._state = ExecutionPoolState.DRAINING
        scheduler.close_intake()
        scheduler.cancel_all()
        await asyncio.to_thread(scheduler.wait_for_quiescence)
        await asyncio.to_thread(scheduler.shutdown)
        self._closed = True
        self._state = _closed_state(broke or scheduler.is_broken())

    def _require_owning_loop(self) -> None:
        """Reject a lifecycle operation from outside the owning loop.

        The loop that entered the pool owns it. A pool that never opened
        has no owner yet, so there is nothing to violate and nothing to
        check -- the state machine answers those calls.

        This guards the pool's own attributes rather than the scheduler's
        state: `_state`, `_closed`, and `_effective_capacity` are read and
        written without a lock, which is sound exactly while one loop
        does it. It is a provenance check, not a state check, so it is the
        one thing the `ExecutionPoolState` guards cannot express.
        """
        if self._owning_loop is None:
            return
        if asyncio.get_running_loop() is not self._owning_loop:
            raise ExecutorFailure(
                "an execution pool is driven only by the event loop that "
                "opened it"
            )

    def _running_scheduler(self) -> _ExecutionScheduler[object]:
        if self._state is not ExecutionPoolState.RUNNING:
            raise ExecutorFailure(
                f"an execution pool in state {self._state} cannot stream"
            )
        if self._scheduler is None:  # pragma: no cover - open sets both
            raise ExecutorFailure("the execution pool has no scheduler")
        return self._scheduler

    def _closing_scheduler(self) -> _ExecutionScheduler[object] | None:
        """The scheduler to close, or None when there is nothing to close.

        Closing twice is not an error -- `__aexit__` after an explicit
        `drain()` is ordinary -- but closing a pool that never opened has
        nothing to wait for. Whether the pool is already closed is the
        `_closed` fact rather than the state, because a pool that broke
        closes like any other but keeps the break as the state a consumer
        reads afterwards.
        """
        if self._closed:
            return None
        if self._scheduler is None:
            self._closed = True
            self._state = ExecutionPoolState.CLOSED
            return None
        return self._scheduler


def _closed_state(broke: bool, /) -> ExecutionPoolState:
    """The terminal state a close lands in, preserving any break.

    A broken pool is closed too -- its scheduler was shut down and its
    workers joined -- but "closed" and "broke" are different answers to
    the one question a consumer asks afterwards: whether the results it
    received are all the results there were. Forcing CLOSED over a break
    would erase that distinction at exactly the moment it matters, so the
    break is the state that survives.

    That covers a break that lands *during* the close as well as one
    observed before it. Closing waits for calls that are still running,
    and any of them may be the one that breaks the scheduler, so the
    caller asks the scheduler after quiescence rather than trusting a
    snapshot taken before the wait began.
    """
    return ExecutionPoolState.BROKEN if broke else ExecutionPoolState.CLOSED


async def _next_submission[T](
    source: AsyncIterator[ExecutionSubmission[T]],
    /,
) -> ExecutionSubmission[T] | None:
    """Pull exactly one submission, or None when the source is exhausted."""
    try:
        return await anext(source)
    except StopAsyncIteration:
        return None


__all__ = [
    "AutoPoolCapacity",
    "EffectivePoolCapacity",
    "ExecutionCompletion",
    "ExecutionPool",
    "ExecutionPoolConfig",
    "ExecutionSubmission",
    "FixedPoolCapacity",
    "PoolCapacity",
]
