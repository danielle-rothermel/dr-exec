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
from dr_exec._scheduler import _ExecutionScheduler, usable_cpu_count
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
    """

    _executor: Executor
    _config: ExecutionPoolConfig
    _effective_capacity: EffectivePoolCapacity | None
    _state: ExecutionPoolState
    _scheduler: _ExecutionScheduler[object] | None

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
        self._scheduler = None

    @property
    def state(self) -> ExecutionPoolState:
        return self._state

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
        if self._state in {
            ExecutionPoolState.CLOSED,
            ExecutionPoolState.CREATED,
        }:
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

        Per-job failures arrive as completion data and the stream
        continues; only a scheduler-wide failure ends it, by breaking the
        pool.

        The context cast is the one place a type is restored rather than
        checked. One pool may serve streams of different context types, so
        the scheduler stores contexts opaquely; what guarantees the cast is
        the scheduler's pairing invariant -- a completion carries the very
        object its own submission carried, moved through in memory and
        never serialized -- so the object leaving this stream is
        necessarily the one that entered it.
        """
        scheduler = self._running_scheduler()
        source = aiter(submissions)
        exhausted = False
        try:
            while True:
                while not exhausted and scheduler.can_admit():
                    submission = await _next_submission(source)
                    if submission is None:
                        exhausted = True
                        break
                    scheduler.admit(submission.job, submission.context)
                if exhausted and not await asyncio.to_thread(
                    scheduler.has_residents
                ):
                    return
                completion = await asyncio.to_thread(scheduler.take_completion)
                if completion is None:
                    return
                yield ExecutionCompletion(
                    completed_execution=completion.completed_execution,
                    context=cast("ContextT", completion.context),
                )
        except BaseException:
            self._state = ExecutionPoolState.BROKEN
            raise

    async def drain(self) -> None:
        """Stop intake and let admitted work finish, then close.

        Draining delivers nothing: a consumer that wants results consumes
        the stream. What drain guarantees is that every admitted call ran
        to completion, including its teardown, before the pool closed.
        """
        scheduler = self._closing_scheduler()
        if scheduler is None:
            return
        self._state = ExecutionPoolState.DRAINING
        scheduler.close_intake()
        await asyncio.to_thread(scheduler.wait_for_quiescence)
        await asyncio.to_thread(scheduler.shutdown)
        self._state = ExecutionPoolState.CLOSED

    async def abort(self) -> None:
        """Stop intake, cancel every active call, and await their teardown.

        Abort is not abandonment. Each cancelled call still performs its
        own group-targeted teardown, reaps its child, and finalizes its
        record; the pool waits for exactly that before closing, because a
        pool that returned first would leave the containment claim
        unenforced at the one moment it matters most.
        """
        scheduler = self._closing_scheduler()
        if scheduler is None:
            return
        self._state = ExecutionPoolState.DRAINING
        scheduler.close_intake()
        scheduler.cancel_all()
        await asyncio.to_thread(scheduler.wait_for_quiescence)
        await asyncio.to_thread(scheduler.shutdown)
        self._state = ExecutionPoolState.CLOSED

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
        nothing to wait for.
        """
        if self._state is ExecutionPoolState.CLOSED:
            return None
        if self._scheduler is None:
            self._state = ExecutionPoolState.CLOSED
            return None
        return self._scheduler


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
