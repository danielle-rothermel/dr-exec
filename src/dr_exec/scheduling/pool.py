from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Generic, TypeVar, cast

from pydantic import PositiveInt

from dr_exec.capabilities.protocols import Executor
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import (
    CapacitySource,
    ExecutionPoolState,
)
from dr_exec.core.model import ContractModel
from dr_exec.declarations.models import ExecutionJob
from dr_exec.recording.models import CompletedExecution
from dr_exec.scheduling.scheduler import (
    SchedulerBroken,
    _AdmissionResult,
    _ExecutionScheduler,
    usable_cpu_count,
)


@dataclass(frozen=True, slots=True)
class AutoPoolCapacity:
    """Resolve capacity from usable CPUs when the pool opens."""


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
    """Resolved pool bound and its capacity source."""

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
    """Bounded scheduler owned by the event loop that opens it."""

    _executor: Executor
    _config: ExecutionPoolConfig
    _effective_capacity: EffectivePoolCapacity | None
    _state: ExecutionPoolState
    _closed: bool
    _scheduler: _ExecutionScheduler[object] | None
    _owning_loop: asyncio.AbstractEventLoop | None
    _close_task: asyncio.Task[None] | None
    _active_streams: int
    _stream_wakeups: set[asyncio.Event]
    _shutdown_complete: bool

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
        self._close_task = None
        self._active_streams = 0
        self._stream_wakeups = set()
        self._shutdown_complete = False

    @property
    def state(self) -> ExecutionPoolState:
        return self._state

    @property
    def effective_capacity(self) -> EffectivePoolCapacity:
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
        self._owning_loop = asyncio.get_running_loop()
        self._scheduler = _ExecutionScheduler(
            executor=self._executor,
            capacity=capacity.max_active_jobs,
            notify_change=self._notify_scheduler_change,
        )
        self._state = ExecutionPoolState.RUNNING
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
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
        """Yield shared-queue completions under one resident bound.

        All streams on a pool consume the same completion queue, so a stream
        may yield a completion for another stream's submission, paired with
        that submission's context.
        A scheduler break drains only completions already buffered before it
        raises; queued admitted work may already have been dropped.
        """

        self._require_owning_loop()
        scheduler = self._running_scheduler()
        source = aiter(submissions)
        exhausted = False
        carried: ExecutionSubmission[ContextT] | None = None
        source_pull: (
            asyncio.Task[ExecutionSubmission[ContextT] | None] | None
        ) = None
        wakeup = asyncio.Event()
        self._stream_wakeups.add(wakeup)
        self._active_streams += 1
        try:
            while True:
                wakeup.clear()
                if self._state is not ExecutionPoolState.RUNNING:
                    exhausted = True
                    carried = None
                    await _cancel_source_pull(source_pull)
                    source_pull = None

                completion = scheduler.take_completion_nowait()
                if completion is not None:
                    if (
                        not exhausted
                        and carried is None
                        and source_pull is None
                        and scheduler.can_admit()
                    ):
                        source_pull = asyncio.create_task(
                            _next_submission(source)
                        )
                    yield ExecutionCompletion(
                        completed_execution=completion.completed_execution,
                        context=cast("ContextT", completion.context),
                    )
                    continue

                if (
                    exhausted
                    and carried is None
                    and not scheduler.has_residents()
                ):
                    return

                if carried is not None:
                    match scheduler.admit(carried.job, carried.context):
                        case _AdmissionResult.ADMITTED:
                            carried = None
                            continue
                        case _AdmissionResult.INTAKE_CLOSED:
                            carried = None
                            exhausted = True
                            continue
                        case _AdmissionResult.NO_ROOM:
                            pass

                if source_pull is not None and source_pull.done():
                    submission = source_pull.result()
                    source_pull = None
                    if submission is None:
                        exhausted = True
                    elif self._state is ExecutionPoolState.RUNNING:
                        carried = submission
                    continue

                if (
                    not exhausted
                    and carried is None
                    and source_pull is None
                    and scheduler.can_admit()
                ):
                    source_pull = asyncio.create_task(_next_submission(source))

                changed = asyncio.create_task(wakeup.wait())
                waits: set[asyncio.Task[object]] = {changed}
                if source_pull is not None:
                    waits.add(source_pull)
                try:
                    await asyncio.wait(
                        waits, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    changed.cancel()
                    with suppress(asyncio.CancelledError):
                        await changed
        except SchedulerBroken:
            if self._state is ExecutionPoolState.RUNNING:
                self._state = ExecutionPoolState.BROKEN
            raise
        finally:
            await _cancel_source_pull(source_pull)
            self._stream_wakeups.discard(wakeup)
            self._active_streams -= 1
            if self._active_streams == 0 and self._shutdown_complete:
                scheduler.discard_completions()

    async def drain(self) -> None:
        """Await work the scheduler still owns, then close.

        After a scheduler break, queued admitted submissions may already have
        been dropped and are therefore outside this drain guarantee.
        """

        self._require_owning_loop()
        await self._close(abort=False)

    async def abort(self) -> None:
        """Cancel scheduler-owned work and await executor teardown."""

        self._require_owning_loop()
        await self._close(abort=True)

    async def _close(self, *, abort: bool) -> None:
        scheduler = self._closing_scheduler()
        if scheduler is None:
            return
        if self._close_task is None:
            self._state = ExecutionPoolState.DRAINING
            scheduler.close_intake()
            self._wake_streams()
            self._close_task = asyncio.create_task(
                self._finish_close(scheduler)
            )
        if abort:
            scheduler.cancel_all()
        close_task = self._close_task
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            await close_task
            raise

    async def _finish_close(
        self, scheduler: _ExecutionScheduler[object], /
    ) -> None:
        await asyncio.to_thread(scheduler.wait_for_quiescence)
        await asyncio.to_thread(scheduler.shutdown, preserve_completions=True)
        self._shutdown_complete = True
        self._closed = True
        self._state = _closed_state(scheduler.is_broken())
        self._wake_streams()
        if self._active_streams == 0:
            scheduler.discard_completions()

    def _notify_scheduler_change(self) -> None:
        loop = self._owning_loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._wake_streams)
        except RuntimeError:
            # A final worker notification can race event-loop teardown.
            pass

    def _wake_streams(self) -> None:
        for wakeup in self._stream_wakeups:
            wakeup.set()

    def _require_owning_loop(self) -> None:
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
        if self._closed:
            return None
        if self._scheduler is None:
            self._closed = True
            self._state = ExecutionPoolState.CLOSED
            return None
        return self._scheduler


def _closed_state(broke: bool, /) -> ExecutionPoolState:
    return ExecutionPoolState.BROKEN if broke else ExecutionPoolState.CLOSED


async def _next_submission[T](
    source: AsyncIterator[ExecutionSubmission[T]],
    /,
) -> ExecutionSubmission[T] | None:
    try:
        return await anext(source)
    except StopAsyncIteration:
        return None


async def _cancel_source_pull[T](
    source_pull: asyncio.Task[ExecutionSubmission[T] | None] | None,
    /,
) -> None:
    if source_pull is None:
        return
    source_pull.cancel()
    with suppress(asyncio.CancelledError, StopAsyncIteration):
        await source_pull


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
