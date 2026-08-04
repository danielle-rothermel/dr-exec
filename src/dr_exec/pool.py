from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from types import TracebackType
from typing import Generic, Literal, TypeVar, cast

from pydantic import NonNegativeInt, PositiveInt

from dr_exec._model import ContractModel
from dr_exec.declare import ExecutionJob
from dr_exec.engine import _ExecutionScheduler
from dr_exec.kinds import (
    CapacitySource,
    ExecutionPoolState,
)
from dr_exec.protocols import Executor
from dr_exec.record import CompletedExecution


@dataclass(frozen=True, slots=True)
class AutoPoolCapacity:
    pass


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
    max_prefetched_jobs: int = 0

    def __post_init__(self) -> None:
        if self.max_prefetched_jobs < 0:
            raise ValueError("max_prefetched_jobs must be nonnegative")


class EffectivePoolCapacity(ContractModel):
    source: CapacitySource
    cpu_count: PositiveInt
    max_active_jobs: PositiveInt
    max_prefetched_jobs: NonNegativeInt
    native_threads_per_job: Literal[1] = 1


ContextT = TypeVar("ContextT")


@dataclass(frozen=True, slots=True)
class ExecutionSubmission(Generic[ContextT]):  # noqa: UP046
    job: ExecutionJob
    context: ContextT


@dataclass(frozen=True, slots=True)
class ExecutionCompletion(Generic[ContextT]):  # noqa: UP046
    completed_execution: CompletedExecution
    context: ContextT


class ExecutionPool:
    _executor: Executor
    _config: ExecutionPoolConfig
    _effective_capacity: EffectivePoolCapacity | None
    _state: ExecutionPoolState
    _scheduler: _ExecutionScheduler | None

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
    def effective_capacity(self) -> EffectivePoolCapacity:
        raise NotImplementedError(
            "ExecutionPool.effective_capacity is not implemented"
        )

    async def __aenter__(self) -> ExecutionPool:  # noqa: PYI034
        raise NotImplementedError(
            "ExecutionPool.__aenter__ is not implemented"
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        raise NotImplementedError("ExecutionPool.__aexit__ is not implemented")

    async def run_stream(
        self,
        submissions: AsyncIterable[ExecutionSubmission[ContextT]],
        /,
    ) -> AsyncIterator[ExecutionCompletion[ContextT]]:
        if False:
            yield cast(ExecutionCompletion[ContextT], None)
        raise NotImplementedError(
            "ExecutionPool.run_stream is not implemented"
        )

    async def drain(self) -> None:
        raise NotImplementedError("ExecutionPool.drain is not implemented")

    async def abort(self) -> None:
        raise NotImplementedError("ExecutionPool.abort is not implemented")


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
