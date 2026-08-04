from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec.declare import ExecutionJob, ExecutorSelfBudgets
from dr_exec.pool import ExecutionPool, ExecutionPoolConfig
from dr_exec.protocols import RunStore, Runtime
from dr_exec.record import CompletedExecution


@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = field(
        default_factory=ExecutorSelfBudgets.unbudgeted
    )

    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        raise NotImplementedError("ProcessExecutor.run is not implemented")

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]:
        raise NotImplementedError(
            "ProcessExecutor.run_many is not implemented"
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        raise NotImplementedError(
            "ProcessExecutor.open_pool is not implemented"
        )


__all__ = ["ProcessExecutor"]
