from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec.capabilities.protocols import RunStore, Runtime
from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import ExecutionJob, ExecutorSelfBudgets
from dr_exec.execution.engine import run_execution
from dr_exec.recording.models import CompletedExecution
from dr_exec.scheduling.offload import offload_run_blocking
from dr_exec.scheduling.pool import (
    AutoPoolCapacity,
    ExecutionPool,
    ExecutionPoolConfig,
    batch_capacity,
)
from dr_exec.scheduling.scheduler import run_batch


@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    """Production executor supported on qualified POSIX platforms."""

    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = field(
        default_factory=ExecutorSelfBudgets
    )

    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        return await offload_run_blocking(self, job, cancellation=cancellation)

    def run_blocking(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        return run_execution(
            job,
            runtime=self.runtime,
            run_store=self.run_store,
            self_budgets=self.self_budgets,
            cancellation=cancellation,
        )

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]:
        return run_batch(
            self,
            jobs,
            capacity=batch_capacity(config, default=AutoPoolCapacity()),
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        return ExecutionPool(
            executor=self,
            config=config or ExecutionPoolConfig(capacity=AutoPoolCapacity()),
        )


__all__ = ["ProcessExecutor"]
