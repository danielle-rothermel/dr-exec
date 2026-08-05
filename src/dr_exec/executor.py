"""The public production executor over the one private engine path."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec.cancel import CancelToken
from dr_exec.declare import ExecutionJob, ExecutorSelfBudgets
from dr_exec.engine import run_execution
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
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        """Execute one job through the one private engine path.

        Nothing is held between calls: the executor's runtime, store, and
        self-budgets are immutable and every mutable process, scratch,
        recording, and I/O value lives inside the engine call, so
        concurrent calls on one executor stay fully separate.
        """
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
