from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec.capabilities.protocols import Executor, RunStore, Runtime
from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import ExecutionJob, ExecutorSelfBudgets
from dr_exec.execution.engine import run_execution
from dr_exec.recording.models import CompletedExecution
from dr_exec.scheduling.offload import offload_run_blocking
from dr_exec.scheduling.pool import (
    AutoPoolCapacity,
    ExecutionPool,
    ExecutionPoolConfig,
    resolve_pool_capacity,
)
from dr_exec.scheduling.scheduler import _AdmissionResult, _ExecutionScheduler


@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    """Production executor supported on macOS only."""

    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = field(
        default_factory=ExecutorSelfBudgets.unbudgeted
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
        """Stream a finite batch in completion order.

        Input is consumed lazily. Exhaustion or explicit close drains admitted
        work; dropping an unclosed iterator does not guarantee prompt cleanup.
        """
        return _run_batch(
            self,
            jobs,
            capacity=resolve_pool_capacity(
                (
                    config or ExecutionPoolConfig(capacity=AutoPoolCapacity())
                ).capacity
            ).max_active_jobs,
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


def _run_batch(
    executor: Executor,
    jobs: Iterable[ExecutionJob],
    /,
    *,
    capacity: int,
) -> Generator[CompletedExecution]:
    """Drive a private bounded scheduler and drain it on generator close."""
    scheduler: _ExecutionScheduler[None] = _ExecutionScheduler(
        executor=executor, capacity=capacity
    )
    source = iter(jobs)
    exhausted = False
    carried: ExecutionJob | None = None
    try:
        while True:
            while not exhausted and scheduler.can_admit():
                job = carried if carried is not None else next(source, None)
                carried = None
                if job is None:
                    exhausted = True
                    break
                match scheduler.admit(job, None):
                    case _AdmissionResult.ADMITTED:
                        pass
                    case _AdmissionResult.INTAKE_CLOSED:
                        exhausted = True
                        break
                    case _AdmissionResult.NO_ROOM:
                        carried = job
                        break
            if exhausted and carried is None and not scheduler.has_residents():
                return
            completion = scheduler.take_completion()
            if completion is None:
                return
            yield completion.completed_execution
    finally:
        scheduler.close_intake()
        scheduler.wait_for_quiescence()
        scheduler.shutdown()


__all__ = ["ProcessExecutor"]
