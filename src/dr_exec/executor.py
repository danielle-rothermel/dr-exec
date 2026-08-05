"""The public production executor over the one private engine path.

`run` is one job through the engine. `run_many` and `open_pool` are the
same executor under the one scheduler core: `run_many` drives it
synchronously over a finite iterable, `open_pool` hands it to
`ExecutionPool` for an async stream. Neither adds scheduling policy --
capacity, admission, completion order, backpressure, cancellation, and
drain are the scheduler's, once.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec._scheduler import _ExecutionScheduler
from dr_exec.cancel import CancelToken
from dr_exec.declare import ExecutionJob, ExecutorSelfBudgets
from dr_exec.engine import run_execution
from dr_exec.pool import (
    ExecutionPool,
    ExecutionPoolConfig,
    _resolve_capacity,
)
from dr_exec.protocols import Executor, RunStore, Runtime
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
        """Run a finite batch through the same scheduler the pool uses.

        The iterable is consumed lazily and admitted only while resident
        capacity exists, so a batch far larger than capacity never
        materializes: at any moment the scheduler holds at most capacity
        submissions, running or completed-but-undelivered.

        Completions arrive in completion order and per-job failures are
        completion data like any other outcome. When the input is
        exhausted the admitted work drains and the scheduler closes --
        including when the caller abandons the generator, because the
        drain runs from the generator's own cleanup.
        """
        return _run_batch(
            self,
            jobs,
            capacity=_resolve_capacity(
                (config or ExecutionPoolConfig()).capacity
            ).max_active_jobs,
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        """Build a pool over this executor; entering it resolves capacity.

        The pool is returned unopened. Automatic capacity resolves when
        the pool is entered rather than here, so the bound belongs to the
        pool's own lifetime and not to the moment a caller happened to
        construct it.
        """
        return ExecutionPool(
            executor=self,
            config=config or ExecutionPoolConfig(),
        )


def _run_batch(
    executor: Executor,
    jobs: Iterable[ExecutionJob],
    /,
    *,
    capacity: int,
) -> Iterator[CompletedExecution]:
    """Drive the scheduler core synchronously over a finite iterable.

    This is the same loop `ExecutionPool.run_stream` runs, with the async
    source and the offloaded waits removed: pull while the shared resident
    bound has room, then deliver one completion, which is what frees the
    slot the next pull needs. Keeping the shape identical is the point --
    admission, ordering, and backpressure are the scheduler's, so the two
    surfaces cannot drift apart.

    The `finally` is load-bearing. A caller who stops consuming leaves
    admitted calls in flight, and closing the generator must still let
    them finish their own teardown before the scheduler's workers are
    released.
    """
    scheduler: _ExecutionScheduler[None] = _ExecutionScheduler(
        executor=executor, capacity=capacity
    )
    source = iter(jobs)
    exhausted = False
    try:
        while True:
            while not exhausted and scheduler.can_admit():
                job = next(source, None)
                if job is None:
                    exhausted = True
                    break
                scheduler.admit(job, None)
            if exhausted and not scheduler.has_residents():
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
