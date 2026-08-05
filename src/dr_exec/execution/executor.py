"""The public production executor over the one private engine path.

`run` is one job through the engine. `run_many` and `open_pool` are the
same executor under the one scheduler core: `run_many` drives it
synchronously over a finite iterable, `open_pool` hands it to
`ExecutionPool` for an async stream. Neither adds scheduling policy --
capacity, admission, completion order, backpressure, cancellation, and
drain are the scheduler's, once.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass, field

from dr_exec.capabilities.protocols import Executor, RunStore, Runtime
from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import ExecutionJob, ExecutorSelfBudgets
from dr_exec.execution.engine import run_execution
from dr_exec.recording.models import CompletedExecution
from dr_exec.scheduling.pool import (
    ExecutionPool,
    ExecutionPoolConfig,
    _resolve_capacity,
)
from dr_exec.scheduling.scheduler import _AdmissionResult, _ExecutionScheduler


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
) -> Generator[CompletedExecution]:
    """Drive the scheduler core synchronously over a finite iterable.

    This is the same loop `ExecutionPool.run_stream` runs, with the async
    source and the offloaded waits removed: pull while the shared resident
    bound has room, then deliver one completion, which is what frees the
    slot the next pull needs. Keeping the shape identical is the point --
    admission, ordering, and backpressure are the scheduler's, so the two
    surfaces cannot drift apart.

    That includes the carry slot for a refused admission, which one
    synchronous driver can never actually need: nothing runs between this
    loop's admission check and its admission, so the bound cannot fill
    behind it. It is here because the shared shape is the guarantee. A
    surface that dropped a pulled job on a refusal would silently lose a
    run, and the two loops staying one shape is what keeps that from
    being true on one surface and not the other.

    Being a generator is part of the contract, not an implementation
    detail: the `finally` is where a caller who stops consuming still gets
    a drain. Closing the generator -- explicitly or by dropping it -- runs
    that cleanup, so admitted calls finish their own teardown before the
    scheduler's workers are released, and no executor call is ever left
    running behind the batch's back.
    """
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
                        # One driver pulls this loop, so the bound cannot
                        # close behind it -- but the carry path is what
                        # keeps this surface the same loop the pool runs,
                        # and a dropped job here would be a lost run.
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
