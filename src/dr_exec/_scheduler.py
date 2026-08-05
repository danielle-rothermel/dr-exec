"""The one scheduler core behind every dr-exec pool entry point.

`ProcessExecutor.run_many`, `ProcessExecutor.open_pool`, and
`ExecutionPool.run_stream` are three surfaces over this single core. They
differ only in how they obtain the next submission -- a synchronous
iterable or an asynchronous one -- and in how they wait; admission,
capacity accounting, dispatch, completion ordering, cancellation, drain,
and abort all live here exactly once, so a behavior qualified through one
surface is the behavior the others have.

Three properties shape the implementation.

**One shared resident bound.** Capacity is not a count of running children
and separately a completion buffer. Admitted-but-unfinished submissions
and completed-but-undelivered submissions occupy one bound together, equal
to the effective capacity. A submission occupies its slot from admission
until its completion is handed to the consumer, so a finished result does
*not* admit replacement work while the bound is full. That is what makes a
slow consumer backpressure intake rather than accumulate results.

**Nothing per unadmitted job.** The core creates no thread, no future, no
child, and no queue entry for a job it has not admitted. Worker threads
are bounded by capacity and started lazily, so a pool that never sees more
than one concurrent job never starts more than one worker, and a
hundred-thousand-item source materializes nothing beyond the resident
submissions.

**Blocking waits synchronize on state.** Every wait here is on a
`Condition` predicate over scheduler state -- a delivered completion, a
finished worker, an emptied bound. No wait is a poll interval and no
elapsed time is evidence of anything.

Cancellation is per submission: each admitted submission gets its own
`CancelToken`, which is what `Executor.run` accepts, so abort cancels
exactly the calls that are in flight and each executor call performs its
own required teardown before its worker reports back.
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass
from threading import Condition, Thread
from typing import TYPE_CHECKING, Generic, TypeVar

from dr_exec.cancel import CancelToken
from dr_exec.errors import ExecutorFailure

if TYPE_CHECKING:
    from dr_exec.declare import ExecutionJob
    from dr_exec.protocols import Executor
    from dr_exec.record import CompletedExecution

ContextT = TypeVar("ContextT")

# The worker thread name prefix. Worker threads are the one thread kind
# this package creates for scheduling, and naming them makes a stray
# thread in a consumer's diagnostics attributable.
_WORKER_THREAD_PREFIX = "dr-exec-pool-worker"


def usable_cpu_count() -> int:
    """The usable CPU count automatic capacity resolves from, at least one.

    ``os.process_cpu_count`` reports the CPUs this process may actually
    use rather than the CPUs the machine has, which is the honest number
    for a pool whose whole purpose is bounding machine-level concurrency.
    It exists from Python 3.13; below that the machine count is the best
    available answer. Either way the floor is one active slot, because a
    pool with zero slots could never make progress.
    """
    reported = getattr(os, "process_cpu_count", os.cpu_count)()
    return max(1, reported or 1)


@dataclass(frozen=True, slots=True)
class _Admitted(Generic[ContextT]):  # noqa: UP046
    """One admitted submission and the token cancelling exactly its call."""

    job: ExecutionJob
    context: ContextT
    cancellation: CancelToken


@dataclass(frozen=True, slots=True)
class _Completion(Generic[ContextT]):  # noqa: UP046
    """One finished call paired with exactly its submission's context.

    The context is carried through in memory and never serialized: it is
    whatever object the caller submitted, moved from the submission to the
    completion untouched.
    """

    completed_execution: CompletedExecution
    context: ContextT


class SchedulerBroken(ExecutorFailure):
    """A scheduler-wide failure broke the pool.

    A per-job failure is completion data and the stream continues. This is
    the other case: machinery that prevents the scheduler itself from
    producing trustworthy completions, which breaks the pool rather than
    being reported as one job's outcome.
    """


class _ExecutionScheduler(Generic[ContextT]):  # noqa: UP046
    """Bounded admission, dispatch, and completion-order delivery.

    The core is deliberately synchronous and thread-based, because
    `Executor.run` is a blocking call: workers are where that blocking
    happens, and every surface -- synchronous or asynchronous -- waits on
    the same conditions over the same state.

    State is guarded by one condition variable. `_residents` is the shared
    bound's occupancy: it rises at admission and falls only at delivery.
    `_pending` holds admitted work no worker has picked up yet, `_ready`
    holds finished completions in completion order, and `_running` counts
    calls currently inside `Executor.run`.
    """

    __slots__ = (
        "_broken",
        "_capacity",
        "_condition",
        "_executor",
        "_intake_closed",
        "_pending",
        "_ready",
        "_residents",
        "_running",
        "_tokens",
        "_workers",
    )

    def __init__(self, *, executor: Executor, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("scheduler capacity must be positive")
        self._executor = executor
        self._capacity = capacity
        self._condition = Condition()
        self._pending: deque[_Admitted[ContextT]] = deque()
        self._ready: deque[_Completion[ContextT]] = deque()
        self._tokens: list[CancelToken] = []
        self._workers: list[Thread] = []
        self._residents = 0
        self._running = 0
        self._intake_closed = False
        self._broken: BaseException | None = None

    # --- Admission -------------------------------------------------------

    @property
    def capacity(self) -> int:
        return self._capacity

    def can_admit(self) -> bool:
        """Whether one more submission fits the shared resident bound now.

        This is the backpressure predicate, and the only gate a surface
        consults before pulling from its source: False means the source
        must not advance, because admitted-and-undelivered work already
        fills capacity. It never blocks -- a surface that finds no room
        delivers a completion, which is the one thing that makes room.
        """
        with self._condition:
            self._raise_if_broken()
            return self._residents < self._capacity and not self._intake_closed

    def admit(self, job: ExecutionJob, context: ContextT, /) -> None:
        """Take one resident slot and queue the submission for a worker.

        Callers reach here only after `can_admit` returned True, so the
        shared bound is never exceeded: every surface's intake loop is
        gated on that predicate, which is the one place the bound is
        enforced.
        """
        with self._condition:
            self._raise_if_broken()
            if self._intake_closed:
                raise SchedulerBroken("intake is closed")
            token = CancelToken()
            self._tokens.append(token)
            self._pending.append(_Admitted(job, context, token))
            self._residents += 1
            self._ensure_worker()
            self._condition.notify_all()

    def close_intake(self) -> None:
        """Stop admitting. Admitted work is untouched and still runs."""
        with self._condition:
            self._intake_closed = True
            self._condition.notify_all()

    # --- Delivery --------------------------------------------------------

    def has_residents(self) -> bool:
        """Whether any admitted submission is still undelivered."""
        with self._condition:
            self._raise_if_broken()
            return self._residents > 0

    def take_completion(self) -> _Completion[ContextT] | None:
        """Block until the next completion in completion order, if any.

        Completion order is the ready queue's own order: workers append as
        their calls return, and delivery pops the front, so a fast job
        submitted late is delivered before a slow job submitted early.

        Delivery is the one place a resident slot is released, which is
        what turns a consumed completion into admission room for the next
        submission. Returns None only when the scheduler holds nothing at
        all -- no ready completion, no running call, no admitted work --
        which a surface reaches only after its source is exhausted.
        """
        with self._condition:
            self._condition.wait_for(
                lambda: (
                    bool(self._ready)
                    or self._broken is not None
                    or (not self._running and not self._pending)
                )
            )
            self._raise_if_broken()
            if not self._ready:
                return None
            completion = self._ready.popleft()
            self._residents -= 1
            self._condition.notify_all()
            return completion

    # --- Cancellation and shutdown ---------------------------------------

    def cancel_all(self) -> None:
        """Cancel every submission this scheduler still owns.

        Both in-flight calls and admitted work no worker has picked up
        are cancelled: a submission cancelled before its worker starts it
        reaches `Executor.run` with an already-cancelled token, which the
        executor boundary answers with a recorded cancelled outcome and
        no child. Cancellation is therefore delivered as completion data
        for every admitted submission, never as a dropped submission.
        """
        with self._condition:
            for token in self._tokens:
                token.cancel()
            self._condition.notify_all()

    def wait_for_quiescence(self) -> None:
        """Block until no executor call is in flight and none is waiting.

        This is what "await their teardown before closing" means: each
        worker returns from `Executor.run` only after that call finished
        its own required teardown, so no running call and no pending
        admission is the exact point at which the pool may close.
        """
        with self._condition:
            self._condition.wait_for(
                lambda: not self._running and not self._pending
            )

    def shutdown(self) -> None:
        """Release worker threads and drop any undelivered state.

        Called once the owning surface is finished with the scheduler.
        Workers exit when intake is closed and nothing is left to run, so
        this joins rather than interrupts: a worker inside `Executor.run`
        is completing teardown, and abandoning it would be exactly the
        orphaned child the lifecycle claim exists to prevent.

        Completions still buffered at this point are dropped, and that is
        a real limit worth stating rather than a detail. Closing means the
        surface has stopped delivering: a drained stream has already
        delivered everything, but an aborted one may have finished calls
        whose results no consumer will ever read. Those runs are not
        lost -- each finished its own record before its worker returned --
        but their in-memory completions do not survive the close, so a
        consumer that needs every result closes by draining, not by
        aborting.
        """
        with self._condition:
            self._intake_closed = True
            self._condition.notify_all()
            workers = tuple(self._workers)
        for worker in workers:
            worker.join()
        with self._condition:
            self._workers.clear()
            self._pending.clear()
            self._ready.clear()
            self._tokens.clear()
            self._residents = 0

    # --- Worker mechanics ------------------------------------------------

    def _ensure_worker(self) -> None:
        """Start one more worker only if work would otherwise sit idle.

        Workers are capped at capacity and started lazily: the pool starts
        as many threads as it has concurrently admitted work, never one
        per job. The caller holds the condition.
        """
        idle = len(self._workers) - self._running
        if idle >= len(self._pending) or len(self._workers) >= self._capacity:
            return
        worker = Thread(
            target=self._work,
            name=f"{_WORKER_THREAD_PREFIX}-{len(self._workers)}",
            daemon=False,
        )
        self._workers.append(worker)
        worker.start()

    def _work(self) -> None:
        while True:
            admitted = self._take_admitted()
            if admitted is None:
                return
            self._run_one(admitted)

    def _take_admitted(self) -> _Admitted[ContextT] | None:
        with self._condition:
            self._condition.wait_for(
                lambda: (
                    bool(self._pending)
                    or self._intake_closed
                    or self._broken is not None
                )
            )
            if not self._pending:
                return None
            self._running += 1
            return self._pending.popleft()

    def _run_one(self, admitted: _Admitted[ContextT], /) -> None:
        """Run one admitted submission and publish exactly its completion.

        A failed call is not per-job completion data: `Executor.run`
        returns outcomes for everything it can observe about a child, so
        an exception escaping it means the machinery could not produce a
        trustworthy result at all. That is a scheduler-wide failure, and
        the pool breaks rather than pretending the job completed.
        """
        try:
            completed = self._executor.run(
                admitted.job, cancellation=admitted.cancellation
            )
        except BaseException as failure:  # noqa: BLE001
            self._finish(broken=failure)
            return
        self._finish(completion=_Completion(completed, admitted.context))

    def _finish(
        self,
        *,
        completion: _Completion[ContextT] | None = None,
        broken: BaseException | None = None,
    ) -> None:
        with self._condition:
            self._running -= 1
            if completion is not None:
                self._ready.append(completion)
            if broken is not None and self._broken is None:
                self._broken = broken
                self._intake_closed = True
            self._condition.notify_all()

    def _raise_if_broken(self) -> None:
        """Re-raise the scheduler-wide failure to every waiting surface.

        The caller holds the condition. The original failure stays as the
        cause, so the machinery error that broke the pool is never lost
        behind the scheduler's own message.
        """
        if self._broken is None:
            return
        raise SchedulerBroken(
            "the execution pool broke: an executor call failed"
        ) from self._broken


__all__ = [
    "SchedulerBroken",
    "_Admitted",
    "_Completion",
    "_ExecutionScheduler",
    "usable_cpu_count",
]
