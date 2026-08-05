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
from enum import UNIQUE, Enum, auto, verify
from threading import Condition, Thread
from typing import TYPE_CHECKING, Generic, TypeVar

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure

if TYPE_CHECKING:
    from dr_exec.capabilities.protocols import Executor
    from dr_exec.declarations.models import ExecutionJob
    from dr_exec.recording.models import CompletedExecution

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


@verify(UNIQUE)
class _AdmissionResult(Enum):
    """What `admit` did with a submission a surface had already pulled.

    The two refusals are separate members because a caller must act on
    them differently, and collapsing them into one falsy answer is a
    silent truncation bug: a surface that reads a full bound as a
    requested close ends its stream while still holding a good
    submission it pulled and never delivered.

    This is in-memory control flow between the scheduler and the
    surfaces driving it. It is never serialized, never recorded, and
    never reaches a caller, so it carries no wire values.
    """

    ADMITTED = auto()
    """The submission took a resident slot and is queued for a worker."""

    INTAKE_CLOSED = auto()
    """A requested close landed; intake is over and the stream ends."""

    NO_ROOM = auto()
    """The bound is full. The submission is still good -- retain it and
    retry after a delivery, which is what frees a slot."""


@dataclass(frozen=True, slots=True)
class _Admitted(Generic[ContextT]):  # noqa: UP046
    """One admitted submission and the token cancelling exactly its call.

    ``ticket`` names this submission's occupancy of the shared resident
    bound. It is what the scheduler's live-token map is keyed by, so the
    token is discarded at exactly the moment the submission stops being a
    resident rather than at the pool's end of life.
    """

    ticket: int
    job: ExecutionJob
    context: ContextT
    cancellation: CancelToken


@dataclass(frozen=True, slots=True)
class _Completion(Generic[ContextT]):  # noqa: UP046
    """One finished call paired with exactly its submission's context.

    The context is carried through in memory and never serialized: it is
    whatever object the caller submitted, moved from the submission to the
    completion untouched.

    The ticket rides along for the same reason: delivery is where the
    resident slot is released, so delivery is where the submission's
    cancellation token is dropped.
    """

    ticket: int
    completed_execution: CompletedExecution
    context: ContextT


class SchedulerBroken(ExecutorFailure):
    """A scheduler-wide failure broke the pool.

    A per-job failure is completion data and the stream continues. This is
    the other case: machinery that prevents the scheduler itself from
    producing trustworthy completions, which breaks the pool rather than
    being reported as one job's outcome.

    A break ends intake and delivery, in that order. Completions already
    buffered when it lands are calls that genuinely finished and recorded,
    so they are delivered first and this is raised once the buffer is
    empty. What a break loses is undelivered work admitted before it --
    submissions dropped from the queue unstarted, and calls still in
    flight whose results arrive after delivery has ended.
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

    `_tokens` holds exactly the residents' cancellation tokens, keyed by
    ticket. It is sized by the bound like every other structure here, not
    by how many submissions the scheduler has ever seen: a token is
    discarded when its submission leaves the bound, which is what lets one
    long-lived pool run a hundred thousand jobs without accumulating a
    hundred thousand tokens and their locks.
    """

    __slots__ = (
        "_broken",
        "_capacity",
        "_condition",
        "_executor",
        "_intake_closed",
        "_next_ticket",
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
        self._tokens: dict[int, CancelToken] = {}
        self._workers: list[Thread] = []
        self._next_ticket = 0
        self._residents = 0
        self._running = 0
        self._intake_closed = False
        self._broken: BaseException | None = None

    # --- Admission -------------------------------------------------------

    def can_admit(self) -> bool:
        """Whether one more submission fits the shared resident bound now.

        This is the backpressure predicate a surface consults before
        pulling from its source: False means the source must not
        advance, because admitted-and-undelivered work already fills
        capacity. It never blocks -- a surface that finds no room
        delivers a completion, which is the one thing that makes room.

        It is a hint, not the enforcement point. The answer is only true
        of the instant it was read, and every surface awaits its source
        between reading it and admitting, so `admit` re-checks the bound
        itself. Pulling on a stale True costs one retained submission,
        never an over-admission.

        A break answers False rather than raising, because a break ends
        intake before it ends delivery: raising here would abandon the
        completions the surface is about to drain.
        """
        with self._condition:
            return self._residents < self._capacity and not self._intake_closed

    def admit(
        self, job: ExecutionJob, context: ContextT, /
    ) -> _AdmissionResult:
        """Take one resident slot and queue the submission for a worker.

        This is where the shared bound is enforced, not `can_admit`. A
        surface checks `can_admit` and then *awaits* its source before
        admitting, and that await releases the condition: several source
        loops feeding one pool can all observe room and all arrive here.
        So the bound is re-checked under the lock actually held at the
        moment the slot is taken, which is the only check a concurrent
        feeder cannot slip past.

        The two refusals are distinct answers and a caller must not
        conflate them. `INTAKE_CLOSED` is the end of intake -- a surface
        that pulled a submission before a concurrent `drain`, `abort`, or
        break landed -- and the stream stops pulling. `NO_ROOM` says only
        "not now": the submission is still good and the caller must retain
        it across a delivery rather than dropping it, because delivery is
        what frees the slot it needs.

        A break is `INTAKE_CLOSED` here rather than a raise. It has closed
        intake, so this is the truthful answer, and it leaves the surface
        free to reach delivery -- where the buffered completions drain and
        the break is raised once they are gone. Reporting it here would
        end the stream on the break before those completions were handed
        over.
        """
        with self._condition:
            if self._intake_closed:
                return _AdmissionResult.INTAKE_CLOSED
            if self._residents >= self._capacity:
                return _AdmissionResult.NO_ROOM
            ticket = self._next_ticket
            self._next_ticket += 1
            token = CancelToken()
            self._tokens[ticket] = token
            self._pending.append(_Admitted(ticket, job, context, token))
            self._residents += 1
            self._ensure_worker()
            self._condition.notify_all()
            # A worker that could not start breaks the scheduler and
            # drops the queue this submission was just placed in, so
            # reporting it admitted would promise a run nothing will
            # perform. Intake is over, which is what the surface needs to
            # stop pulling; the break itself is raised at delivery, after
            # whatever is already buffered has been handed over.
            if self._broken is not None:
                return _AdmissionResult.INTAKE_CLOSED
            return _AdmissionResult.ADMITTED

    def close_intake(self) -> None:
        """Stop admitting. Admitted work is untouched and still runs."""
        with self._condition:
            self._intake_closed = True
            self._condition.notify_all()

    # --- Delivery --------------------------------------------------------

    def has_residents(self) -> bool:
        """Whether any admitted submission is still undelivered.

        A break does not raise here either. Residents outlive a break --
        buffered completions and calls still in flight both hold slots --
        and this is the predicate a surface uses to decide whether to
        deliver at all, so answering with the raise would skip the drain
        the break is supposed to precede.
        """
        with self._condition:
            return self._residents > 0

    def is_broken(self) -> bool:
        """Whether a scheduler-wide failure has landed, without raising.

        `take_completion` reports the break by raising, which is right for
        a consumer that was mid-stream. A close is not mid-stream: it must
        still shut down and join its workers and *then* report the break
        as the state it landed in, including a break that landed after the
        last delivery, so it asks rather than being raised at.
        """
        with self._condition:
            return self._broken is not None

    def take_completion(self) -> _Completion[ContextT] | None:
        """Block until the next completion in completion order, if any.

        Completion order is the ready queue's own order: workers append as
        their calls return, and delivery pops the front, so a fast job
        submitted late is delivered before a slow job submitted early.

        Delivery is the one place a resident slot is released, which is
        what turns a consumed completion into admission room for the next
        submission. It is therefore also where the submission's
        cancellation token is dropped: the call is over and nothing can
        cancel it any more, so retaining the token past this point would
        grow a structure the bound is supposed to size.

        Returns None only when the scheduler holds nothing at all -- no
        ready completion, no running call, no admitted work -- which a
        surface reaches only after its source is exhausted.

        This is the one place a break is raised, and it is raised last.
        A buffered completion is a call that genuinely ran and recorded
        its own result, and a machinery failure on some *other* job says
        nothing about it, so the buffer drains first and the break is
        raised only once nothing is left to hand over. Delivery therefore
        ends with the tail the pool actually produced, and then with the
        failure that stopped it -- never with the failure alone.
        """
        with self._condition:
            self._condition.wait_for(
                lambda: (
                    bool(self._ready)
                    or self._broken is not None
                    or (not self._running and not self._pending)
                )
            )
            if not self._ready:
                self._raise_if_broken()
                return None
            completion = self._ready.popleft()
            self._tokens.pop(completion.ticket, None)
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
        no child. Cancellation therefore reaches every submission that is
        started as completion data rather than as a vanished call.

        It is not a delivery promise. Two paths end without handing that
        data to a consumer: a break drops the queue behind the failing
        call, so submissions cancelled there are never dispatched at all,
        and `shutdown` drops whatever the consumer stopped reading. Both
        record what actually ran; neither guarantees the consumer sees
        it, which is why a consumer that needs every result consumes
        until its stream ends.

        Only current residents are held, so this walks the bound rather
        than the scheduler's history: an abort after a hundred thousand
        jobs cancels the handful still in flight.
        """
        with self._condition:
            for token in tuple(self._tokens.values()):
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
        surface has stopped delivering: a stream consumed to its end has
        already taken everything, including the buffered tail a break
        hands over before it raises, but an aborted one may have finished
        calls whose results no consumer will ever read. Those runs are not
        lost -- each finished its own record before its worker returned --
        but their in-memory completions do not survive the close, so a
        consumer that needs every result consumes its stream to the end
        rather than closing over it.
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

        A thread that cannot start is a scheduler-wide failure, not a
        recoverable admission: the submission is queued for a worker that
        will never exist, so every wait for quiescence would block on
        work nothing can run. The break path is what that is for, so this
        takes it -- the caller sees `SchedulerBroken` with the resource
        error as its cause instead of an unbounded wait.

        The worker is counted only once it is running. Recording it first
        would leave a never-started thread in the roster for `shutdown`
        to join, which raises rather than closing the pool -- a second
        failure on top of the one being reported.
        """
        idle = len(self._workers) - self._running
        if idle >= len(self._pending) or len(self._workers) >= self._capacity:
            return
        worker = Thread(
            target=self._work,
            name=f"{_WORKER_THREAD_PREFIX}-{len(self._workers)}",
            daemon=False,
        )
        try:
            worker.start()
        except BaseException as failure:  # noqa: BLE001
            self._break(failure)
            self._condition.notify_all()
            return
        self._workers.append(worker)

    def _work(self) -> None:
        while True:
            admitted = self._take_admitted()
            if admitted is None:
                return
            self._run_one(admitted)

    def _take_admitted(self) -> _Admitted[ContextT] | None:
        """The next submission to run, or None when the worker should stop.

        A broken scheduler dispatches nothing further: `_finish` drops
        the queue at the moment of the break, and this guard keeps a
        worker that was mid-wait from starting a submission admitted in
        the same window. It bounds dispatch, not flight -- a submission
        this method already returned is inside `Executor.run` by the time
        a concurrent break lands, and finishes there.
        """
        with self._condition:
            self._condition.wait_for(
                lambda: (
                    bool(self._pending)
                    or self._intake_closed
                    or self._broken is not None
                )
            )
            if self._broken is not None or not self._pending:
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
            self._finish(ticket=admitted.ticket, broken=failure)
            return
        self._finish(
            ticket=admitted.ticket,
            completion=_Completion(
                admitted.ticket, completed, admitted.context
            ),
        )

    def _finish(
        self,
        *,
        ticket: int,
        completion: _Completion[ContextT] | None = None,
        broken: BaseException | None = None,
    ) -> None:
        """Retire one call, publishing its completion or breaking the pool.

        A break drops this submission's token here rather than at
        delivery, because a failed call produced no completion to deliver:
        the ticket would otherwise stay resident for the pool's remaining
        life. Its resident slot is deliberately not released -- the
        scheduler must stay non-empty so every surface reaches delivery
        once more and is told the pool broke.

        Breaking also stops further dispatch: every submission still
        queued here is cancelled and dropped with the queue rather than
        started. A broken scheduler delivers only what it has already
        buffered, so starting that queue would spawn children, write
        durable records, and consume the very capacity the scheduler
        exists to bound, all for results no consumer can ever receive.
        Those submissions were never started, so nothing is dropped
        mid-flight and no teardown is skipped.

        What breaking does *not* do is stop a call another worker already
        entered. With capacity above one, a second worker can take a
        submission out of the queue and be inside `Executor.run` before
        the failing call reaches this point. That call is left alone: it
        runs to its own end, so its teardown completes and its record is
        written. Whether its in-memory result is delivered depends on
        where the consumer is: delivery drains the buffer before raising,
        so a result that lands there in time is handed over, and one that
        arrives after the buffer emptied is discarded. Cancelling the
        call instead would trade a possibly-useful result for a torn-down
        child on a path that is already failing, which is the worse
        teardown story. The guarantee is that a break dispatches nothing
        more, not that nothing is in flight when it lands.
        """
        with self._condition:
            self._running -= 1
            if completion is not None:
                self._ready.append(completion)
            else:
                self._tokens.pop(ticket, None)
            if broken is not None:
                self._break(broken)
            self._condition.notify_all()

    def _break(self, failure: BaseException, /) -> None:
        """Record a scheduler-wide failure and stop dispatching anything.

        The first failure is the one kept: a break already in progress
        has closed intake and dropped the queue, and a later failure --
        typically a consequence of the first -- must not displace the
        cause a consumer will be shown.

        Every submission still queued is cancelled and dropped rather
        than started, and quiescence is reachable afterwards precisely
        because the queue is emptied here. The caller holds the
        condition; it notifies.
        """
        if self._broken is not None:
            return
        self._broken = failure
        self._intake_closed = True
        for queued in self._pending:
            queued.cancellation.cancel()
            self._tokens.pop(queued.ticket, None)
        self._pending.clear()

    def _raise_if_broken(self) -> None:
        """Report the scheduler-wide failure at the one place delivery ends.

        Only `take_completion` calls this, and only with nothing left to
        hand over: intake answers a break by closing rather than raising,
        so the raise happens once and marks the end of the stream.

        The caller holds the condition. The original failure stays as the
        cause, so the machinery error that broke the pool is never lost
        behind the scheduler's own message -- which is why the message
        names the class of failure rather than one of its causes. A
        failed executor call and a worker thread that could not start
        both break the scheduler, and the cause is where they differ.
        """
        if self._broken is None:
            return
        raise SchedulerBroken(
            "the execution pool broke: its scheduling machinery failed"
        ) from self._broken


__all__ = [
    "SchedulerBroken",
    "_Admitted",
    "_Completion",
    "_ExecutionScheduler",
    "usable_cpu_count",
]
