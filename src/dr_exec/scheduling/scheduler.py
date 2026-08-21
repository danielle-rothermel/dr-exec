from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Generator, Iterable
from dataclasses import dataclass
from enum import UNIQUE, Enum, auto, verify
from threading import Condition, Event, Thread
from typing import TYPE_CHECKING

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import ExecutorFailureCode
from dr_exec.declarations.models import FiniteDurationLimit

if TYPE_CHECKING:
    from dr_exec.capabilities.protocols import Executor
    from dr_exec.declarations.models import ExecutionJob
    from dr_exec.recording.models import CompletedExecution

_WORKER_THREAD_PREFIX = "dr-exec-pool-worker"


@verify(UNIQUE)
class AdmissionResult(Enum):
    ADMITTED = auto()

    INTAKE_CLOSED = auto()

    NO_ROOM = auto()


@dataclass(frozen=True, slots=True)
class _Admitted[ContextT]:
    ticket: int
    job: ExecutionJob
    context: ContextT
    cancellation: CancelToken


@dataclass(frozen=True, slots=True)
class _Completion[ContextT]:
    ticket: int
    completed_execution: CompletedExecution
    context: ContextT


class SchedulerBroken(ExecutorFailure):
    """Scheduler machinery failed, ending trustworthy delivery.

    Buffered completions drain first; queued admitted work is dropped and
    in-flight results arriving after delivery ends are not delivered. See the
    ``Scheduler break drops undelivered work`` contract in ``.defs/contracts.toml``.
    """

    def __init__(self, message: str, /) -> None:
        super().__init__(message, code=ExecutorFailureCode.SCHEDULER_BROKEN)


class ExecutionScheduler[ContextT]:
    """Share one resident bound and completion queue across drivers."""

    __slots__ = (
        "_broken",
        "_capacity",
        "_condition",
        "_executor",
        "_expired",
        "_intake_closed",
        "_next_ticket",
        "_notify_change",
        "_pending",
        "_ready",
        "_residents",
        "_running",
        "_tokens",
        "_workers",
    )

    def __init__(
        self,
        *,
        executor: Executor,
        capacity: int,
        notify_change: Callable[[], None] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("scheduler capacity must be positive")
        self._executor = executor
        self._capacity = capacity
        self._notify_change = notify_change
        self._condition = Condition()
        self._pending: deque[_Admitted[ContextT]] = deque()
        self._ready: deque[_Completion[ContextT]] = deque()
        self._tokens: dict[int, CancelToken] = {}
        self._workers: list[Thread] = []
        self._next_ticket = 0
        self._residents = 0
        self._running = 0
        self._intake_closed = False
        self._expired = False
        self._broken: BaseException | None = None

    def can_admit(self) -> bool:
        with self._condition:
            return self._residents < self._capacity and not self._intake_closed

    def admit(
        self, job: ExecutionJob, context: ContextT, /
    ) -> AdmissionResult:
        with self._condition:
            if self._intake_closed:
                return AdmissionResult.INTAKE_CLOSED
            if self._residents >= self._capacity:
                return AdmissionResult.NO_ROOM
            ticket = self._next_ticket
            self._next_ticket += 1
            token = CancelToken()
            if self._expired:
                token.cancel()
            self._tokens[ticket] = token
            self._pending.append(_Admitted(ticket, job, context, token))
            self._residents += 1
            self._ensure_worker()
            self._announce_change()
            # A failed worker start drops this queued submission; buffered
            # completions still drain before the break surfaces.
            if self._broken is not None:
                return AdmissionResult.INTAKE_CLOSED
            return AdmissionResult.ADMITTED

    def close_intake(self) -> None:
        with self._condition:
            self._intake_closed = True
            self._announce_change()

    def has_residents(self) -> bool:
        with self._condition:
            return self._residents > 0

    def is_broken(self) -> bool:
        with self._condition:
            return self._broken is not None

    def take_completion_nowait(
        self, /, *, owned_by: Callable[[ContextT], bool] | None = None
    ) -> _Completion[ContextT] | None:
        """Atomically hand one buffered completion to the calling driver.

        With ``owned_by``, the driver takes only the oldest completion whose
        context it recognizes as its own and leaves the rest buffered for the
        driver that owns them, so drivers sharing one scheduler never consume
        each other's completions.

        A break surfaces to a driver that finds nothing of its own, even while
        another driver's completions are still buffered: those are results this
        driver can never take, so treating the queue as non-empty would hide
        the break behind work that will never be handed over.
        """

        with self._condition:
            if not self._ready:
                self._raise_if_broken()
                return None
            if owned_by is None:
                return self._take_ready()
            completion = self._take_ready_owned(owned_by)
            if completion is None:
                self._raise_if_broken()
            return completion

    def take_completion(self) -> _Completion[ContextT] | None:
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
            return self._take_ready()

    def cancel_all(self) -> None:
        with self._condition:
            for token in tuple(self._tokens.values()):
                token.cancel()
            self._announce_change()

    def expire(self) -> None:
        """Cancel live work and birth later tokens already cancelled.

        Intake stays open so remaining source jobs still admit and complete
        as ``CancelledOutcome`` rather than vanishing from the batch.
        """

        with self._condition:
            self._expired = True
            for token in tuple(self._tokens.values()):
                token.cancel()
            self._announce_change()

    def wait_for_quiescence(self) -> None:
        with self._condition:
            self._condition.wait_for(
                lambda: not self._running and not self._pending
            )

    def shutdown(self, *, preserve_completions: bool = False) -> None:
        with self._condition:
            self._intake_closed = True
            self._announce_change()
            workers = tuple(self._workers)
        for worker in workers:
            worker.join()
        with self._condition:
            self._workers.clear()
            self._pending.clear()
            if not preserve_completions:
                self._discard_completions()
            self._announce_change()

    def discard_completions(self) -> None:
        """Release results after no event-loop stream can consume them."""

        with self._condition:
            self._discard_completions()
            self._announce_change()

    def release_owned(self, owned_by: Callable[[ContextT], bool], /) -> None:
        """Drop completions only a departed driver could ever have taken.

        An owned completion is claimable by exactly one driver, so once that
        driver is gone its completions are unreachable while still counting
        against the shared resident bound. Releasing them is what keeps a
        driver that ended early from stalling the drivers that outlive it.
        """

        with self._condition:
            kept = deque(
                completion
                for completion in self._ready
                if not owned_by(completion.context)
            )
            for completion in self._ready:
                if owned_by(completion.context):
                    self._tokens.pop(completion.ticket, None)
                    self._residents -= 1
            self._ready = kept
            self._announce_change()

    def _ensure_worker(self) -> None:
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
            self._announce_change()
            return
        self._workers.append(worker)

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
            if self._broken is not None or not self._pending:
                return None
            self._running += 1
            return self._pending.popleft()

    def _run_one(self, admitted: _Admitted[ContextT], /) -> None:
        try:
            completed = self._executor.run_blocking(
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
        with self._condition:
            self._running -= 1
            if completion is not None:
                self._ready.append(completion)
            else:
                self._tokens.pop(ticket, None)
            if broken is not None:
                self._break(broken)
            self._announce_change()

    def _take_ready(self) -> _Completion[ContextT]:
        completion = self._ready.popleft()
        self._tokens.pop(completion.ticket, None)
        self._residents -= 1
        self._announce_change()
        return completion

    def _take_ready_owned(
        self, owned_by: Callable[[ContextT], bool], /
    ) -> _Completion[ContextT] | None:
        """Take the oldest completion this driver owns, preserving the rest."""

        for position, completion in enumerate(self._ready):
            if not owned_by(completion.context):
                continue
            del self._ready[position]
            self._tokens.pop(completion.ticket, None)
            self._residents -= 1
            self._announce_change()
            return completion
        return None

    def _discard_completions(self) -> None:
        self._ready.clear()
        self._tokens.clear()
        self._residents = 0

    def _announce_change(self) -> None:
        self._condition.notify_all()
        if self._notify_change is not None:
            self._notify_change()

    def _break(self, failure: BaseException, /) -> None:
        if self._broken is not None:
            return
        self._broken = failure
        self._intake_closed = True
        for queued in self._pending:
            queued.cancellation.cancel()
            self._tokens.pop(queued.ticket, None)
        self._pending.clear()

    def _raise_if_broken(self) -> None:
        if self._broken is None:
            return
        raise SchedulerBroken(
            "the execution pool broke: its scheduling machinery failed"
        ) from self._broken


def run_batch(
    executor: Executor,
    jobs: Iterable[ExecutionJob],
    /,
    *,
    capacity: int,
    wall_time: FiniteDurationLimit | None = None,
) -> Generator[CompletedExecution]:
    """Stream a finite batch in completion order under a private scheduler.

    Input is consumed lazily. Exhaustion or explicit close drains admitted
    work; dropping an unclosed iterator does not guarantee prompt cleanup.
    A finite ``wall_time`` is an operation-wide ceiling: on expiry the
    scheduler cancels in-flight and remaining jobs as ``CancelledOutcome``.
    """

    scheduler: ExecutionScheduler[None] = ExecutionScheduler(
        executor=executor, capacity=capacity
    )
    source = iter(jobs)
    exhausted = False
    carried: ExecutionJob | None = None
    disarm = Event()
    if wall_time is not None:
        Thread(
            target=_watch_batch_deadline,
            args=(scheduler, time.monotonic_ns() + wall_time.limit, disarm),
            name="dr-exec-batch-deadline",
            daemon=True,
        ).start()
    try:
        while True:
            while not exhausted and scheduler.can_admit():
                job = carried if carried is not None else next(source, None)
                carried = None
                if job is None:
                    exhausted = True
                    break
                match scheduler.admit(job, None):
                    case AdmissionResult.ADMITTED:
                        pass
                    case AdmissionResult.INTAKE_CLOSED:
                        exhausted = True
                        break
                    case AdmissionResult.NO_ROOM:
                        carried = job
                        break
            if exhausted and carried is None and not scheduler.has_residents():
                return
            completion = scheduler.take_completion()
            if completion is None:
                return
            yield completion.completed_execution
    finally:
        disarm.set()
        scheduler.close_intake()
        scheduler.wait_for_quiescence()
        scheduler.shutdown()


def _watch_batch_deadline(
    scheduler: ExecutionScheduler[object],
    deadline_ns: int,
    disarm: Event,
    /,
) -> None:
    remaining = (deadline_ns - time.monotonic_ns()) / 1e9
    if remaining > 0 and disarm.wait(timeout=remaining):
        return
    if disarm.is_set():
        return
    scheduler.expire()


__all__ = [
    "AdmissionResult",
    "ExecutionScheduler",
    "SchedulerBroken",
    "run_batch",
]
