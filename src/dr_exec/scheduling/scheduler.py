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

_WORKER_THREAD_PREFIX = "dr-exec-pool-worker"


def usable_cpu_count() -> int:
    reported = getattr(os, "process_cpu_count", os.cpu_count)()
    return max(1, reported or 1)


@verify(UNIQUE)
class _AdmissionResult(Enum):
    ADMITTED = auto()

    INTAKE_CLOSED = auto()

    NO_ROOM = auto()


@dataclass(frozen=True, slots=True)
class _Admitted(Generic[ContextT]):  # noqa: UP046
    ticket: int
    job: ExecutionJob
    context: ContextT
    cancellation: CancelToken


@dataclass(frozen=True, slots=True)
class _Completion(Generic[ContextT]):  # noqa: UP046
    ticket: int
    completed_execution: CompletedExecution
    context: ContextT


class SchedulerBroken(ExecutorFailure):
    """Scheduler machinery failed, ending trustworthy delivery.

    Buffered completions drain first; queued admitted work is dropped and
    in-flight results arriving after delivery ends are not delivered.
    """


class _ExecutionScheduler(Generic[ContextT]):  # noqa: UP046
    """Share one resident bound and completion queue across drivers."""

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

    def can_admit(self) -> bool:
        with self._condition:
            return self._residents < self._capacity and not self._intake_closed

    def admit(
        self, job: ExecutionJob, context: ContextT, /
    ) -> _AdmissionResult:
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
            # A failed worker start drops this queued submission; buffered
            # completions still drain before the break surfaces.
            if self._broken is not None:
                return _AdmissionResult.INTAKE_CLOSED
            return _AdmissionResult.ADMITTED

    def close_intake(self) -> None:
        with self._condition:
            self._intake_closed = True
            self._condition.notify_all()

    def has_residents(self) -> bool:
        with self._condition:
            return self._residents > 0

    def is_broken(self) -> bool:
        with self._condition:
            return self._broken is not None

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
            completion = self._ready.popleft()
            self._tokens.pop(completion.ticket, None)
            self._residents -= 1
            self._condition.notify_all()
            return completion

    def cancel_all(self) -> None:
        with self._condition:
            for token in tuple(self._tokens.values()):
                token.cancel()
            self._condition.notify_all()

    def wait_for_quiescence(self) -> None:
        with self._condition:
            self._condition.wait_for(
                lambda: not self._running and not self._pending
            )

    def shutdown(self) -> None:
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


__all__ = [
    "SchedulerBroken",
    "_Admitted",
    "_Completion",
    "_ExecutionScheduler",
    "usable_cpu_count",
]
