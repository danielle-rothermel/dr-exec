from __future__ import annotations

import threading
from collections.abc import Iterable

from dr_exec import (
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    ExecutionJob,
    JobId,
)
from support.executor import completion_for

WATCHDOG_SECONDS = 30.0


def wait_for(event: threading.Event, /, *, what: str) -> None:
    if not event.wait(WATCHDOG_SECONDS):
        raise AssertionError(f"watchdog fired waiting for {what}")


class GatedResponder:
    def __init__(self, *, cancellation_aware: bool = False) -> None:
        self._cancellation_aware = cancellation_aware
        self._lock = threading.Lock()
        self._arrived: dict[JobId, threading.Event] = {}
        self._release: dict[JobId, threading.Event] = {}
        self._executor_returned: dict[JobId, threading.Event] = {}
        self._arrival_order: list[JobId] = []
        self._cancelled: list[JobId] = []
        self._active = 0
        self._peak_active = 0
        self._watchers: list[threading.Thread] = []
        self.arrivals = threading.Semaphore(0)

    def __call__(
        self, job: ExecutionJob, cancellation: CancelToken | None, /
    ) -> CompletedExecution:
        self._announce(job.job_id)
        try:
            if not self._hold(job.job_id, cancellation):
                return self._cancelled_completion(job.job_id)
            return completion_for(job.job_id)
        finally:
            with self._lock:
                self._active -= 1
            self.executor_returned_gate(job.job_id).set()

    def _hold(
        self, job_id: JobId, cancellation: CancelToken | None, /
    ) -> bool:
        gate = self.release_gate(job_id)
        watcher: threading.Thread | None = None
        if self._cancellation_aware and cancellation is not None:
            if cancellation.cancelled:
                return False
            watcher = _watch_token_into(cancellation, gate)
            with self._lock:
                self._watchers.append(watcher)
        wait_for(gate, what=f"job {job_id} to be released or cancelled")
        if watcher is not None:
            watcher.join(WATCHDOG_SECONDS)
            if watcher.is_alive():
                raise AssertionError(
                    f"watchdog fired joining cancellation watcher for {job_id}"
                )
        return cancellation is None or not cancellation.cancelled

    def _announce(self, job_id: JobId, /) -> None:
        with self._lock:
            self._arrival_order.append(job_id)
            self._active += 1
            self._peak_active = max(self._peak_active, self._active)
        self.arrived_gate(job_id).set()
        self.arrivals.release()

    def _cancelled_completion(self, job_id: JobId, /) -> CompletedExecution:
        with self._lock:
            self._cancelled.append(job_id)
        completed = completion_for(job_id)
        return CompletedExecution(
            result=completed.result.model_copy(
                update={"outcome": CancelledOutcome()}
            ),
            record_receipt=completed.record_receipt,
        )

    def arrived_gate(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._arrived.setdefault(job_id, threading.Event())

    def release_gate(self, job_id: JobId, /) -> threading.Event:
        with self._lock:
            return self._release.setdefault(job_id, threading.Event())

    def executor_returned_gate(self, job_id: JobId, /) -> threading.Event:
        """Return the responder-return gate, not a scheduler-publication gate.

        Tests needing a buffered completion must separately wait for scheduler
        readiness.
        """
        with self._lock:
            return self._executor_returned.setdefault(
                job_id, threading.Event()
            )

    def await_executor_returned(self, job_id: JobId, /) -> None:
        wait_for(
            self.executor_returned_gate(job_id),
            what=f"job {job_id} to return from its executor responder",
        )

    def await_arrival(self, job_id: JobId, /) -> None:
        wait_for(self.arrived_gate(job_id), what=f"job {job_id} to start")

    def await_arrival_count(self, count: int, /) -> None:
        for _ in range(count):
            if not self.arrivals.acquire(timeout=WATCHDOG_SECONDS):
                raise AssertionError(
                    f"watchdog fired waiting for {count} calls to start"
                )

    def release(self, *job_ids: JobId) -> None:
        for job_id in job_ids:
            self.release_gate(job_id).set()

    def release_all(self, jobs: Iterable[ExecutionJob], /) -> None:
        for job in jobs:
            self.release_gate(job.job_id).set()

    @property
    def started(self) -> tuple[JobId, ...]:
        with self._lock:
            return tuple(self._arrival_order)

    @property
    def cancelled(self) -> tuple[JobId, ...]:
        with self._lock:
            return tuple(self._cancelled)

    @property
    def peak_active(self) -> int:
        with self._lock:
            return self._peak_active

    def assert_no_watchers(self) -> None:
        with self._lock:
            alive = [
                watcher.name
                for watcher in self._watchers
                if watcher.is_alive()
            ]
        assert not alive, f"live cancellation watchers remain: {alive}"


def _watch_token_into(
    token: CancelToken, gate: threading.Event, /
) -> threading.Thread:

    def watch() -> None:
        if token._wait(WATCHDOG_SECONDS):
            gate.set()

    watcher = threading.Thread(
        target=watch,
        name="gated-responder-cancellation-watcher",
        daemon=True,
    )
    watcher.start()
    return watcher
