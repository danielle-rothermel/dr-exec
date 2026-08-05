"""Deterministic gates for qualifying the scheduler without real work.

The pool's promises are all about *when* things happen relative to each
other: whether intake advanced, whether a slot was released, whether a
result was buffered rather than delivered. None of that can be established
by running fast jobs and looking at the clock -- a passing timing test only
says the machine was fast enough this once.

So every case here drives a `FakeExecutor` whose responder blocks inside
the call until the test releases it. That turns "a job is in flight" into
an event the test sets. Tests that qualify scheduler publication pair these
responder-side gates with the scheduler's exact ready state; executor return
and scheduler publication are deliberately not conflated.

Two rules hold throughout, and the helpers exist to make them easy:

- no sleep, no elapsed-time assertion, and no "it did not happen within N
  seconds" as positive evidence of ordering. Where a case must show that
  something did *not* happen, it first waits for a state that would have
  had to follow it, then asserts the absence -- a state gate, not a delay.
- every blocking wait carries a watchdog timeout and fails loudly rather
  than hanging the suite.
"""

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

# The bound on every blocking wait in the pool suites. It is a watchdog:
# reaching it means a case is wedged, never that a behavior "took too
# long". No assertion is ever made about how much of it elapsed.
WATCHDOG_SECONDS = 30.0


def wait_for(event: threading.Event, /, *, what: str) -> None:
    """Block on one event, failing the case rather than hanging on it."""
    if not event.wait(WATCHDOG_SECONDS):
        raise AssertionError(f"watchdog fired waiting for {what}")


class GatedResponder:
    """A responder that holds each call until the test releases that job.

    Every call announces its arrival on a per-job `arrived` event and then
    blocks on a per-job `release` gate. A test therefore controls the
    scheduler's timeline exactly: it knows which jobs are in flight
    because they arrived, and it decides which finishes first by choosing
    which gate to open.

    Cancellation-aware instances honor cancellation the way production
    does -- a call whose token is already cancelled returns a cancelled
    outcome without "running" -- so abort cases observe the same outcome
    data a real cancelled call would produce. Release-only instances start
    no cancellation watcher.
    """

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

    # --- Responder -------------------------------------------------------

    def __call__(
        self, job: ExecutionJob, cancellation: CancelToken | None, /
    ) -> CompletedExecution:
        """Announce, hold until released or cancelled, then complete.

        Cancellation releases the hold, which is what a real executor call
        does: a cancelled call performs its teardown and returns rather
        than running to completion. Without that, an aborting pool would
        be waiting on a gate only the test could open, and the test would
        be waiting on the abort -- a deadlock the watchdog would report as
        a scheduler bug that is not there.
        """
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
        """Wait for release or cancellation; False means cancelled.

        Cancellation-aware instances use one watcher that blocks on the
        token's own event and opens the release gate when it fires. Abort
        is the intended release for those instances, and the watcher is
        joined before this call returns. Ordinary gate-driven cases start
        no watcher. Neither path polls or uses an interval.

        `CancelToken` exposes exactly this blocking wait for the engine's
        use, and the fake substrate needs the same primitive to behave
        like a call that observes cancellation promptly.
        """
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

    # --- Test-side control ----------------------------------------------

    def arrived_gate(self, job_id: JobId, /) -> threading.Event:
        """Set when this job's call has begun."""
        with self._lock:
            return self._arrived.setdefault(job_id, threading.Event())

    def release_gate(self, job_id: JobId, /) -> threading.Event:
        """Set to let this job's call return."""
        with self._lock:
            return self._release.setdefault(job_id, threading.Event())

    def executor_returned_gate(self, job_id: JobId, /) -> threading.Event:
        """Set when this job's executor responder has returned.

        Scheduler publication is a later transition. Tests that need a
        buffered completion must separately wait for the scheduler's ready
        queue rather than treating this responder-side gate as publication.
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
        """Block until this job's call is inside the executor."""
        wait_for(self.arrived_gate(job_id), what=f"job {job_id} to start")

    def await_arrival_count(self, count: int, /) -> None:
        """Block until `count` calls have started, in total.

        The semaphore is what makes "the scheduler admitted N jobs" a
        state a test can wait for without knowing which jobs those were.
        """
        for _ in range(count):
            if not self.arrivals.acquire(timeout=WATCHDOG_SECONDS):
                raise AssertionError(
                    f"watchdog fired waiting for {count} calls to start"
                )

    def release(self, *job_ids: JobId) -> None:
        """Let the named calls return, in the order given."""
        for job_id in job_ids:
            self.release_gate(job_id).set()

    def release_all(self, jobs: Iterable[ExecutionJob], /) -> None:
        for job in jobs:
            self.release_gate(job.job_id).set()

    @property
    def started(self) -> tuple[JobId, ...]:
        """Every job whose call has begun, in the order calls began."""
        with self._lock:
            return tuple(self._arrival_order)

    @property
    def cancelled(self) -> tuple[JobId, ...]:
        """Every job whose call observed a cancelled token."""
        with self._lock:
            return tuple(self._cancelled)

    @property
    def peak_active(self) -> int:
        """Largest number of responder calls held concurrently."""
        with self._lock:
            return self._peak_active

    def assert_no_watchers(self) -> None:
        """Assert that every cancellation watcher this responder made exited."""
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
    """Open `gate` as soon as `token` is cancelled, on its own thread.

    This is what lets one `Event.wait` stand for "released or cancelled".
    Cancellation-aware responders use this only in cases where cancellation
    is the intended release. The responder joins the returned thread before
    the executor call returns; ordinary release-only cases start no watcher.
    """

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
