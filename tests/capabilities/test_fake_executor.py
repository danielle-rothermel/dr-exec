"""``FakeExecutor`` behavior that is the fake's own, not shared semantics.

The behaviors both executors must agree on -- validation parity, outcome
shapes, receipt kinds, cancellation -- live in the shared conformance suite
next door. What this file qualifies is what only the fake has: the two
mutually exclusive response sources, deterministic ordering and exhaustion
of the scripted queue, immutable call capture, mismatched receipt
rejection, and the absence of process, scratch, and record side effects.

Concurrency here synchronizes on barriers and events. No case treats
elapsed time as evidence that a call interleaved, and every blocking case
carries a watchdog so a deadlocked fake fails loudly instead of hanging the
suite.
"""

from __future__ import annotations

import os
import signal
import threading
from pathlib import Path
from uuid import uuid4

import pytest
from support.executor import (
    completion_for,
    fake_completion,
    job_for,
    real_receipted_completion,
    trusted_target,
)

from dr_exec import (
    CancelToken,
    CompletedExecution,
    CompleteRecordReceipt,
    DeclarationError,
    ExecutionJob,
    ExecutorFailure,
    FakeExecutor,
    JobId,
)

WATCHDOG_SECONDS = 30.0
CONCURRENT_CALLERS = 8


@pytest.fixture(autouse=True)
def watchdog() -> object:
    """Fail a hung case instead of letting it hang the whole suite."""
    timer = threading.Timer(
        WATCHDOG_SECONDS,
        lambda: os.kill(os.getpid(), signal.SIGALRM),
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: the case did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


def job() -> ExecutionJob:
    return job_for(trusted_target(("/usr/bin/true",)))


# --- One response source, chosen at construction -------------------------


def test_construction_rejects_both_response_sources() -> None:
    """Two sources would make response selection ambiguous per call."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        FakeExecutor(
            [fake_completion()],
            responder=lambda _job, _cancellation: fake_completion(),
        )


def test_a_responder_alone_and_a_queue_alone_both_construct() -> None:
    """Each source is complete on its own; neither implies the other."""
    assert FakeExecutor([fake_completion()]).calls == ()
    assert FakeExecutor(responder=lambda _j, _c: fake_completion()).calls == ()


def test_an_empty_queue_still_admits_a_responder() -> None:
    """The default empty iterable is absence, not a scripted queue."""
    executor = FakeExecutor((), responder=lambda _j, _c: fake_completion())

    assert executor.run(job()).record_receipt.kind is (
        fake_completion().record_receipt.kind
    )


# --- Deterministic ordering and exhaustion -------------------------------


def test_queued_responses_are_returned_in_declared_order() -> None:
    """The queue is in-order: call N gets response N, never a match."""
    ids = [JobId(uuid4()) for _ in range(3)]
    executor = FakeExecutor([completion_for(one) for one in ids])

    returned = [executor.run(job()).result.execution_id.job_id for _ in ids]

    assert returned == ids


def test_an_exhausted_queue_fails_rather_than_inventing_a_completion() -> None:
    """A completion the consumer never scripted would be a fabrication."""
    executor = FakeExecutor([fake_completion()])
    executor.run(job())

    with pytest.raises(ExecutorFailure, match="no scripted response left"):
        executor.run(job())


def test_exhaustion_still_records_the_call_that_exhausted_the_queue() -> None:
    """The job was accepted; only the response was missing."""
    executor = FakeExecutor()
    accepted = job()

    with pytest.raises(ExecutorFailure):
        executor.run(accepted)

    assert executor.calls == (accepted,)


# --- Immutable call capture ----------------------------------------------


def test_calls_captures_every_accepted_job_in_order() -> None:
    """Consumers assert on declarations, so capture must be exact."""
    executor = FakeExecutor([fake_completion() for _ in range(3)])
    jobs = [job() for _ in range(3)]

    for one in jobs:
        executor.run(one)

    assert executor.calls == tuple(jobs)


def test_calls_returns_a_snapshot_that_later_calls_do_not_mutate() -> None:
    """A held snapshot is evidence about a moment, not a live view."""
    executor = FakeExecutor([fake_completion() for _ in range(2)])
    executor.run(job())
    snapshot = executor.calls

    executor.run(job())

    assert len(snapshot) == 1
    assert len(executor.calls) == 2


def test_a_rejected_declaration_is_not_captured_as_a_call() -> None:
    """Production leaves nothing behind a pre-spawn refusal either."""
    executor = FakeExecutor([fake_completion()])
    invalid = job_for(trusted_target(("dr-exec-test-relative",)))

    with pytest.raises(DeclarationError):
        executor.run(invalid)

    assert executor.calls == ()


# --- Mismatched receipt rejection ----------------------------------------


@pytest.mark.parametrize("source", ["queue", "responder"])
def test_a_production_receipt_is_refused_from_either_source(
    source: str,
) -> None:
    """A fake call recorded nothing, so it may not claim a record.

    This is what keeps `NOT_APPLICABLE` meaning "no record was ever
    attempted" rather than decaying into a production no-record option.
    """
    real = real_receipted_completion()
    executor = (
        FakeExecutor([real])
        if source == "queue"
        else FakeExecutor(responder=lambda _j, _c: real)
    )

    with pytest.raises(ExecutorFailure, match="fake record receipt"):
        executor.run(job())


def test_a_refused_receipt_does_not_silently_become_a_fake_one() -> None:
    """Rejection is refusal, not repair: the caller scripted a mistake."""
    real = real_receipted_completion()
    executor = FakeExecutor([real])

    with pytest.raises(ExecutorFailure):
        executor.run(job())

    assert isinstance(real.record_receipt, CompleteRecordReceipt)


# --- Responder access to the call's cancellation token -------------------


def test_the_responder_receives_the_calls_own_cancellation_token() -> None:
    """Consumers script cancellation-dependent behavior through it."""
    seen: list[CancelToken | None] = []

    def responder(
        _job: ExecutionJob, cancellation: CancelToken | None
    ) -> CompletedExecution:
        seen.append(cancellation)
        return fake_completion()

    executor = FakeExecutor(responder=responder)
    token = CancelToken()
    executor.run(job(), cancellation=token)
    executor.run(job())

    assert seen == [token, None]


def test_the_responder_sees_cancellation_observed_during_its_own_call() -> (
    None
):
    """The token is live, not a snapshot taken before the call."""
    released = threading.Event()
    entered = threading.Event()
    observed: list[bool] = []

    def responder(
        _job: ExecutionJob, cancellation: CancelToken | None
    ) -> CompletedExecution:
        entered.set()
        assert released.wait(WATCHDOG_SECONDS)
        assert cancellation is not None
        observed.append(cancellation.cancelled)
        return fake_completion()

    executor = FakeExecutor(responder=responder)
    token = CancelToken()
    caller = threading.Thread(
        target=lambda: executor.run(job(), cancellation=token)
    )
    caller.start()

    assert entered.wait(WATCHDOG_SECONDS)
    token.cancel()
    released.set()
    caller.join(WATCHDOG_SECONDS)

    assert not caller.is_alive()
    assert observed == [True]


def test_the_responder_sees_each_calls_own_declaration() -> None:
    """Response selection is declaration-dependent, per call."""
    executor = FakeExecutor(
        responder=lambda one, _c: completion_for(one.job_id)
    )
    first, second = job(), job()

    assert executor.run(first).result.execution_id.job_id == first.job_id
    assert executor.run(second).result.execution_id.job_id == second.job_id


# --- Concurrent call isolation -------------------------------------------


def test_concurrent_calls_take_distinct_queued_responses() -> None:
    """No response is delivered twice and none is lost.

    Every caller is held at one barrier so the pops genuinely contend,
    then released together; the assertion is on the exact set of delivered
    responses, never on how long anything took.
    """
    ids = [JobId(uuid4()) for _ in range(CONCURRENT_CALLERS)]
    executor = FakeExecutor([completion_for(one) for one in ids])
    barrier = threading.Barrier(CONCURRENT_CALLERS)
    delivered: list[JobId] = []
    guard = threading.Lock()

    def call() -> None:
        barrier.wait(WATCHDOG_SECONDS)
        completed = executor.run(job())
        with guard:
            delivered.append(completed.result.execution_id.job_id)

    threads = [
        threading.Thread(target=call) for _ in range(CONCURRENT_CALLERS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WATCHDOG_SECONDS)

    assert not any(thread.is_alive() for thread in threads)
    assert sorted(delivered, key=str) == sorted(ids, key=str)


def test_concurrent_calls_capture_every_job_exactly_once() -> None:
    """Contended appends lose nothing and duplicate nothing."""
    executor = FakeExecutor(
        responder=lambda _j, _c: fake_completion(),
    )
    barrier = threading.Barrier(CONCURRENT_CALLERS)
    jobs = [job() for _ in range(CONCURRENT_CALLERS)]

    def call(one: ExecutionJob) -> None:
        barrier.wait(WATCHDOG_SECONDS)
        executor.run(one)

    threads = [threading.Thread(target=call, args=(one,)) for one in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WATCHDOG_SECONDS)

    assert not any(thread.is_alive() for thread in threads)
    assert sorted(executor.calls, key=lambda one: str(one.job_id)) == sorted(
        jobs, key=lambda one: str(one.job_id)
    )


def test_one_responder_call_does_not_block_another() -> None:
    """The responder runs outside the lock, so calls truly overlap.

    Each responder waits for the *other* call to have entered before
    returning. That is only satisfiable if both are inside the executor at
    once, so completion of the pair is the evidence -- a serializing fake
    deadlocks here and the watchdog reports it.
    """
    entered = threading.Barrier(2)

    def responder(
        _job: ExecutionJob, _cancellation: CancelToken | None
    ) -> CompletedExecution:
        entered.wait(WATCHDOG_SECONDS)
        return fake_completion()

    executor = FakeExecutor(responder=responder)
    threads = [
        threading.Thread(target=lambda: executor.run(job())) for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WATCHDOG_SECONDS)

    assert not any(thread.is_alive() for thread in threads)
    assert len(executor.calls) == 2


# --- No process, scratch, or record side effects -------------------------


def test_a_fake_call_creates_no_child_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fake executes no payload, so nothing may reach a spawn."""
    import subprocess

    def refuse(*_args: object, **_kwargs: object) -> object:
        pytest.fail("the fake executor must not spawn a child")

    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(os, "posix_spawn", refuse)
    monkeypatch.setattr(os, "fork", refuse)

    FakeExecutor([fake_completion()]).run(job())


def test_a_fake_call_creates_no_scratch_or_record_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No scratch workspace, no record directory, no durable trace.

    Both the default temporary root and the working directory are watched,
    so a scratch workspace created under either is visible as a new entry.
    """
    watched = tmp_path / "watched"
    watched.mkdir()
    monkeypatch.setenv("TMPDIR", str(watched))
    monkeypatch.chdir(watched)

    FakeExecutor([fake_completion()]).run(job())

    assert list(watched.iterdir()) == []
