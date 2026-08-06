from __future__ import annotations

import os
import signal
import threading
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from support.executor import (
    completion_for,
    fake_completion,
    job_for,
    real_receipted_completion,
    run_thread_calls,
    trusted_target,
)

from dr_exec import (
    CancelToken,
    CompletedExecution,
    DeclarationError,
    ExecutionJob,
    ExecutorFailure,
    FakeExecutor,
    JobId,
)

WATCHDOG_SECONDS = 30.0
CONCURRENT_CALLERS = 8


@pytest.fixture
def watchdog() -> object:
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


def test_construction_rejects_both_response_sources() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        FakeExecutor(
            [fake_completion()],
            responder=lambda _job, _cancellation: fake_completion(),
        )


def test_a_responder_alone_and_a_queue_alone_both_construct() -> None:
    assert FakeExecutor([fake_completion()]).calls == ()
    assert FakeExecutor(responder=lambda _j, _c: fake_completion()).calls == ()


def test_an_empty_queue_still_admits_a_responder() -> None:
    executor = FakeExecutor((), responder=lambda _j, _c: fake_completion())

    assert executor.run(job()).record_receipt.kind is (
        fake_completion().record_receipt.kind
    )


def test_queued_responses_are_returned_in_declared_order() -> None:
    ids = [JobId(uuid4()) for _ in range(3)]
    executor = FakeExecutor([completion_for(one) for one in ids])

    returned = [executor.run(job()).result.execution_id.job_id for _ in ids]

    assert returned == ids


@pytest.mark.parametrize("source", ["queue", "responder"])
def test_completion_identity_is_scripted_by_the_caller(source: str) -> None:
    accepted = job()
    scripted = completion_for(
        JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))
    )
    executor = (
        FakeExecutor([scripted])
        if source == "queue"
        else FakeExecutor(responder=lambda _job, _cancellation: scripted)
    )

    returned = executor.run(accepted)

    assert returned is scripted
    assert returned.result.execution_id.job_id != accepted.job_id


@pytest.mark.parametrize("source", ["queue", "responder"])
def test_cancellation_outcome_is_scripted_by_the_caller(source: str) -> None:
    scripted = fake_completion()
    executor = (
        FakeExecutor([scripted])
        if source == "queue"
        else FakeExecutor(responder=lambda _job, _cancellation: scripted)
    )
    token = CancelToken()
    token.cancel()

    assert executor.run(job(), cancellation=token) is scripted


@pytest.mark.parametrize("source", ["queue", "responder"])
def test_attempt_identity_is_scripted_by_the_caller(source: str) -> None:
    scripted = fake_completion()
    executor = (
        FakeExecutor([scripted, scripted])
        if source == "queue"
        else FakeExecutor(responder=lambda _job, _cancellation: scripted)
    )

    first = executor.run(job())
    second = executor.run(job())

    assert first.result.execution_id == second.result.execution_id


def test_an_exhausted_queue_fails_rather_than_inventing_a_completion() -> None:
    executor = FakeExecutor([fake_completion()])
    executor.run(job())

    with pytest.raises(ExecutorFailure, match="no scripted response left"):
        executor.run(job())


def test_exhaustion_still_records_the_call_that_exhausted_the_queue() -> None:
    executor = FakeExecutor()
    accepted = job()

    with pytest.raises(ExecutorFailure):
        executor.run(accepted)

    assert executor.calls == (accepted,)


def test_calls_captures_every_accepted_job_in_order() -> None:
    executor = FakeExecutor([fake_completion() for _ in range(3)])
    jobs = [job() for _ in range(3)]

    for one in jobs:
        executor.run(one)

    assert executor.calls == tuple(jobs)


def test_calls_returns_a_snapshot_that_later_calls_do_not_mutate() -> None:
    executor = FakeExecutor([fake_completion() for _ in range(2)])
    executor.run(job())
    snapshot = executor.calls

    executor.run(job())

    assert len(snapshot) == 1
    assert len(executor.calls) == 2


def test_a_rejected_declaration_is_not_captured_as_a_call() -> None:
    executor = FakeExecutor([fake_completion()])
    invalid = job_for(trusted_target(("dr-exec-test-relative",)))

    with pytest.raises(DeclarationError):
        executor.run(invalid)

    assert executor.calls == ()


@pytest.mark.parametrize("source", ["queue", "responder"])
def test_a_production_receipt_is_refused_from_either_source(
    source: str,
) -> None:
    real = real_receipted_completion()
    executor = (
        FakeExecutor([real])
        if source == "queue"
        else FakeExecutor(responder=lambda _j, _c: real)
    )

    with pytest.raises(ExecutorFailure, match="fake record receipt"):
        executor.run(job())


def test_the_responder_receives_the_calls_own_cancellation_token() -> None:
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


@pytest.mark.usefixtures("watchdog")
def test_the_responder_sees_cancellation_observed_during_its_own_call() -> (
    None
):
    released = threading.Event()
    entered = threading.Event()
    observed: list[bool] = []
    completed: list[CompletedExecution] = []
    errors: list[Exception] = []

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

    def call() -> None:
        try:
            completed.append(executor.run(job(), cancellation=token))
        except Exception as error:  # noqa: BLE001 - retain worker failure
            errors.append(error)

    caller = threading.Thread(target=call)
    caller.start()

    assert entered.wait(WATCHDOG_SECONDS)
    token.cancel()
    released.set()
    caller.join(WATCHDOG_SECONDS)

    assert not caller.is_alive()
    assert errors == []
    assert len(completed) == 1
    assert observed == [True]


def test_the_responder_sees_each_calls_own_declaration() -> None:
    executor = FakeExecutor(
        responder=lambda one, _c: completion_for(one.job_id)
    )
    first, second = job(), job()

    assert executor.run(first).result.execution_id.job_id == first.job_id
    assert executor.run(second).result.execution_id.job_id == second.job_id


@pytest.mark.usefixtures("watchdog")
def test_concurrent_calls_take_distinct_queued_responses() -> None:
    ids = [JobId(uuid4()) for _ in range(CONCURRENT_CALLERS)]
    executor = FakeExecutor([completion_for(one) for one in ids])
    barrier = threading.Barrier(CONCURRENT_CALLERS)

    def call() -> JobId:
        barrier.wait(WATCHDOG_SECONDS)
        return executor.run(job()).result.execution_id.job_id

    run = run_thread_calls(
        (call for _ in range(CONCURRENT_CALLERS)),
        timeout=WATCHDOG_SECONDS,
    )

    assert run.unfinished == 0
    assert run.errors == ()
    assert sorted(run.values, key=str) == sorted(ids, key=str)


@pytest.mark.usefixtures("watchdog")
def test_concurrent_calls_capture_every_job_exactly_once() -> None:
    executor = FakeExecutor(
        responder=lambda _j, _c: fake_completion(),
    )
    barrier = threading.Barrier(CONCURRENT_CALLERS)
    jobs = [job() for _ in range(CONCURRENT_CALLERS)]

    def call(one: ExecutionJob) -> CompletedExecution:
        barrier.wait(WATCHDOG_SECONDS)
        return executor.run(one)

    run = run_thread_calls(
        (lambda one=one: call(one) for one in jobs),
        timeout=WATCHDOG_SECONDS,
    )

    assert run.unfinished == 0
    assert run.errors == ()
    assert len(run.values) == CONCURRENT_CALLERS
    assert sorted(executor.calls, key=lambda one: str(one.job_id)) == sorted(
        jobs, key=lambda one: str(one.job_id)
    )


@pytest.mark.usefixtures("watchdog")
def test_one_responder_call_does_not_block_another() -> None:
    entered = threading.Barrier(2)

    def responder(
        _job: ExecutionJob, _cancellation: CancelToken | None
    ) -> CompletedExecution:
        entered.wait(WATCHDOG_SECONDS)
        return fake_completion()

    executor = FakeExecutor(responder=responder)
    run = run_thread_calls(
        (lambda: executor.run(job()) for _ in range(2)),
        timeout=WATCHDOG_SECONDS,
    )

    assert run.unfinished == 0
    assert run.errors == ()
    assert len(run.values) == 2
    assert len(executor.calls) == 2


def test_a_fake_call_creates_no_child_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    watched = tmp_path / "watched"
    watched.mkdir()
    monkeypatch.setenv("TMPDIR", str(watched))
    monkeypatch.chdir(watched)

    FakeExecutor([fake_completion()]).run(job())

    assert list(watched.iterdir()) == []
