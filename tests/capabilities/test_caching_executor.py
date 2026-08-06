from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document
from dr_store import (
    CacheHit,
    MemoryBackend,
    ObjectStore,
    RecordCache,
    SqliteBackend,
    StoreError,
)
from support.executor import (
    cache_scope_identity_document,
    empty_payload_outputs,
    job_for,
    run_thread_calls,
    trusted_target,
)

from dr_exec import (
    AttemptId,
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    EnvGrant,
    ExecutionAttribution,
    ExecutionId,
    ExecutionJob,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExitedOutcome,
    FailureOwner,
    FakeExecutor,
    FakeRecordReceipt,
    FiniteDurationLimit,
    JobId,
    PayloadOutputs,
    RecordReceiptKind,
    RetainedPayloadStream,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
)
from dr_exec.capabilities import CachedRecordReceipt, CachingExecutor
from dr_exec.capabilities.caching import _cache_key, _CacheFormat

WATCHDOG_SECONDS = 30.0


def fresh_cache() -> RecordCache:
    return RecordCache(ObjectStore(MemoryBackend()))


def job() -> ExecutionJob:
    return job_for(trusted_target(("/usr/bin/true",)))


def completion_with(
    outcome: ExecutionOutcome,
    owner: FailureOwner,
    /,
) -> CompletedExecution:
    execution_id = ExecutionId(
        job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4())
    )
    moment = datetime.now(UTC)
    return CompletedExecution(
        result=ExecutionResult(
            execution_id=execution_id,
            outcome=outcome,
            attribution=ExecutionAttribution(owner=owner),
            protocol_outputs=(),
            payload_outputs=empty_payload_outputs(),
            measurements=ExecutionMeasurements(
                started_at=moment,
                finished_at=moment,
                duration_ns=0,
                teardown_duration_ns=0,
                input_bytes=0,
                protocol_bytes_received=0,
            ),
        ),
        record_receipt=FakeRecordReceipt(execution_id=execution_id),
    )


def exited_completion() -> CompletedExecution:
    return completion_with(ExitedOutcome(exit_code=0), FailureOwner.NONE)


def replay_evidence_completion() -> CompletedExecution:
    execution_id = ExecutionId(
        job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4())
    )
    moment = datetime.now(UTC)
    stdout_head = b"\xffhead"
    stdout_tail = b"tail\x00"
    return CompletedExecution(
        result=ExecutionResult(
            execution_id=execution_id,
            outcome=ExitedOutcome(exit_code=0),
            attribution=ExecutionAttribution(
                owner=FailureOwner.NONE,
                detail="observed detail",
            ),
            protocol_outputs=(
                build_identity_document(
                    schema="dr_exec.test_output",
                    schema_version=1,
                    payload={"value": [1, True, None]},
                ),
            ),
            payload_outputs=PayloadOutputs(
                stdout=RetainedPayloadStream(
                    head=stdout_head,
                    tail=stdout_tail,
                    produced_bytes=len(stdout_head) + len(stdout_tail) + 7,
                    dropped_bytes=7,
                ),
                stderr=RetainedPayloadStream(
                    head=b"error",
                    tail=b"",
                    produced_bytes=5,
                    dropped_bytes=0,
                ),
            ),
            measurements=ExecutionMeasurements(
                started_at=moment,
                finished_at=moment,
                duration_ns=12,
                teardown_duration_ns=3,
                input_bytes=5,
                protocol_bytes_received=41,
            ),
        ),
        record_receipt=FakeRecordReceipt(execution_id=execution_id),
    )


def caching_over_fake(
    fake: FakeExecutor,
    /,
    *,
    cache: RecordCache | None = None,
    cache_budget_exceeded: bool = False,
) -> CachingExecutor:
    return CachingExecutor(
        fake,
        cache=fresh_cache() if cache is None else cache,
        cache_scope_identity=cache_scope_identity_document(),
        cache_budget_exceeded=cache_budget_exceeded,
    )


def test_a_hit_replays_the_stored_result_without_delegating() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)
    first_job = job()
    second_job = job()

    first = executor.run(first_job)
    second = executor.run(second_job)

    assert len(fake.calls) == 1
    assert second.result == first.result
    assert isinstance(second.record_receipt, CachedRecordReceipt)
    assert second.record_receipt.kind is RecordReceiptKind.CACHED
    assert second.record_receipt.requested_job_id == second_job.job_id
    assert (
        second.record_receipt.source_execution_id == first.result.execution_id
    )
    assert second.record_receipt.cache_key == _cache_key(
        second_job, cache_scope_identity=cache_scope_identity_document()
    )


def test_an_already_cancelled_call_bypasses_a_warm_hit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    cache = fresh_cache()
    executor = caching_over_fake(fake, cache=cache)
    executor.run(job())
    monkeypatch.setattr(
        cache,
        "get",
        lambda *_args, **_kwargs: pytest.fail("cancelled call read cache"),
    )
    token = CancelToken()
    token.cancel()

    completed = executor.run(job(), cancellation=token)

    assert len(fake.calls) == 2
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


def test_cancellation_observed_during_a_cache_read_bypasses_the_hit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    cache = fresh_cache()
    executor = caching_over_fake(fake, cache=cache)
    executor.run(job())
    token = CancelToken()
    read = cache.get

    def cancel_after_read(key: str, /, *, schema: str) -> CacheHit | None:
        hit = read(key, schema=schema)
        token.cancel()
        return hit

    monkeypatch.setattr(cache, "get", cancel_after_read)

    completed = executor.run(job(), cancellation=token)

    assert len(fake.calls) == 2
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


@pytest.mark.integration
def test_a_reopened_cache_replays_the_complete_result(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.sqlite3"
    first_fake = FakeExecutor(
        responder=lambda _job, _cancellation: replay_evidence_completion()
    )
    first_cache = RecordCache(ObjectStore(SqliteBackend(cache_path)))
    first = caching_over_fake(first_fake, cache=first_cache).run(job())

    second_fake = FakeExecutor()
    reopened_cache = RecordCache(ObjectStore(SqliteBackend(cache_path)))
    second = caching_over_fake(second_fake, cache=reopened_cache).run(job())

    assert isinstance(first.record_receipt, FakeRecordReceipt)
    assert len(first_fake.calls) == 1
    assert second_fake.calls == ()
    assert second.result == first.result
    assert isinstance(second.record_receipt, CachedRecordReceipt)


def test_a_changed_declaration_misses() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)

    executor.run(job_for(trusted_target(("/usr/bin/true",))))
    executor.run(job_for(trusted_target(("/usr/bin/false",))))

    assert len(fake.calls) == 2


def test_a_changed_environment_value_misses() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)
    target = trusted_target(("/usr/bin/true",))

    executor.run(job_for(target, env=EnvGrant.fixed({"VALUE": "first"})))
    executor.run(job_for(target, env=EnvGrant.fixed({"VALUE": "second"})))

    assert len(fake.calls) == 2


def test_a_changed_workload_budget_misses() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)
    target = trusted_target(("/usr/bin/true",))

    executor.run(
        job_for(
            target,
            budgets=Budgets(
                wall_time=FiniteDurationLimit(max_ns=1_000_000_000)
            ),
        )
    )
    executor.run(
        job_for(
            target,
            budgets=Budgets(
                wall_time=FiniteDurationLimit(max_ns=2_000_000_000)
            ),
        )
    )

    assert len(fake.calls) == 2


def test_a_changed_cache_scope_identity_misses() -> None:
    cache = fresh_cache()
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    CachingExecutor(
        fake,
        cache=cache,
        cache_scope_identity=cache_scope_identity_document("scope-a"),
    ).run(job())
    other = CachingExecutor(
        fake,
        cache=cache,
        cache_scope_identity=cache_scope_identity_document("scope-b"),
    ).run(job())

    assert len(fake.calls) == 2
    assert isinstance(other.record_receipt, FakeRecordReceipt)


NEVER_STORED = [
    pytest.param(
        completion_with(
            SpawnAbsentOutcome(executable="/usr/bin/true"),
            FailureOwner.EXECUTOR,
        ),
        id="executor-attributed",
    ),
    pytest.param(
        completion_with(
            SpawnFailedOutcome(errno=11, error_message="fork failed"),
            FailureOwner.MACHINE,
        ),
        id="machine-attributed",
    ),
    pytest.param(
        completion_with(ExitedOutcome(exit_code=1), FailureOwner.EXECUTOR),
        id="exited-but-executor-attributed",
    ),
    pytest.param(
        completion_with(
            SignaledOutcome(signal_number=9), FailureOwner.PAYLOAD
        ),
        id="signaled",
    ),
    pytest.param(
        completion_with(CancelledOutcome(), FailureOwner.NONE),
        id="cancelled",
    ),
]


@pytest.mark.parametrize("scripted", NEVER_STORED)
def test_non_cacheable_completions_delegate_on_every_call(
    scripted: CompletedExecution,
) -> None:
    fake = FakeExecutor(responder=lambda _j, _c: scripted)
    executor = caching_over_fake(fake)

    executor.run(job())
    second = executor.run(job())

    assert len(fake.calls) == 2
    assert isinstance(second.record_receipt, FakeRecordReceipt)


def budget_exceeded_completion() -> CompletedExecution:
    return completion_with(
        BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME),
        FailureOwner.PAYLOAD,
    )


def test_budget_exceeded_is_not_stored_by_default() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: budget_exceeded_completion())
    executor = caching_over_fake(fake)

    executor.run(job())
    executor.run(job())

    assert len(fake.calls) == 2


def test_budget_exceeded_is_stored_behind_the_flag() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: budget_exceeded_completion())
    executor = caching_over_fake(fake, cache_budget_exceeded=True)

    executor.run(job())
    second = executor.run(job())

    assert len(fake.calls) == 1
    assert isinstance(second.record_receipt, CachedRecordReceipt)


def test_default_policy_does_not_replay_opted_in_budget_result() -> None:
    cache = fresh_cache()
    writer = FakeExecutor(
        responder=lambda _j, _c: budget_exceeded_completion()
    )
    caching_over_fake(
        writer,
        cache=cache,
        cache_budget_exceeded=True,
    ).run(job())
    reader = FakeExecutor(responder=lambda _j, _c: exited_completion())

    completed = caching_over_fake(reader, cache=cache).run(job())

    assert len(reader.calls) == 1
    assert isinstance(completed.result.outcome, ExitedOutcome)
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


def test_a_policy_ineligible_valid_entry_reads_as_a_miss() -> None:
    cache = fresh_cache()
    requested = job()
    key = _cache_key(
        requested,
        cache_scope_identity=cache_scope_identity_document(),
    )
    ineligible = completion_with(
        ExitedOutcome(exit_code=1), FailureOwner.MACHINE
    ).result
    cache.put(
        key,
        _CacheFormat.VALUE_SCHEMA,
        ineligible.model_dump(mode="json"),
    )
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())

    completed = caching_over_fake(fake, cache=cache).run(requested)

    assert len(fake.calls) == 1
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


def test_a_corrupt_entry_reads_as_a_miss_and_stays_first_bound() -> None:
    cache = fresh_cache()
    key = _cache_key(
        job(), cache_scope_identity=cache_scope_identity_document()
    )
    cache.put(
        key,
        _CacheFormat.VALUE_SCHEMA,
        {"not": "an execution result"},
    )
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake, cache=cache)

    first = executor.run(job())
    second = executor.run(job())

    assert len(fake.calls) == 2
    assert isinstance(first.record_receipt, FakeRecordReceipt)
    assert isinstance(second.record_receipt, FakeRecordReceipt)


def test_a_schema_mismatched_entry_reads_as_a_miss() -> None:
    store = ObjectStore(MemoryBackend())
    cache = RecordCache(store)
    key = _cache_key(
        job(), cache_scope_identity=cache_scope_identity_document()
    )
    reference, _ = store.put("dr_exec.test_other_schema.v1", {"other": 1})
    store.bind(key, reference)
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())

    completed = caching_over_fake(fake, cache=cache).run(job())

    assert len(fake.calls) == 1
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


def test_concurrent_misses_converge_on_one_cache_entry() -> None:
    caller_count = 4
    entered_inner = Barrier(caller_count)
    cache = fresh_cache()

    def complete_after_all_miss(
        _job: ExecutionJob,
        _cancellation: CancelToken | None,
    ) -> CompletedExecution:
        entered_inner.wait(WATCHDOG_SECONDS)
        return exited_completion()

    fake = FakeExecutor(responder=complete_after_all_miss)
    executor = caching_over_fake(fake, cache=cache)

    outcome = run_thread_calls(
        [lambda: executor.run(job()) for _ in range(caller_count)],
        timeout=WATCHDOG_SECONDS,
    )

    assert outcome.errors == ()
    assert outcome.unfinished == 0
    assert len(outcome.values) == caller_count
    assert len(fake.calls) == caller_count
    for completed in outcome.values:
        assert isinstance(completed.result.outcome, ExitedOutcome)
        assert isinstance(completed.record_receipt, FakeRecordReceipt)

    replayed = executor.run(job())
    assert isinstance(replayed.record_receipt, CachedRecordReceipt)
    stored = {
        completed.result.execution_id
        for completed in outcome.values
        if isinstance(completed.record_receipt, FakeRecordReceipt)
    }
    assert replayed.result.execution_id in stored


def test_a_cache_write_failure_does_not_replace_the_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = fresh_cache()

    def reject_write(*_args: object, **_kwargs: object) -> None:
        raise StoreError("injected cache write failure")

    monkeypatch.setattr(cache, "put", reject_write)
    fake = FakeExecutor(
        responder=lambda _job, _cancellation: exited_completion()
    )
    executor = caching_over_fake(fake, cache=cache)

    first = executor.run(job())
    second = executor.run(job())

    assert len(fake.calls) == 2
    assert isinstance(first.record_receipt, FakeRecordReceipt)
    assert isinstance(second.record_receipt, FakeRecordReceipt)


def test_a_cache_miss_forwards_the_callers_cancellation_token() -> None:
    seen: list[CancelToken | None] = []

    def capture_token(
        _job: ExecutionJob,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        seen.append(cancellation)
        return exited_completion()

    executor = caching_over_fake(FakeExecutor(responder=capture_token))
    token = CancelToken()

    executor.run(job(), cancellation=token)

    assert seen == [token]
