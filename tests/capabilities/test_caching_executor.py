from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from dr_store import MemoryBackend, ObjectStore, RecordCache
from support.executor import (
    empty_payload_outputs,
    job_for,
    run_thread_calls,
    runtime_identity_document,
    trusted_target,
)

from dr_exec import (
    AttemptId,
    BudgetAxis,
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
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
    JobId,
    RecordReceiptKind,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
)
from dr_exec.capabilities import CachedRecordReceipt, CachingExecutor
from dr_exec.capabilities.caching import CACHE_VALUE_SCHEMA, _cache_key

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
        runtime_identity=runtime_identity_document(),
        cache_budget_exceeded=cache_budget_exceeded,
    )


def test_a_hit_replays_the_stored_result_without_delegating() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)

    first = executor.run(job())
    second = executor.run(job())

    assert len(fake.calls) == 1
    assert second.result == first.result
    assert isinstance(second.record_receipt, CachedRecordReceipt)
    assert second.record_receipt.kind is RecordReceiptKind.CACHED
    assert second.record_receipt.cache_key == _cache_key(
        job(), runtime_identity=runtime_identity_document()
    )


def test_a_miss_delegates_once_and_stores_durably() -> None:
    cache = fresh_cache()
    first_fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    first = caching_over_fake(first_fake, cache=cache).run(job())
    assert isinstance(first.record_receipt, FakeRecordReceipt)
    assert len(first_fake.calls) == 1

    # A second wrapper sharing only the cache replays without running.
    second_fake = FakeExecutor()
    second = caching_over_fake(second_fake, cache=cache).run(job())

    assert second_fake.calls == ()
    assert second.result == first.result
    assert isinstance(second.record_receipt, CachedRecordReceipt)


def test_a_changed_declaration_misses() -> None:
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake)

    executor.run(job_for(trusted_target(("/usr/bin/true",))))
    executor.run(job_for(trusted_target(("/usr/bin/false",))))

    assert len(fake.calls) == 2


def test_a_changed_runtime_identity_misses() -> None:
    cache = fresh_cache()
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    CachingExecutor(
        fake,
        cache=cache,
        runtime_identity=runtime_identity_document("/opt/python/a"),
    ).run(job())
    other = CachingExecutor(
        fake,
        cache=cache,
        runtime_identity=runtime_identity_document("/opt/python/b"),
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


def test_a_corrupt_entry_reads_as_a_miss_and_stays_first_bound() -> None:
    cache = fresh_cache()
    key = _cache_key(job(), runtime_identity=runtime_identity_document())
    cache.put(key, CACHE_VALUE_SCHEMA, {"not": "an execution result"})
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake, cache=cache)

    first = executor.run(job())
    second = executor.run(job())

    # The corrupt binding keeps winning, so every call falls through.
    assert len(fake.calls) == 2
    assert isinstance(first.record_receipt, FakeRecordReceipt)
    assert isinstance(second.record_receipt, FakeRecordReceipt)


def test_a_schema_mismatched_entry_reads_as_a_miss() -> None:
    store = ObjectStore(MemoryBackend())
    cache = RecordCache(store)
    key = _cache_key(job(), runtime_identity=runtime_identity_document())
    reference, _ = store.put("dr_exec.test_other_schema.v1", {"other": 1})
    store.bind(key, reference)
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())

    completed = caching_over_fake(fake, cache=cache).run(job())

    assert len(fake.calls) == 1
    assert isinstance(completed.record_receipt, FakeRecordReceipt)


def test_racing_callers_each_get_a_completion_and_one_entry_wins() -> None:
    cache = fresh_cache()
    fake = FakeExecutor(responder=lambda _j, _c: exited_completion())
    executor = caching_over_fake(fake, cache=cache)

    outcome = run_thread_calls(
        [lambda: executor.run(job()) for _ in range(4)],
        timeout=WATCHDOG_SECONDS,
    )

    assert outcome.errors == ()
    assert outcome.unfinished == 0
    assert len(outcome.values) == 4
    for completed in outcome.values:
        assert isinstance(completed.result.outcome, ExitedOutcome)

    # After the race, one entry is bound and later calls replay it.
    replayed = executor.run(job())
    assert isinstance(replayed.record_receipt, CachedRecordReceipt)
    stored = {
        completed.result.execution_id
        for completed in outcome.values
        if isinstance(completed.record_receipt, FakeRecordReceipt)
    }
    assert replayed.result.execution_id in stored
