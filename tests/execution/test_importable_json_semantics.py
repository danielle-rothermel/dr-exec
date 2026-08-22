"""Semantic conformance shared by both trusted importable-JSON executors.

The shared `Executor` fixture in `tests/capabilities` is parameterized over
command and process-Python declarations, which an importable-JSON executor
cannot accept. This suite is the semantic conformance baseline in its place:
every case here must hold for the in-process executor and for the worker pool.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from dr_serialize import Jsonable
from support.executor import job_for, trusted_python_target
from support.importable_json import (
    ECHO,
    ECHO_OR_BLOCK,
    ECHO_OR_RAISE,
    HARNESSES,
    IMPORT_FAIL,
    MISSING_MODULE,
    NOT_CALLABLE,
    RAISE_SYSTEM_EXIT,
    RAISES,
    RAISES_HOSTILE_ENCODE_MESSAGE,
    RAISES_HOSTILE_QUALNAME,
    RAISES_HOSTILE_SIZING_MESSAGE,
    RAISES_HUGE_MESSAGE,
    RAISES_LONE_SURROGATE,
    RAISES_SENTINEL,
    RAISES_SURROGATE_ESCAPE,
    RAISES_UNPRINTABLE,
    RETURN_NON_JSON,
    RETURN_NULL,
    ExecutorHarness,
    PooledExecutor,
)
from support.pool import WATCHDOG_SECONDS
from support.process import Gate

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    DeclarationError,
    EnvGrant,
    ExecutionJob,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExecutorFailure,
    ExitedOutcome,
    FailureOwner,
    FiniteByteLimit,
    FiniteDurationLimit,
    FiniteOutput,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonExecutor,
    InProcessRecordReceipt,
    JobId,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    ProtocolFailedOutcome,
    StreamRetentionBudget,
    WorkerPoolImportableJsonExecutor,
    WorkerPoolRecordReceipt,
    build_in_process_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.importable_json import (
    PAYLOAD_ERROR_DETAIL_MAX_BYTES,
    PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER,
)

JOB_ID = JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))


@pytest.fixture(params=HARNESSES, ids=lambda harness: harness.name)
def harness(request: pytest.FixtureRequest) -> ExecutorHarness:
    return cast("ExecutorHarness", request.param)


def build_job(
    entry_point: ImportableEntryPoint = ECHO,
    request: Jsonable | None = None,
    *,
    job_id: JobId = JOB_ID,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    payload: Jsonable = {"question": 42} if request is None else request
    return build_in_process_importable_json_job(
        job_id,
        entry_point,
        payload,
        budgets=budgets,
    )


def run_one(
    harness: ExecutorHarness,
    entry_point: ImportableEntryPoint,
    /,
    *,
    request: Jsonable | None = None,
    budgets: Budgets | None = None,
    cancellation: CancelToken | None = None,
) -> CompletedExecution:
    with harness.open(entry_point, workers=1) as executor:
        return executor.run_blocking(
            build_job(entry_point, request, budgets=budgets),
            cancellation=cancellation,
        )


def test_echo_round_trips_through_parse_importable_json_result(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, ECHO, request={"question": 42})

    assert parse_importable_json_result(completed) == {
        "value": {"question": 42}
    }


def test_each_executor_stamps_its_own_receipt_kind(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, ECHO)

    assert harness.receipt_is_own(completed.record_receipt)
    assert (
        completed.record_receipt.execution_id == completed.result.execution_id
    )


def test_the_two_executors_stamp_the_two_record_less_receipts() -> None:
    # The shared completion builder takes the receipt already constructed, so
    # which executor built a completion stays readable from its receipt.
    in_process = ImportableJsonExecutor().run_blocking(build_job(ECHO, {}))
    with WorkerPoolImportableJsonExecutor(
        entry_point=ECHO, worker_count=1
    ) as pool:
        pooled = pool.run_blocking(build_job(ECHO, {}))

    assert isinstance(in_process.record_receipt, InProcessRecordReceipt)
    assert isinstance(pooled.record_receipt, WorkerPoolRecordReceipt)


def test_json_null_result_is_accepted(harness: ExecutorHarness) -> None:
    completed = run_one(harness, RETURN_NULL, request={"ignored": True})

    assert parse_importable_json_result(completed) is None


def test_import_error_maps_to_protocol_failure_with_executor_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, MISSING_MODULE)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_not_callable_maps_to_protocol_failure_with_executor_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, NOT_CALLABLE)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_import_initialization_failure_maps_to_protocol_failure(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, IMPORT_FAIL)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR


def test_entry_point_exception_maps_to_nonzero_exit_with_payload_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, RAISES)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_a_payload_raise_carries_its_exception_type_message_and_traceback(
    harness: ExecutorHarness,
) -> None:
    # The whole point of the detail: a caller reading only the completion can
    # tell what went wrong without the child's stderr.
    completed = run_one(harness, RAISES_SENTINEL)

    detail = completed.result.attribution.detail
    assert detail is not None
    assert "ValueError" in detail
    assert "SENTINEL-12345" in detail
    assert "raise_sentinel_value_error" in detail
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_a_giant_payload_exception_message_is_truncated_to_the_cap(
    harness: ExecutorHarness,
) -> None:
    # A payload controls its own message, so the cap is what keeps a worker's
    # result frame from growing without bound. The completion still parses.
    completed = run_one(harness, RAISES_HUGE_MESSAGE)

    detail = completed.result.attribution.detail
    assert detail is not None
    assert len(detail.encode("utf-8")) <= PAYLOAD_ERROR_DETAIL_MAX_BYTES
    assert detail.endswith(PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER)
    # Truncation keeps the head, so the identifying part survives the cut.
    assert "ValueError" in detail
    assert "SENTINEL-12345" in detail
    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_an_unprintable_payload_exception_still_fails_as_the_payloads(
    harness: ExecutorHarness,
) -> None:
    # The formatter runs inside the handler that owns this failure. If its own
    # rendering could raise, the pool would report worker death and the
    # in-process executor a generic termination — a different outcome kind for
    # what is still an ordinary payload raise. It stays payload-owned.
    completed = run_one(harness, RAISES_UNPRINTABLE)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    detail = completed.result.attribution.detail
    assert detail is not None
    # The type still identifies the failure even though its message cannot be
    # rendered, and the placeholder names what refused to render.
    assert "UnprintableError" in detail
    assert "<unprintable UnprintableError: __str__ raised TypeError>" in detail


@pytest.mark.parametrize(
    "entry_point",
    [
        RAISES_HOSTILE_ENCODE_MESSAGE,
        RAISES_HOSTILE_SIZING_MESSAGE,
        RAISES_HOSTILE_QUALNAME,
    ],
    ids=["encode-raises", "sizing-raises", "hostile-qualname"],
)
def test_a_hostile_str_subclass_still_fails_as_the_payloads(
    harness: ExecutorHarness,
    entry_point: ImportableEntryPoint,
) -> None:
    # A payload controls the *type* of the strings the formatter reads, not
    # just their contents. A str subclass satisfies isinstance(x, str) while
    # overriding encode, __len__, __getitem__, __add__, or __format__ with a
    # method that raises, which would escape the guards during sizing or
    # interpolation. The formatter normalizes to an exact str first, so this
    # stays an ordinary payload raise rather than becoming worker death in
    # pool execution or a generic termination in-process.
    completed = run_one(harness, entry_point)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    detail = completed.result.attribution.detail
    assert detail is not None
    assert type(detail) is str
    assert len(detail.encode("utf-8")) <= PAYLOAD_ERROR_DETAIL_MAX_BYTES


def test_a_lone_surrogate_payload_message_is_escaped_not_raised(
    harness: ExecutorHarness,
) -> None:
    # A lone surrogate is a legal str that strict UTF-8 cannot encode. Sizing
    # the detail must not be the step that turns a payload raise into
    # something else, so the character is escaped rather than encoded.
    completed = run_one(harness, RAISES_LONE_SURROGATE)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    detail = completed.result.attribution.detail
    assert detail is not None
    assert "ValueError" in detail
    assert "SENTINEL-12345" in detail
    assert "\\ud800" in detail


def test_a_surrogate_escaped_payload_message_is_escaped_not_raised(
    harness: ExecutorHarness,
) -> None:
    # Undecodable OS-level bytes reach Python as surrogateescape code points,
    # which strict UTF-8 also refuses. Same requirement: escape, never raise.
    completed = run_one(harness, RAISES_SURROGATE_ESCAPE)

    outcome = completed.result.outcome
    assert isinstance(outcome, ExitedOutcome)
    assert outcome.exit_code == 1
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD
    detail = completed.result.attribution.detail
    assert detail is not None
    assert "OSError" in detail
    assert "SENTINEL-12345" in detail
    assert "\\udcff" in detail


def test_non_json_return_maps_to_protocol_failure_with_payload_owner(
    harness: ExecutorHarness,
) -> None:
    completed = run_one(harness, RETURN_NON_JSON)

    assert isinstance(completed.result.outcome, ProtocolFailedOutcome)
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


def test_wrong_target_raises_executor_failure(
    harness: ExecutorHarness,
) -> None:
    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(ExecutorFailure),
    ):
        executor.run_blocking(job_for(trusted_python_target()))


def test_non_empty_env_is_rejected_before_run(
    harness: ExecutorHarness,
) -> None:
    job = replace(build_job(), env=EnvGrant.fixed({"VISIBLE": "yes"}))

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError),
    ):
        executor.run_blocking(job)


def test_finite_input_budget_is_enforced_before_run(
    harness: ExecutorHarness,
) -> None:
    job = build_job(
        request={"too": "large"},
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=1)),
    )

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError),
    ):
        executor.run_blocking(job)


def test_finite_payload_output_budget_is_rejected_before_run(
    harness: ExecutorHarness,
) -> None:
    job = build_job(
        budgets=Budgets(
            payload_output=FiniteOutput(
                max_bytes=100,
                overflow_policy=OutputOverflowPolicy.MARKED_TRUNCATION,
                retention=PayloadRetentionBudget(
                    stdout=StreamRetentionBudget(head_bytes=100, tail_bytes=0),
                    stderr=StreamRetentionBudget(head_bytes=0, tail_bytes=0),
                ),
            ),
        ),
    )

    with (
        harness.open(ECHO, workers=1) as executor,
        pytest.raises(DeclarationError, match="payload_output"),
    ):
        executor.run_blocking(job)


def test_pre_cancelled_token_returns_cancelled_outcome(
    harness: ExecutorHarness,
) -> None:
    token = CancelToken()
    token.cancel()

    completed = run_one(harness, ECHO, cancellation=token)

    assert completed.result.outcome == CancelledOutcome(started=False)
    assert completed.result.attribution.owner is FailureOwner.NONE


def test_cancel_after_entry_outranks_a_non_json_result(
    harness: ExecutorHarness,
    tmp_path: Path,
) -> None:
    completed = _cancel_after_ready(
        harness, RETURN_NON_JSON, tmp_path / "non-json"
    )

    assert completed.result.outcome == CancelledOutcome(started=True)


def test_cancel_after_entry_outranks_system_exit(
    harness: ExecutorHarness,
    tmp_path: Path,
) -> None:
    completed = _cancel_after_ready(
        harness, RAISE_SYSTEM_EXIT, tmp_path / "system-exit"
    )

    assert completed.result.outcome == CancelledOutcome(started=True)


def test_an_unbudgeted_job_installs_no_deadline(
    harness: ExecutorHarness,
) -> None:
    """An unbudgeted job is never stopped by the executor itself."""

    completed = run_one(harness, ECHO, budgets=Budgets.unbudgeted())

    assert parse_importable_json_result(completed) == {
        "value": {"question": 42}
    }


def test_concurrent_calls_each_return_their_own_result(
    harness: ExecutorHarness,
) -> None:
    jobs = tuple(
        build_job(ECHO, {"index": index}, job_id=JobId(UUID(int=index + 10)))
        for index in range(4)
    )

    with harness.open(ECHO, workers=4) as executor:
        returned = _run_pool(executor, jobs, capacity=4)

    assert sorted(
        cast("list[dict[str, dict[str, int]]]", returned),
        key=lambda value: value["value"]["index"],
    ) == [{"value": {"index": index}} for index in range(4)]


def test_open_pool_streams_parsed_results(harness: ExecutorHarness) -> None:
    jobs = tuple(
        build_job(ECHO, {"index": index}, job_id=JobId(UUID(int=index + 1)))
        for index in range(2)
    )

    with harness.open(ECHO, workers=2) as executor:
        returned = _run_pool(executor, jobs, capacity=2)

    assert sorted(
        cast("list[dict[str, dict[str, int]]]", returned),
        key=lambda value: value["value"]["index"],
    ) == [{"value": {"index": 0}}, {"value": {"index": 1}}]


def test_a_failing_job_leaves_the_pool_healthy(
    harness: ExecutorHarness,
) -> None:
    fail_job = build_job(
        ECHO_OR_RAISE, {"raise": True}, job_id=JobId(UUID(int=3))
    )
    echo_job = build_job(
        ECHO_OR_RAISE, {"index": 4}, job_id=JobId(UUID(int=4))
    )

    async def run() -> list[tuple[str, CompletedExecution]]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[str]]:
            yield ExecutionSubmission(job=fail_job, context="fail")
            yield ExecutionSubmission(job=echo_job, context="echo")

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=1)
            )
        ) as pool:
            return [
                (item.context, item.completed_execution)
                async for item in pool.run_stream(submissions())
            ]

    with harness.open(ECHO_OR_RAISE, workers=1) as executor:
        returned = dict(asyncio.run(run()))

    failed = returned["fail"].result.outcome
    assert isinstance(failed, ExitedOutcome)
    assert failed.exit_code == 1
    assert parse_importable_json_result(returned["echo"]) == {
        "value": {"index": 4}
    }


def test_a_batch_wall_reports_whether_cancelled_execution_started(
    harness: ExecutorHarness,
    tmp_path: Path,
) -> None:
    ready = tmp_path / "ready"
    gate = Gate.create(tmp_path, "gate")
    running = build_job(
        ECHO_OR_BLOCK,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
        job_id=JobId(UUID(int=21)),
    )
    pending = build_job(
        ECHO_OR_BLOCK, {"value": 1}, job_id=JobId(UUID(int=22))
    )
    wall = FiniteDurationLimit(max_ns=500_000_000)
    collected: list[CompletedExecution] = []

    def drain() -> None:
        with harness.open(ECHO_OR_BLOCK, workers=1) as executor:
            collected.extend(
                executor.run_many(
                    (running, pending),
                    config=ExecutionPoolConfig(
                        capacity=FixedPoolCapacity(max_active_jobs=1)
                    ),
                    wall_time=wall,
                )
            )

    driver = threading.Thread(target=drain)
    begun = time.monotonic()
    driver.start()
    _await_ready(ready, driver=driver)
    _wait_out_the_wall(begun, wall)
    _release_in_process_gate(harness, gate)
    _join_driver(driver)

    outcomes = {
        one.result.execution_id.job_id: one.result.outcome for one in collected
    }
    assert outcomes[running.job_id] == CancelledOutcome(started=True)
    assert outcomes[pending.job_id] == CancelledOutcome(started=False)


def test_a_job_admitted_after_the_batch_wall_reports_started_false(
    harness: ExecutorHarness,
    tmp_path: Path,
) -> None:
    ready = tmp_path / "ready"
    gate = Gate.create(tmp_path, "gate")
    holder = build_job(
        ECHO_OR_BLOCK,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
        job_id=JobId(UUID(int=23)),
    )
    later = build_job(ECHO_OR_BLOCK, {"value": 2}, job_id=JobId(UUID(int=24)))
    wall = FiniteDurationLimit(max_ns=500_000_000)
    expired = threading.Event()
    collected: list[CompletedExecution] = []

    def jobs() -> Iterator[ExecutionJob]:
        yield holder
        if not expired.wait(WATCHDOG_SECONDS):
            raise AssertionError("watchdog fired waiting for the batch wall")
        yield later

    def drain() -> None:
        with harness.open(ECHO_OR_BLOCK, workers=1) as executor:
            collected.extend(
                executor.run_many(
                    jobs(),
                    config=ExecutionPoolConfig(
                        capacity=FixedPoolCapacity(max_active_jobs=1)
                    ),
                    wall_time=wall,
                )
            )

    driver = threading.Thread(target=drain)
    begun = time.monotonic()
    driver.start()
    _await_ready(ready, driver=driver)
    _wait_out_the_wall(begun, wall)
    expired.set()
    _release_in_process_gate(harness, gate)
    _join_driver(driver)

    outcomes = {
        one.result.execution_id.job_id: one.result.outcome for one in collected
    }
    assert outcomes[holder.job_id] == CancelledOutcome(started=True)
    assert outcomes[later.job_id] == CancelledOutcome(started=False)


def _cancel_after_ready(
    harness: ExecutorHarness,
    entry_point: ImportableEntryPoint,
    directory: Path,
    /,
) -> CompletedExecution:
    directory.mkdir()
    ready = directory / "ready"
    gate = Gate.create(directory, "gate")
    token = CancelToken()
    job = build_job(
        entry_point,
        {"ready_path": str(ready), "gate_path": str(gate.path)},
    )
    collected: list[CompletedExecution] = []

    def run() -> None:
        with harness.open(entry_point, workers=1) as executor:
            collected.append(executor.run_blocking(job, cancellation=token))

    driver = threading.Thread(target=run)
    driver.start()
    _await_ready(ready, driver=driver)
    token.cancel()
    _release_in_process_gate(harness, gate)
    _join_driver(driver)
    assert collected, "the batch driver returned no completion"
    return collected[0]


def _await_ready(marker: Path, /, *, driver: threading.Thread) -> None:
    deadline = time.monotonic() + WATCHDOG_SECONDS
    while not marker.exists():
        if not driver.is_alive():
            pytest.fail(
                "the batch finished before the payload announced ready"
            )
        if time.monotonic() >= deadline:
            pytest.fail("watchdog fired waiting for the payload ready marker")
        time.sleep(0.001)


def _wait_out_the_wall(begun: float, wall: FiniteDurationLimit, /) -> None:
    remaining = (wall.max_ns / 1e9) - (time.monotonic() - begun) + 0.05
    if remaining > 0:
        time.sleep(remaining)


def _release_in_process_gate(harness: ExecutorHarness, gate: Gate, /) -> None:
    """Unblock the cooperative in-process body after the wall has fired.

    Worker-pool cancel kills the reader. Opening the FIFO for write would
    then block forever, and the killed job does not need the release.
    """

    if harness.name == "in_process":
        gate.release()


def _join_driver(driver: threading.Thread, /) -> None:
    driver.join(WATCHDOG_SECONDS)
    if driver.is_alive():
        pytest.fail("watchdog fired joining the batch driver")


def _run_pool(
    executor: PooledExecutor,
    jobs: tuple[ExecutionJob, ...],
    /,
    *,
    capacity: int,
) -> list[Jsonable]:
    async def run() -> list[Jsonable]:
        async def submissions() -> AsyncIterator[ExecutionSubmission[int]]:
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=index)

        async with executor.open_pool(
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=capacity)
            )
        ) as pool:
            return [
                parse_importable_json_result(item.completed_execution)
                async for item in pool.run_stream(submissions())
            ]

    return asyncio.run(run())
