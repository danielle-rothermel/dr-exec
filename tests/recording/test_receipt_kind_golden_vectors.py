from __future__ import annotations

from uuid import UUID

from dr_exec import (
    AttemptId,
    CompleteRecordReceipt,
    DegradedRecordReceipt,
    ExecutionId,
    ExecutorFailureCode,
    FakeRecordReceipt,
    InProcessRecordReceipt,
    JobId,
    RecordReceiptKind,
    RecordState,
    RunRecordReference,
    WorkerPoolRecordReceipt,
)

# Persisted-format contract: these are the exact stored receipt-kind literals.
# Never derive them from member names; drift here silently changes evidence
# already written to disk.
EXPECTED_RECEIPT_KIND_LITERALS = {
    "COMPLETE": "complete",
    "DEGRADED": "degraded",
    "NOT_APPLICABLE": "not_applicable",
    "IN_PROCESS": "in_process",
    "WORKER_POOL": "worker_pool",
}

EXPECTED_EXECUTOR_FAILURE_CODE_LITERALS = {
    "TARGET_NOT_SUPPORTED": "target_not_supported",
    "BOOTSTRAP_TIMEOUT": "bootstrap_timeout",
    "TRANSPORT_WORKER_FAILED": "transport_worker_failed",
    "STDIN_TRANSPORT_TAKEN": "stdin_transport_taken",
    "PROTOCOL_TRANSPORT_TAKEN": "protocol_transport_taken",
    "TRANSPORT_JOIN_TIMEOUT": "transport_join_timeout",
    "BOOTSTRAP_START_FAILED": "bootstrap_start_failed",
    "RECORDING_OPERATION_FAILED": "recording_operation_failed",
    "POOL_CAPACITY_UNRESOLVED": "pool_capacity_unresolved",
    "POOL_INVALID_STATE": "pool_invalid_state",
    "POOL_WRONG_EVENT_LOOP": "pool_wrong_event_loop",
    "POOL_NO_SCHEDULER": "pool_no_scheduler",
    "SCHEDULER_BROKEN": "scheduler_broken",
    "WORKER_POOL_TARGET_MISMATCH": "worker_pool_target_mismatch",
    "WORKER_POOL_ENTRY_POINT_MISMATCH": "worker_pool_entry_point_mismatch",
    "IMPORTABLE_JSON_TARGET_MISMATCH": "importable_json_target_mismatch",
    "FAKE_NO_RESPONSE": "fake_no_response",
    "FAKE_RECEIPT_MISMATCH": "fake_receipt_mismatch",
}

EXECUTION_ID = ExecutionId(
    job_id=JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70")),
    attempt_id=AttemptId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71")),
)


def test_every_receipt_kind_literal_is_pinned() -> None:
    assert {
        member.name: member.value for member in RecordReceiptKind
    } == EXPECTED_RECEIPT_KIND_LITERALS


def test_every_executor_failure_code_literal_is_pinned() -> None:
    assert {
        member.name: member.value for member in ExecutorFailureCode
    } == EXPECTED_EXECUTOR_FAILURE_CODE_LITERALS


def test_complete_receipts_serialize_their_pinned_kind() -> None:
    receipt = CompleteRecordReceipt(
        execution_id=EXECUTION_ID,
        reference=RunRecordReference(record_id=UUID(int=4)),
    )

    assert receipt.model_dump(mode="json")["kind"] == "complete"


def test_degraded_receipts_serialize_their_pinned_kind() -> None:
    receipt = DegradedRecordReceipt(
        execution_id=EXECUTION_ID,
        reference=RunRecordReference(record_id=UUID(int=4)),
        latest_state=RecordState.RUNNING,
        failures=(),
    )

    assert receipt.model_dump(mode="json")["kind"] == "degraded"


def test_not_applicable_receipts_serialize_their_pinned_kind() -> None:
    receipt = FakeRecordReceipt(execution_id=EXECUTION_ID)

    assert receipt.model_dump(mode="json")["kind"] == "not_applicable"


def test_in_process_receipts_serialize_their_pinned_kind() -> None:
    receipt = InProcessRecordReceipt(execution_id=EXECUTION_ID)

    assert receipt.model_dump(mode="json")["kind"] == "in_process"


def test_worker_pool_receipts_serialize_their_pinned_kind() -> None:
    receipt = WorkerPoolRecordReceipt(execution_id=EXECUTION_ID)

    assert receipt.model_dump(mode="json")["kind"] == "worker_pool"
