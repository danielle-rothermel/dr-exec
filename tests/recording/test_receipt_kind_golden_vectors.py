from __future__ import annotations

from uuid import UUID

from dr_exec import (
    AttemptId,
    CompleteRecordReceipt,
    DegradedRecordReceipt,
    ExecutionId,
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

EXECUTION_ID = ExecutionId(
    job_id=JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70")),
    attempt_id=AttemptId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71")),
)


def test_every_receipt_kind_literal_is_pinned() -> None:
    assert {
        member.name: member.value for member in RecordReceiptKind
    } == EXPECTED_RECEIPT_KIND_LITERALS


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
