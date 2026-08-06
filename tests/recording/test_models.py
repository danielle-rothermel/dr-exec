from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from dr_exec import (
    AttemptId,
    CompletedExecution,
    CompleteRecordReceipt,
    ExecutionAttribution,
    ExecutionAttributionRecord,
    ExecutionId,
    ExecutionMeasurements,
    ExecutionResult,
    ExecutionResultRecord,
    ExitedOutcome,
    FailureOwner,
    JobId,
    OutputArtifactRecord,
    PayloadOutputRecords,
    PayloadOutputs,
    ProtocolFailedOutcome,
    ProtocolFailedOutcomeRecord,
    ProtocolFailureCode,
    RetainedPayloadStream,
    RetainedPayloadStreamRecord,
)
from dr_exec.capabilities import CachedRecordReceipt

VALID_DIGEST = "a" * 64
STARTED_AT = datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)
FINISHED_AT = datetime(2026, 8, 5, 12, 0, 1, tzinfo=UTC)


def _execution_id(value: int) -> ExecutionId:
    return ExecutionId(
        job_id=JobId(UUID(int=value)),
        attempt_id=AttemptId(UUID(int=value + 1)),
    )


def _measurements() -> ExecutionMeasurements:
    return ExecutionMeasurements(
        started_at=STARTED_AT,
        finished_at=FINISHED_AT,
        duration_ns=1_000_000_000,
        teardown_duration_ns=0,
        input_bytes=0,
        protocol_bytes_received=0,
    )


def _empty_stream() -> RetainedPayloadStream:
    return RetainedPayloadStream(
        head=b"",
        tail=b"",
        produced_bytes=0,
        dropped_bytes=0,
    )


def _empty_stream_record() -> RetainedPayloadStreamRecord:
    return RetainedPayloadStreamRecord(
        head_bytes=0,
        tail_bytes=0,
        produced_bytes=0,
        dropped_bytes=0,
    )


def _result(execution_id: ExecutionId) -> ExecutionResult:
    return ExecutionResult(
        execution_id=execution_id,
        outcome=ExitedOutcome(exit_code=0),
        attribution=ExecutionAttribution(owner=FailureOwner.NONE),
        protocol_outputs=(),
        payload_outputs=PayloadOutputs(
            stdout=_empty_stream(),
            stderr=_empty_stream(),
        ),
        measurements=_measurements(),
    )


@pytest.mark.parametrize(
    "relative_path",
    [
        pytest.param("/absolute.bin", id="absolute"),
        pytest.param("./stdout.bin", id="dot-component"),
        pytest.param("../stdout.bin", id="parent-component"),
        pytest.param("nested//stdout.bin", id="empty-component"),
        pytest.param("", id="empty"),
        pytest.param(".", id="bare-dot"),
    ],
)
def test_artifact_paths_reject_unsafe_wire_spellings(
    relative_path: str,
) -> None:
    payload = json.dumps(
        {
            "relative_path": relative_path,
            "size_bytes": 0,
            "sha256": VALID_DIGEST,
        }
    ).encode()

    with pytest.raises(ValidationError):
        OutputArtifactRecord.model_validate_json(payload, strict=True)


@pytest.mark.parametrize(
    "digest",
    [
        pytest.param("a" * 63, id="too-short"),
        pytest.param("a" * 65, id="too-long"),
        pytest.param("A" * 64, id="uppercase"),
        pytest.param("g" * 64, id="non-hexadecimal"),
        pytest.param("sha256:" + "a" * 57, id="prefixed"),
    ],
)
def test_artifact_digests_reject_noncanonical_spellings(digest: str) -> None:
    payload = json.dumps(
        {
            "relative_path": "stdout.bin",
            "size_bytes": 0,
            "sha256": digest,
        }
    ).encode()

    with pytest.raises(ValidationError):
        OutputArtifactRecord.model_validate_json(payload, strict=True)


def test_live_stream_accounting_rejects_an_inconsistent_total() -> None:
    with pytest.raises(ValidationError, match="retained and dropped"):
        RetainedPayloadStream(
            head=b"head",
            tail=b"tail",
            produced_bytes=7,
            dropped_bytes=0,
        )


def test_recorded_stream_accounting_rejects_an_inconsistent_total() -> None:
    with pytest.raises(ValidationError, match="retained and dropped"):
        RetainedPayloadStreamRecord(
            head_bytes=4,
            tail_bytes=4,
            produced_bytes=7,
            dropped_bytes=0,
        )


def test_measurements_reject_finish_before_start() -> None:
    with pytest.raises(ValidationError, match="must not precede"):
        ExecutionMeasurements(
            started_at=FINISHED_AT,
            finished_at=STARTED_AT,
            duration_ns=0,
            teardown_duration_ns=0,
            input_bytes=0,
            protocol_bytes_received=0,
        )


def test_live_protocol_failure_count_must_match_accepted_outputs() -> None:
    execution_id = _execution_id(1)

    with pytest.raises(ValidationError, match="count does not match"):
        ExecutionResult(
            execution_id=execution_id,
            outcome=ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
                failure_detail="diagnostic",
                accepted_output_count=1,
            ),
            attribution=ExecutionAttribution(owner=FailureOwner.PAYLOAD),
            protocol_outputs=(),
            payload_outputs=PayloadOutputs(
                stdout=_empty_stream(),
                stderr=_empty_stream(),
            ),
            measurements=_measurements(),
        )


def test_recorded_protocol_failure_count_must_match_accepted_outputs() -> None:
    execution_id = _execution_id(1)

    with pytest.raises(ValidationError, match="count does not match"):
        ExecutionResultRecord(
            execution_id=execution_id,
            outcome=ProtocolFailedOutcomeRecord(
                failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
                accepted_output_count=1,
            ),
            attribution=ExecutionAttributionRecord(owner=FailureOwner.PAYLOAD),
            protocol_outputs=(),
            payload_outputs=PayloadOutputRecords(
                stdout=_empty_stream_record(),
                stderr=_empty_stream_record(),
            ),
            measurements=_measurements(),
        )


def test_completed_execution_binds_result_and_receipt_ids(
    tmp_path: Path,
) -> None:
    result = _result(_execution_id(1))
    receipt = CompleteRecordReceipt(
        execution_id=_execution_id(3),
        record_dir=tmp_path / "run",
    )

    with pytest.raises(ValidationError, match="execution IDs differ"):
        CompletedExecution(result=result, record_receipt=receipt)


def test_cached_completion_binds_result_to_source_not_requested_job() -> None:
    source_execution_id = _execution_id(1)
    receipt = CachedRecordReceipt(
        requested_job_id=_execution_id(3).job_id,
        source_execution_id=_execution_id(5),
        cache_key="dr_exec.test_cache_key",
    )

    with pytest.raises(ValidationError, match="execution IDs differ"):
        CompletedExecution(
            result=_result(source_execution_id),
            record_receipt=receipt,
        )
