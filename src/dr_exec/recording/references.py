from __future__ import annotations

from typing import Final
from uuid import NAMESPACE_URL, UUID, uuid5

from dr_exec.core.names import AttemptId, JobId
from dr_exec.recording.models import RunRecordReference

_ATTEMPT_ID_NAMESPACE: Final = UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f80")
_RECORD_ID_NAMESPACE: Final = UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f81")
_JOB_ID_NAME_NAMESPACE: Final = NAMESPACE_URL


def _job_id_name(job_id: JobId, /) -> str:
    return f"dr-exec/job-id/{job_id}"


def attempt_id_for_job(job_id: JobId, /) -> AttemptId:
    """Derive one attempt identity from one caller-supplied job identity."""

    derived = uuid5(
        _ATTEMPT_ID_NAMESPACE, _job_id_name(job_id).encode("ascii")
    )
    return AttemptId(derived)


def record_reference_for_job(job_id: JobId, /) -> RunRecordReference:
    """Derive one durable record locator from one caller-supplied job identity."""

    derived = uuid5(_RECORD_ID_NAMESPACE, _job_id_name(job_id).encode("ascii"))
    return RunRecordReference(record_id=derived)


__all__ = ["attempt_id_for_job", "record_reference_for_job"]
