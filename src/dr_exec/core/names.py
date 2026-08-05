from __future__ import annotations

from typing import NewType

from dr_exec.core.model import CanonicalUuid, ContractModel

JobId = NewType("JobId", CanonicalUuid)
AttemptId = NewType("AttemptId", CanonicalUuid)


class ExecutionId(ContractModel):
    job_id: JobId
    attempt_id: AttemptId


__all__ = ["AttemptId", "ExecutionId", "JobId"]
