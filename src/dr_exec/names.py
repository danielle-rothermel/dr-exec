from __future__ import annotations

from typing import NewType
from uuid import UUID

from dr_exec._model import ContractModel

JobId = NewType("JobId", UUID)
AttemptId = NewType("AttemptId", UUID)


class ExecutionId(ContractModel):
    job_id: JobId
    attempt_id: AttemptId


__all__ = ["AttemptId", "ExecutionId", "JobId"]
