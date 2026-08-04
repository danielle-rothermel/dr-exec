from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dr_exec.names import ExecutionId
from dr_exec.record import (
    ExecutionResult,
    PreparedRecord,
    ProcessRecord,
    RealRecordReceipt,
    RunRecord,
)


@dataclass(frozen=True, slots=True)
class PreparedRun:
    execution_id: ExecutionId
    record_dir: Path


@dataclass(frozen=True, slots=True)
class RunningRun:
    execution_id: ExecutionId
    record_dir: Path


type FinalizableRun = PreparedRun | RunningRun


@dataclass(frozen=True, slots=True)
class DirectoryRunStore:
    root: Path

    def prepare(
        self,
        record: PreparedRecord,
        /,
    ) -> PreparedRun:
        raise NotImplementedError(
            "DirectoryRunStore.prepare is not implemented"
        )

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        raise NotImplementedError(
            "DirectoryRunStore.mark_running is not implemented"
        )

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        raise NotImplementedError(
            "DirectoryRunStore.finalize is not implemented"
        )

    def load(
        self,
        record_dir: Path,
        /,
    ) -> RunRecord:
        raise NotImplementedError("DirectoryRunStore.load is not implemented")


__all__ = [
    "DirectoryRunStore",
    "FinalizableRun",
    "PreparedRun",
    "RunningRun",
]
