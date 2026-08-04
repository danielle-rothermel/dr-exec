from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dr_exec.declare import ExecutionJob, UntrustedPythonTarget
from dr_exec.record import (
    CompletedExecution,
    ExecutionResult,
    PreparedRecord,
    ProcessRecord,
    RealRecordReceipt,
    RunRecord,
)
from dr_exec.runtime import PreparedPythonProcess, RuntimeRecord
from dr_exec.store import FinalizableRun, PreparedRun, RunningRun


class Executor(Protocol):
    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        raise NotImplementedError


class Runtime(Protocol):
    def prepare(
        self,
        target: UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        raise NotImplementedError

    def describe(self) -> RuntimeRecord:
        raise NotImplementedError


class RunStore(Protocol):
    def prepare(
        self,
        record: PreparedRecord,
        /,
    ) -> PreparedRun:
        raise NotImplementedError

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        raise NotImplementedError

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        raise NotImplementedError

    def load(
        self,
        record_dir: Path,
        /,
    ) -> RunRecord:
        raise NotImplementedError


__all__ = ["Executor", "RunStore", "Runtime"]
