from __future__ import annotations

from typing import Protocol

from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import (
    ExecutionJob,
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.recording.models import (
    CompletedExecution,
    ExecutionResult,
    OutputArtifactRecord,
    PreparedRecord,
    ProcessRecord,
    RealRecordReceipt,
    RunRecord,
    RunRecordReference,
)
from dr_exec.recording.store import FinalizableRun, PreparedRun, RunningRun
from dr_exec.runtime.host import PreparedPythonProcess, RuntimeRecord


class Executor(Protocol):
    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        raise NotImplementedError

    def run_blocking(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        raise NotImplementedError


class Runtime(Protocol):
    """Prepare a Python target for one child invocation.

    A conforming ``prepare()`` returns an absolute ``argv[0]``, the resolved
    interpreter path, and ``request_bytes`` that are exactly the canonical
    transport bytes of the declared request. The declaration gate enforces
    workload budgets against the declaration, so these two properties are what
    make pre-spawn enforcement truthful for the prepared artifacts; a runtime
    that breaks either one voids it.
    """

    def prepare(
        self,
        target: TrustedPythonTarget | UntrustedPythonTarget,
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
        reference: RunRecordReference,
        /,
    ) -> RunRecord:
        raise NotImplementedError

    def read_artifact(
        self,
        reference: RunRecordReference,
        artifact: OutputArtifactRecord,
        /,
        *,
        max_bytes: int,
    ) -> bytes:
        raise NotImplementedError


__all__ = ["Executor", "RunStore", "Runtime"]
