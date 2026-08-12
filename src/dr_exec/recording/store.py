from __future__ import annotations

import shutil
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast
from uuid import UUID

from dr_serialize import (
    Jsonable,
    JsonByteLimitError,
    SerializationError,
    Sha256Digest,
    canonical_json_bytes,
)
from dr_store import (
    AllocationError,
    DocumentDirectory,
    DocumentDirectoryError,
    SidecarSummary,
    VerifiedRegularChildReadError,
    read_verified_regular_child,
)
from pydantic import TypeAdapter, ValidationError

from dr_exec.core.errors import ExecutorFailure, RecordLoadError
from dr_exec.core.kinds import RecordState
from dr_exec.core.model import (
    STRUCTURAL_DEPTH_CEILING,
    ContractModel,
)
from dr_exec.core.names import ExecutionId
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    BudgetExceededOutcomeRecord,
    CancelledOutcome,
    CancelledOutcomeRecord,
    CompleteRecordReceipt,
    DegradedRecordReceipt,
    ExecutionAttribution,
    ExecutionAttributionRecord,
    ExecutionOutcome,
    ExecutionOutcomeRecord,
    ExecutionResult,
    ExecutionResultRecord,
    ExitedOutcome,
    ExitedOutcomeRecord,
    FinalizedRecord,
    OutputArtifactRecord,
    OutputArtifactRecords,
    PayloadOutputRecords,
    PreparedRecord,
    ProcessRecord,
    ProtocolFailedOutcome,
    ProtocolFailedOutcomeRecord,
    RealRecordReceipt,
    RecordingFailure,
    RetainedPayloadStream,
    RetainedPayloadStreamRecord,
    RunDeclaration,
    RunningRecord,
    RunRecord,
    RunRecordHeader,
    RunRecordReference,
    SignaledOutcome,
    SignaledOutcomeRecord,
    SpawnAbsentOutcome,
    SpawnAbsentOutcomeRecord,
    SpawnFailedOutcome,
    SpawnFailedOutcomeRecord,
)
from dr_exec.recording.references import record_reference_for_job

RECORD_DIRECTORY_PREFIX = "run"
MANIFEST_NAME = "record.json"
STDOUT_SIDECAR_NAME = "stdout.bin"
STDERR_SIDECAR_NAME = "stderr.bin"

_RUN_RECORD_ADAPTER: TypeAdapter[RunRecord] = TypeAdapter(RunRecord)

STRUCTURAL_MANIFEST_BYTE_CEILING: Final = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PreparedRun:
    """Run whose complete declaration is durably recorded pre-spawn."""

    execution_id: ExecutionId
    reference: RunRecordReference
    header: RunRecordHeader
    declaration: RunDeclaration


@dataclass(frozen=True, slots=True)
class RunningRun:
    """Run whose spawned child is durably recorded."""

    execution_id: ExecutionId
    reference: RunRecordReference
    header: RunRecordHeader
    declaration: RunDeclaration


type FinalizableRun = PreparedRun | RunningRun


def _manifest_payload(record: ContractModel, /) -> Jsonable:
    return cast("Jsonable", record.model_dump(mode="json"))


def _outcome_record(outcome: ExecutionOutcome, /) -> ExecutionOutcomeRecord:
    match outcome:
        case ExitedOutcome():
            return ExitedOutcomeRecord(exit_code=outcome.exit_code)
        case SignaledOutcome():
            return SignaledOutcomeRecord(signal_number=outcome.signal_number)
        case SpawnAbsentOutcome():
            return SpawnAbsentOutcomeRecord(executable=outcome.executable)
        case SpawnFailedOutcome():
            return SpawnFailedOutcomeRecord(
                errno=outcome.errno,
                error_message=outcome.error_message,
            )
        case BudgetExceededOutcome():
            return BudgetExceededOutcomeRecord(axis=outcome.axis)
        case ProtocolFailedOutcome():
            return ProtocolFailedOutcomeRecord(
                failure_code=outcome.failure_code,
                failure_detail=outcome.failure_detail,
                accepted_output_count=outcome.accepted_output_count,
            )
        case CancelledOutcome():
            return CancelledOutcomeRecord()


def _attribution_record(
    attribution: ExecutionAttribution,
    /,
) -> ExecutionAttributionRecord:
    return ExecutionAttributionRecord(
        owner=attribution.owner,
        detail=attribution.detail,
    )


def _retained_stream_record(
    stream: RetainedPayloadStream,
    summary: SidecarSummary,
    /,
) -> RetainedPayloadStreamRecord:
    return RetainedPayloadStreamRecord(
        head_bytes=summary.head_length,
        tail_bytes=summary.tail_length,
        produced_bytes=stream.produced_bytes,
        dropped_bytes=stream.dropped_bytes,
    )


def _execution_result_record(
    result: ExecutionResult,
    stdout_summary: SidecarSummary,
    stderr_summary: SidecarSummary,
    /,
) -> ExecutionResultRecord:
    return ExecutionResultRecord(
        execution_id=result.execution_id,
        outcome=_outcome_record(result.outcome),
        attribution=_attribution_record(result.attribution),
        protocol_outputs=result.protocol_outputs,
        payload_outputs=PayloadOutputRecords(
            stdout=_retained_stream_record(
                result.payload_outputs.stdout, stdout_summary
            ),
            stderr=_retained_stream_record(
                result.payload_outputs.stderr, stderr_summary
            ),
        ),
        measurements=result.measurements,
    )


def _artifact_record(
    name: str,
    summary: SidecarSummary,
    /,
) -> OutputArtifactRecord:
    return OutputArtifactRecord(
        relative_path=Path(name),
        size_bytes=summary.head_length + summary.tail_length,
        sha256=Sha256Digest(summary.sidecar_hash),
    )


def _recording_failure(
    operation: str,
    error: Exception,
    /,
) -> RecordingFailure:
    return RecordingFailure(
        operation=operation,
        errno=_explicit_cause_chain_errno(error),
        detail=type(error).__name__,
    )


def _explicit_cause_chain_errno(error: BaseException, /) -> int | None:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        error_number = getattr(current, "errno", None)
        if isinstance(error_number, int):
            return error_number
        current = current.__cause__
    return None


def _write_sidecar(
    directory: DocumentDirectory,
    name: str,
    stream: RetainedPayloadStream,
    /,
) -> SidecarSummary:
    writer = directory.open_sidecar(
        name,
        head_cap=len(stream.head),
        tail_cap=len(stream.tail),
    )
    writer.write(stream.head)
    writer.write(stream.tail)
    return writer.finalize()


@dataclass(frozen=True, slots=True)
class DirectoryRunStore:
    """Persist each attempt as an atomic lifecycle manifest and sidecars."""

    root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", self.root.absolute())

    def prepare(
        self,
        record: PreparedRecord,
        /,
    ) -> PreparedRun:
        with _executor_failure("prepare the run record"):
            reference = record_reference_for_job(
                record.declaration.execution_id.job_id
            )
            directory = self._allocate(reference)
            try:
                directory.publish(_manifest_payload(record))
            except DocumentDirectoryError:
                self._reclaim_unprepared_allocation(directory.path)
                raise
        return PreparedRun(
            execution_id=record.declaration.execution_id,
            reference=reference,
            header=record.header,
            declaration=record.declaration,
        )

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        with _executor_failure("publish the running run record"):
            _directory(self._resolve(prepared_run.reference)).publish(
                _manifest_payload(
                    RunningRecord(
                        header=prepared_run.header,
                        declaration=prepared_run.declaration,
                        process=process,
                    )
                )
            )
        return RunningRun(
            execution_id=prepared_run.execution_id,
            reference=prepared_run.reference,
            header=prepared_run.header,
            declaration=prepared_run.declaration,
        )

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        try:
            self._publish_finalized(run, result)
        except (DocumentDirectoryError, RecordLoadError) as error:
            return DegradedRecordReceipt(
                execution_id=run.execution_id,
                reference=run.reference,
                latest_state=self._durable_state(run),
                failures=(_recording_failure("finalize", error),),
            )
        return CompleteRecordReceipt(
            execution_id=run.execution_id,
            reference=run.reference,
        )

    def load(
        self,
        reference: RunRecordReference,
        /,
    ) -> RunRecord:
        """Recover a lifecycle record across process boundaries.

        Intended for cross-process recovery only, not for in-frame store
        transitions that already carry the manifest header forward.
        """

        record_dir = self._resolve(reference)
        record = _load_record(record_dir)
        if isinstance(record, FinalizedRecord):
            _verify_sidecars(record_dir, record)
        return record

    def read_artifact(
        self,
        reference: RunRecordReference,
        artifact: OutputArtifactRecord,
        /,
        *,
        max_bytes: int,
    ) -> bytes:
        """Read one finalized owned artifact through one pinned descriptor."""

        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
            raise TypeError("max_bytes must be an integer")
        if max_bytes < 0:
            raise ValueError("max_bytes must be non-negative")
        record_dir = self._resolve(reference)
        record = _load_record(record_dir)
        if not isinstance(record, FinalizedRecord):
            raise RecordLoadError(
                f"run record {reference.record_id} is not finalized"
            )
        if artifact not in (record.outputs.stdout, record.outputs.stderr):
            raise RecordLoadError(
                f"artifact is not owned by run record {reference.record_id}"
            )
        if artifact.size_bytes > max_bytes:
            raise RecordLoadError(
                f"artifact exceeds the {max_bytes}-byte read limit"
            )
        try:
            return read_verified_regular_child(
                record_dir,
                artifact.relative_path.as_posix(),
                max_bytes=max_bytes,
                expected_byte_length=artifact.size_bytes,
                expected_sha256=artifact.sha256,
            )
        except VerifiedRegularChildReadError as error:
            raise RecordLoadError(
                f"artifact {artifact.relative_path.as_posix()!r} at "
                f"{record_dir} does not match its record"
            ) from error

    def _publish_finalized(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> None:
        record_dir = self._resolve(run.reference)
        record = _load_record(record_dir)
        if isinstance(record, FinalizedRecord):
            raise RecordLoadError(
                f"run record {run.reference.record_id} is already finalized"
            )
        directory = _directory(record_dir)
        stdout_summary = _write_sidecar(
            directory,
            STDOUT_SIDECAR_NAME,
            result.payload_outputs.stdout,
        )
        stderr_summary = _write_sidecar(
            directory,
            STDERR_SIDECAR_NAME,
            result.payload_outputs.stderr,
        )
        directory.publish(
            _manifest_payload(
                FinalizedRecord(
                    header=record.header,
                    declaration=record.declaration,
                    result=_execution_result_record(
                        result,
                        stdout_summary,
                        stderr_summary,
                    ),
                    outputs=OutputArtifactRecords(
                        stdout=_artifact_record(
                            STDOUT_SIDECAR_NAME, stdout_summary
                        ),
                        stderr=_artifact_record(
                            STDERR_SIDECAR_NAME, stderr_summary
                        ),
                    ),
                )
            )
        )

    def _load_prepared(
        self, reference: RunRecordReference, /
    ) -> PreparedRecord:
        record_dir = self._resolve(reference)
        record = _load_record(record_dir)
        if not isinstance(record, PreparedRecord):
            raise RecordLoadError(
                f"run record at {record_dir} is not in the prepared state"
            )
        return record

    def _allocate(self, reference: RunRecordReference, /) -> DocumentDirectory:
        record_dir = self._record_dir(reference)
        try:
            record_dir.mkdir(exist_ok=False)
        except FileExistsError:
            self._reclaim_unprepared_allocation(record_dir)
            try:
                record_dir.mkdir(exist_ok=False)
            except OSError as error:
                raise AllocationError(
                    f"could not allocate run record {reference.record_id}"
                ) from error
        except OSError as error:
            raise AllocationError(
                f"could not allocate run record {reference.record_id}"
            ) from error
        return _directory(record_dir)

    def _reclaim_unprepared_allocation(self, record_dir: Path, /) -> None:
        try:
            _load_record(record_dir)
        except RecordLoadError:
            shutil.rmtree(record_dir)

    def _record_dir(self, reference: RunRecordReference, /) -> Path:
        return self.root / f"{RECORD_DIRECTORY_PREFIX}-{reference.record_id}"

    def _resolve(self, reference: RunRecordReference, /) -> Path:
        if not isinstance(reference, RunRecordReference):
            raise RecordLoadError("unsupported run record reference")
        if reference.backend != "directory":
            raise RecordLoadError(
                f"unsupported run record backend {reference.backend!r}"
            )
        if not isinstance(reference.record_id, UUID):
            raise RecordLoadError("malformed directory run record identifier")
        record_dir = self._record_dir(reference)
        try:
            metadata = record_dir.lstat()
        except OSError as error:
            raise RecordLoadError(
                f"could not resolve run record {reference.record_id}"
            ) from error
        if not stat.S_ISDIR(metadata.st_mode):
            raise RecordLoadError(
                f"could not resolve run record {reference.record_id}"
            )
        return record_dir

    def _durable_state(self, run: FinalizableRun, /) -> RecordState:
        try:
            return _load_record(self._resolve(run.reference)).state
        except RecordLoadError:
            return (
                RecordState.PREPARED
                if isinstance(run, PreparedRun)
                else RecordState.RUNNING
            )


@contextmanager
def _executor_failure(operation: str, /) -> Iterator[None]:
    try:
        yield
    except (DocumentDirectoryError, RecordLoadError) as error:
        raise ExecutorFailure(f"could not {operation}") from error


def _directory(record_dir: Path, /) -> DocumentDirectory:
    return DocumentDirectory(
        record_dir,
        MANIFEST_NAME,
        manifest_max_bytes=STRUCTURAL_MANIFEST_BYTE_CEILING,
        manifest_max_depth=STRUCTURAL_DEPTH_CEILING,
    )


def _load_record(record_dir: Path, /) -> RunRecord:
    try:
        manifest = _directory(record_dir).read_manifest()
    except DocumentDirectoryError as error:
        seen: set[int] = set()
        current: BaseException | None = error
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if isinstance(current, JsonByteLimitError):
                message = (
                    f"run record manifest at {record_dir} exceeds "
                    f"{STRUCTURAL_MANIFEST_BYTE_CEILING} bytes"
                )
                break
            if isinstance(current, (SerializationError, ValueError)):
                message = (
                    f"run record at {record_dir} is not canonical JSON bytes"
                )
                break
            current = current.__cause__
        else:
            message = f"could not read the run record at {record_dir}"
        raise RecordLoadError(message) from error
    manifest_bytes = canonical_json_bytes(manifest)
    try:
        return _RUN_RECORD_ADAPTER.validate_json(manifest_bytes, strict=True)
    except ValidationError as error:
        raise RecordLoadError(
            f"run record at {record_dir} is not a valid lifecycle record"
        ) from error


def _verify_sidecars(record_dir: Path, record: FinalizedRecord, /) -> None:
    try:
        directory = _directory(record_dir)
    except DocumentDirectoryError as error:
        raise RecordLoadError(
            f"could not verify sidecars for the run record at {record_dir}"
        ) from error
    for expected_name, artifact, stream in (
        (
            STDOUT_SIDECAR_NAME,
            record.outputs.stdout,
            record.result.payload_outputs.stdout,
        ),
        (
            STDERR_SIDECAR_NAME,
            record.outputs.stderr,
            record.result.payload_outputs.stderr,
        ),
    ):
        if artifact.relative_path != Path(expected_name):
            raise RecordLoadError(
                f"run record at {record_dir} names an unexpected "
                f"{expected_name} artifact path"
            )
        try:
            directory.verify_sidecar(
                expected_name,
                expected_sidecar_hash=artifact.sha256,
                expected_head_length=stream.head_bytes,
                expected_tail_length=stream.tail_bytes,
            )
        except DocumentDirectoryError as error:
            raise RecordLoadError(
                f"sidecar {artifact.relative_path} at {record_dir} does not "
                "match its record"
            ) from error


__all__ = [
    "DirectoryRunStore",
    "FinalizableRun",
    "PreparedRun",
    "RunningRun",
]
