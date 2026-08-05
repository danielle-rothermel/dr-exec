"""Execution-local durable recording over the Document Directory.

dr-exec owns the canonical manifest model, the typed lifecycle handles,
the secret-safe projection of an execution result into durable evidence,
the receipt semantics, and strict load validation. The pinned
``dr_store.docdir`` primitive owns directory allocation, atomic durable
manifest replacement, sidecar streaming, truncation, and digests: sidecar
lengths and digests are read out of the finalized ``SidecarSummary`` and
are never recomputed here.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from dr_serialize import Jsonable, Sha256Digest, canonical_json_bytes
from dr_store.docdir import DocumentDirectory, SidecarSummary
from dr_store.errors import DocumentDirectoryError, ManifestReadError
from pydantic import TypeAdapter, ValidationError

from dr_exec._model import ContractModel
from dr_exec.errors import ExecutorFailure, RecordLoadError
from dr_exec.kinds import RecordState
from dr_exec.names import ExecutionId
from dr_exec.record import (
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
    RunningRecord,
    RunRecord,
    SignaledOutcome,
    SignaledOutcomeRecord,
    SpawnAbsentOutcome,
    SpawnAbsentOutcomeRecord,
    SpawnFailedOutcome,
    SpawnFailedOutcomeRecord,
)

# Persisted layout literals: the on-disk contract, never derived from
# module, class, or field names.
RECORD_DIRECTORY_PREFIX = "run"
MANIFEST_NAME = "record.json"
STDOUT_SIDECAR_NAME = "stdout.bin"
STDERR_SIDECAR_NAME = "stderr.bin"

_RUN_RECORD_ADAPTER: TypeAdapter[RunRecord] = TypeAdapter(RunRecord)


@dataclass(frozen=True, slots=True)
class PreparedRun:
    """A run whose complete declaration is durably recorded, pre-spawn."""

    execution_id: ExecutionId
    record_dir: Path


@dataclass(frozen=True, slots=True)
class RunningRun:
    """A run whose child spawned successfully and is durably recorded."""

    execution_id: ExecutionId
    record_dir: Path


type FinalizableRun = PreparedRun | RunningRun


def _manifest_payload(record: ContractModel, /) -> Jsonable:
    """Project a lifecycle record into its secret-safe wire payload.

    The explicit Pydantic JSON-mode projection is the secret-safe value;
    the primitive validates it as strict JSON and owns the canonical
    bytes it writes.
    """
    return cast("Jsonable", record.model_dump(mode="json"))


def _outcome_record(outcome: ExecutionOutcome, /) -> ExecutionOutcomeRecord:
    """Reduce a live outcome to its durable, secret-free evidence.

    The live outcome carries diagnostic strings derived from the
    declaration -- the missing executable's spelling, the spawn error
    message, protocol failure detail -- which are dropped here rather
    than filtered downstream, so no durable path can reintroduce them.
    """
    match outcome:
        case ExitedOutcome():
            return ExitedOutcomeRecord(exit_code=outcome.exit_code)
        case SignaledOutcome():
            return SignaledOutcomeRecord(signal_number=outcome.signal_number)
        case SpawnAbsentOutcome():
            return SpawnAbsentOutcomeRecord()
        case SpawnFailedOutcome():
            return SpawnFailedOutcomeRecord(errno=outcome.errno)
        case BudgetExceededOutcome():
            return BudgetExceededOutcomeRecord(axis=outcome.axis)
        case ProtocolFailedOutcome():
            return ProtocolFailedOutcomeRecord(
                failure_code=outcome.failure_code,
                accepted_output_count=outcome.accepted_output_count,
            )
        case CancelledOutcome():
            return CancelledOutcomeRecord()


def _attribution_record(
    attribution: ExecutionAttribution,
    /,
) -> ExecutionAttributionRecord:
    """Retain the attributed owner; drop its free-text diagnostic."""
    return ExecutionAttributionRecord(owner=attribution.owner)


def _retained_stream_record(
    stream: RetainedPayloadStream,
    summary: SidecarSummary,
    /,
) -> RetainedPayloadStreamRecord:
    """Describe one retained stream from its finalized sidecar summary.

    The writer stores exactly the bytes it was offered, so the summary's
    segment lengths restate the caller's own head/tail split rather than
    deriving it. The summary's digest is the primitive's independent
    contribution: it is what pins the stored bytes. Produced and dropped
    counts are the executor's retention accounting, which the writer
    never observes.
    """
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
    """Project one result into durable, secret-free execution evidence.

    Accepted protocol outputs are retained inline and complete; a digest
    never replaces them.
    """
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
    """Reference one stored sidecar by its primitive-reported digest."""
    return OutputArtifactRecord(
        relative_path=Path(name),
        size_bytes=summary.head_length + summary.tail_length,
        sha256=Sha256Digest(summary.digest),
    )


def _recording_failure(
    operation: str,
    error: Exception,
    /,
) -> RecordingFailure:
    """Describe one recording failure without naming a rejected value."""
    cause = error.__cause__
    errno = getattr(cause, "errno", None)
    return RecordingFailure(
        operation=operation,
        errno=errno if isinstance(errno, int) else None,
        detail=type(error).__name__,
    )


def _write_sidecar(
    directory: DocumentDirectory,
    name: str,
    stream: RetainedPayloadStream,
    /,
) -> SidecarSummary:
    """Store one stream's already-retained bytes, head segment first.

    Retention against the declared budget already happened upstream, so
    the caps are the lengths of the very bytes being written and the
    writer never drops one. They exist to keep the two segments the
    reader must separate from collapsing into one, not to bound
    anything the caller has not already bounded.
    """
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
    """One execution-local run directory per attempt, under ``root``.

    Every successfully published lifecycle state is valid and complete.
    Recording degradation after the attempt starts is reported in the
    receipt with the latest valid lifecycle state and never replaces the
    execution outcome.
    """

    root: Path

    def prepare(
        self,
        record: PreparedRecord,
        /,
    ) -> PreparedRun:
        """Allocate a run directory and publish the ``prepared`` manifest.

        Prepare failure prevents the spawn, so it raises rather than
        degrading: no attempt has started that a receipt could describe.
        The primitive's typed errors are translated here, so the store
        boundary raises only dr-exec's own error taxonomy.
        """
        with _executor_failure("prepare the run record"):
            directory = DocumentDirectory.allocate(
                self.root,
                prefix=RECORD_DIRECTORY_PREFIX,
                manifest_name=MANIFEST_NAME,
            )
            directory.publish(_manifest_payload(record))
        return PreparedRun(
            execution_id=record.declaration.execution_id,
            record_dir=directory.path,
        )

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        """Publish the process-bearing ``running`` manifest after a spawn.

        This is a post-start publication, so its failure must not replace
        the execution outcome. The ``RunStore`` contract returns a
        lifecycle handle here rather than a receipt, so failure surfaces
        as ``ExecutorFailure``; the engine converts it into a degraded
        receipt naming ``prepared`` as the latest valid state, which
        remains intact on disk because publication is atomic.
        """
        with _executor_failure("publish the running run record"):
            prepared = self._load_prepared(prepared_run.record_dir)
            _directory(prepared_run.record_dir).publish(
                _manifest_payload(
                    RunningRecord(
                        header=prepared.header,
                        declaration=prepared.declaration,
                        process=process,
                    )
                )
            )
        return RunningRun(
            execution_id=prepared_run.execution_id,
            record_dir=prepared_run.record_dir,
        )

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        """Flush retained sidecars, then publish the ``finalized`` manifest.

        A recognized pre-child outcome finalizes directly from a prepared
        handle. Any failure on this path yields a degraded receipt naming
        the latest lifecycle state that remains valid on disk; the
        execution outcome it was asked to record is never replaced.
        """
        try:
            self._publish_finalized(run, result)
        except (DocumentDirectoryError, RecordLoadError) as error:
            return DegradedRecordReceipt(
                execution_id=run.execution_id,
                record_dir=run.record_dir,
                latest_state=_durable_state(run),
                failures=(_recording_failure("finalize", error),),
            )
        return CompleteRecordReceipt(
            execution_id=run.execution_id,
            record_dir=run.record_dir,
        )

    def load(
        self,
        record_dir: Path,
        /,
    ) -> RunRecord:
        """Validate and return the run record stored at ``record_dir``.

        Malformed manifest bytes, an invalid lifecycle model, an unsafe
        artifact path, and a sidecar length or digest mismatch all raise
        ``RecordLoadError``, preserving the originating shared decoding,
        verification, or validation error as ``__cause__``. An incomplete
        record loads as the incomplete state it is; success is never
        inferred from sidecars present on disk.
        """
        record = _load_record(record_dir)
        if isinstance(record, FinalizedRecord):
            _verify_sidecars(record_dir, record)
        return record

    def _publish_finalized(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> None:
        """Write both sidecars, then publish the manifest naming them.

        A ``SidecarSummary`` exists only after its writer flushed, so a
        manifest carrying sidecar digests structurally cannot precede
        the sidecar bytes it describes.
        """
        record = _load_record(run.record_dir)
        if isinstance(record, FinalizedRecord):
            raise RecordLoadError(
                f"run record at {run.record_dir} is already finalized"
            )
        directory = _directory(run.record_dir)
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

    def _load_prepared(self, record_dir: Path, /) -> PreparedRecord:
        record = _load_record(record_dir)
        if not isinstance(record, PreparedRecord):
            raise RecordLoadError(
                f"run record at {record_dir} is not in the prepared state"
            )
        return record


@contextmanager
def _executor_failure(operation: str, /) -> Iterator[None]:
    """Translate the primitive's and the read path's errors into one type.

    dr-exec owns the error taxonomy at the ``RunStore`` boundary, so
    neither the Document Directory's exception types nor the read path's
    ``RecordLoadError`` escape an operation documented to fail as
    ``ExecutorFailure``. The original error is preserved as ``__cause__``.
    """
    try:
        yield
    except (DocumentDirectoryError, RecordLoadError) as error:
        raise ExecutorFailure(f"could not {operation}") from error


def _durable_state(run: FinalizableRun, /) -> RecordState:
    """Name the latest lifecycle state still valid on disk.

    A handle proves only a lower bound on what was published, so the
    record itself is the authority: a degraded receipt must not
    understate a finalized record that is durably present. When no valid
    record can be read at all, the handle's own state is the closest
    remaining claim, and this derivation never raises out of the
    degradation path it describes.
    """
    try:
        return _load_record(run.record_dir).state
    except RecordLoadError:
        return (
            RecordState.PREPARED
            if isinstance(run, PreparedRun)
            else RecordState.RUNNING
        )


def _directory(record_dir: Path, /) -> DocumentDirectory:
    return DocumentDirectory(record_dir, MANIFEST_NAME)


def _load_record(record_dir: Path, /) -> RunRecord:
    """Read, then strictly validate, the manifest at ``record_dir``.

    The primitive returns the decoded payload rather than the bytes it
    verified, so the bytes handed to strict JSON-mode validation are a
    canonical re-encoding of that payload. Their equality with the
    stored bytes rests on the two pinned packages sharing one canonical
    profile, which is a cross-package assumption pinned by test rather
    than a property re-derived here. The decoded ``Jsonable`` itself
    never reaches Pydantic; dr-exec then owns lifecycle meaning.

    This read is unbounded in v1: the primitive reads the manifest whole
    and applies no byte or depth limit, and ``DirectoryRunStore`` takes
    no self-budgets, so the ``manifest_bytes`` axis is not enforced here.
    """
    try:
        payload = DocumentDirectory.read_manifest(
            record_dir,
            manifest_name=MANIFEST_NAME,
        )
    except ManifestReadError as error:
        raise RecordLoadError(
            f"could not read the run record at {record_dir}"
        ) from error
    try:
        return _RUN_RECORD_ADAPTER.validate_json(
            canonical_json_bytes(payload), strict=True
        )
    except ValidationError as error:
        raise RecordLoadError(
            f"run record at {record_dir} is not a valid lifecycle record"
        ) from error


def _verify_sidecars(record_dir: Path, record: FinalizedRecord, /) -> None:
    """Check every referenced sidecar against the manifest's evidence.

    The digest pins the stored bytes exactly. A stored sidecar carries no
    segment boundary, so the two declared segment lengths are verifiable
    only as their sum: the head/tail split is a manifest assertion, not a
    property the stored bytes can confirm. ``relative_path`` is already
    validated normalized and relative, so it can only name a child of
    the run directory.
    """
    for artifact, stream in (
        (record.outputs.stdout, record.result.payload_outputs.stdout),
        (record.outputs.stderr, record.result.payload_outputs.stderr),
    ):
        try:
            DocumentDirectory.verify_sidecar(
                record_dir / artifact.relative_path,
                expected_digest=artifact.sha256,
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
