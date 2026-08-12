from __future__ import annotations

import errno
import json
import os
from base64 import urlsafe_b64encode
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from uuid import UUID, uuid4

import pytest
from dr_serialize import (
    IdentityDocument,
    Sha256Digest,
    build_identity_document,
    canonical_identity_json_bytes,
    canonical_json_bytes,
)
from dr_store import (
    AllocationError,
    DocumentDirectory,
    DocumentDirectoryError,
    DocumentPublishError,
    ManifestPublishError,
    PublicationStage,
    ReplacementState,
    SidecarSummary,
    SidecarWriter,
)
from pydantic import ValidationError

import dr_exec.recording.store
from dr_exec import (
    AttemptId,
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    CompleteRecordReceipt,
    ContainmentProfile,
    DegradedRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionAttribution,
    ExecutionId,
    ExecutionJob,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExecutionTarget,
    ExecutionTargetRecord,
    ExecutorFailure,
    ExitedOutcome,
    FailureOwner,
    FinalizedRecord,
    IsolatedHostPythonRuntime,
    JobId,
    OutcomeKind,
    OutputArtifactRecord,
    PayloadOutputs,
    PreparedRecord,
    PreparedRun,
    ProcessRecord,
    ProtocolFailedOutcome,
    ProtocolFailureCode,
    RecordLoadError,
    RecordReceiptKind,
    RecordState,
    RetainedPayloadStream,
    RunDeclaration,
    RunningRecord,
    RunningRun,
    RunRecordHeader,
    RunRecordReference,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
    TrustedCommandTargetRecord,
    TrustedPythonTargetRecord,
)
from dr_exec.declarations.models import (
    ExecutorSelfBudgets,
    TrustedCommandTarget,
    TrustedPythonTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.execution.engine import _target_of
from dr_exec.recording.identity import (
    _build_env_grant_record,
    _build_executor_config_identity,
    _build_executor_identity,
    _canonical_declaration_digest,
)
from dr_exec.recording.provenance import ExecutorSourceSnapshot
from dr_exec.recording.references import record_reference_for_job
from dr_exec.recording.store import (
    MANIFEST_NAME,
    RECORD_DIRECTORY_PREFIX,
    STDERR_SIDECAR_NAME,
    STDOUT_SIDECAR_NAME,
    STRUCTURAL_MANIFEST_BYTE_CEILING,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dr_serialize import Jsonable

SECRET_ARGUMENT = 'hunter2-argv-"secret"'
SECRET_STDIN = b'hunter2-stdin-"secret"'
SECRET_ENV_VALUE = 'hunter2-env-"secret"'
SECRET_EXECUTABLE = '/nonexistent/hunter2-"executable-secret"'
SECRET_ERROR_DETAIL = 'hunter2-diagnostic-"secret"'
TRUSTED_ARGV_CANARY = 'trusted-argv-"\\\n-canary'
TRUSTED_STDIN_CANARY = b'trusted-stdin-"\\\n-canary'
TRUSTED_ENV_CANARY = 'trusted-env-"\\\n-canary'
UNTRUSTED_ARGV_CANARY = 'untrusted-argv-"\\\n-canary'
UNTRUSTED_STDIN_CANARY = b'untrusted-stdin-"\\\n-canary'
UNTRUSTED_ENV_CANARY = 'untrusted-env-"\\\n-canary'
PYTHON_DRIVER_CANARY = 'python-driver-"\\\n-canary'
PYTHON_REQUEST_CANARY = 'python-request-"\\\n-canary'
PYTHON_ENV_CANARY = 'python-env-"\\\n-canary'
SPAWN_ABSENT_CANARY = '/absent/spawn-"\\\n-canary'
SPAWN_FAILURE_CANARY = 'spawn-failure-"\\\n-canary'
PROTOCOL_FAILURE_CANARY = 'protocol-failure-"\\\n-canary'
ATTRIBUTION_CANARY = 'attribution-"\\\n-canary'
STDOUT_HEAD_CANARY = b'stdout-head-"\\\n-canary'
STDOUT_TAIL_CANARY = b'stdout-tail-"\\\n-canary'
STDERR_HEAD_CANARY = b'stderr-head-"\\\n-canary'
STDERR_TAIL_CANARY = b'stderr-tail-"\\\n-canary'
PREPARED_AT = datetime(2026, 8, 4, 12, 0, 0, 500000, tzinfo=UTC)
STARTED_AT = datetime(2026, 8, 4, 12, 0, 1, 500000, tzinfo=UTC)
FINISHED_AT = datetime(2026, 8, 4, 12, 0, 2, 500000, tzinfo=UTC)

pytestmark = pytest.mark.integration


@pytest.fixture
def store(tmp_path: Path) -> DirectoryRunStore:
    root = tmp_path / "records"
    root.mkdir()
    return DirectoryRunStore(root=root)


def _record_dir(
    store: DirectoryRunStore, run: PreparedRun | RunningRun, /
) -> Path:
    return store._record_dir(run.reference)


def _header() -> RunRecordHeader:
    return RunRecordHeader(
        executor_identity=_build_executor_identity(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit=None,
                source_state="unknown",
                session_id=str(UUID(int=7)),
            )
        ),
        executor_config_identity=_build_executor_config_identity(
            ExecutorSelfBudgets.unbudgeted()
        ),
        prepared_at=PREPARED_AT,
    )


def _declaration(
    execution_id: ExecutionId,
    *,
    target_record: ExecutionTargetRecord | None = None,
    env: EnvGrant | None = None,
) -> RunDeclaration:
    target = TrustedCommandTarget(
        argv=("/bin/echo", SECRET_ARGUMENT),
        stdin=SECRET_STDIN,
    )
    return RunDeclaration(
        execution_id=execution_id,
        target=target_record
        or TrustedCommandTargetRecord(
            canonical_declaration_sha256=_canonical_declaration_digest(target)
        ),
        env=_build_env_grant_record(
            EnvGrant.fixed({"TOKEN": SECRET_ENV_VALUE}) if env is None else env
        ),
        budgets=Budgets.unbudgeted(),
    )


def _prepared_record(
    execution_id: ExecutionId,
    *,
    target_record: ExecutionTargetRecord | None = None,
    env: EnvGrant | None = None,
) -> PreparedRecord:
    return PreparedRecord(
        header=_header(),
        declaration=_declaration(
            execution_id,
            target_record=target_record,
            env=env,
        ),
    )


def _producer_prepared_record(
    execution_id: ExecutionId,
    target: ExecutionTarget,
    env: EnvGrant,
    runtime: IsolatedHostPythonRuntime,
) -> PreparedRecord:
    job = ExecutionJob(
        job_id=execution_id.job_id,
        target=target,
        env=env,
        budgets=Budgets.unbudgeted(),
    )
    target_record = _target_of(job, runtime).record
    return _prepared_record(
        execution_id,
        target_record=target_record,
        env=job.env,
    )


def _stream(
    head: bytes = b"",
    tail: bytes = b"",
    dropped_bytes: int = 0,
) -> RetainedPayloadStream:
    return RetainedPayloadStream(
        head=head,
        tail=tail,
        produced_bytes=len(head) + len(tail) + dropped_bytes,
        dropped_bytes=dropped_bytes,
    )


def _result(
    execution_id: ExecutionId,
    *,
    outcome: ExecutionOutcome | None = None,
    attribution: ExecutionAttribution | None = None,
    stdout: RetainedPayloadStream | None = None,
    stderr: RetainedPayloadStream | None = None,
    protocol_outputs: tuple[IdentityDocument, ...] = (),
) -> ExecutionResult:
    return ExecutionResult(
        execution_id=execution_id,
        outcome=outcome if outcome is not None else ExitedOutcome(exit_code=0),
        attribution=attribution
        or ExecutionAttribution(owner=FailureOwner.NONE),
        protocol_outputs=protocol_outputs,
        payload_outputs=PayloadOutputs(
            stdout=stdout if stdout is not None else _stream(),
            stderr=stderr if stderr is not None else _stream(),
        ),
        measurements=ExecutionMeasurements(
            started_at=STARTED_AT,
            finished_at=FINISHED_AT,
            duration_ns=1_000_000_000,
            teardown_duration_ns=0,
            input_bytes=len(SECRET_STDIN),
            protocol_bytes_received=0,
        ),
    )


def _manifest_bytes(record_dir: Path) -> bytes:
    return (record_dir / MANIFEST_NAME).read_bytes()


def _container_depth(value: Jsonable, /) -> int:
    if isinstance(value, dict):
        return 1 + max(
            (_container_depth(item) for item in value.values()), default=0
        )
    if isinstance(value, list):
        return 1 + max((_container_depth(item) for item in value), default=0)
    return 0


class _FinalizeFaultWriter:
    def __init__(
        self, writer: SidecarWriter, *, fault: bool, errno: int
    ) -> None:
        self._writer = writer
        self._fault = fault
        self._errno = errno

    def write(self, chunk: bytes, /) -> None:
        self._writer.write(chunk)

    def finalize(self) -> SidecarSummary:
        summary = self._writer.finalize()
        if self._fault:
            try:
                raise OSError(self._errno, os.strerror(self._errno))
            except OSError as error:
                raise AllocationError(
                    "injected sidecar finalization fault"
                ) from error
        return summary


def _install_finalization_fault(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stage: Literal[
        "stdout_finalize",
        "stderr_open",
        "stderr_finalize",
        "manifest_publish",
    ],
    error_number: int,
) -> None:
    original_open = DocumentDirectory.open_sidecar

    def open_sidecar(
        directory: DocumentDirectory,
        name: str,
        *,
        head_cap: int | None = None,
        tail_cap: int | None = None,
    ) -> SidecarWriter | _FinalizeFaultWriter:
        if stage == "stderr_open" and name == STDERR_SIDECAR_NAME:
            try:
                raise OSError(error_number, os.strerror(error_number))
            except OSError as error:
                raise AllocationError("injected sidecar open fault") from error
        writer = original_open(
            directory,
            name,
            head_cap=head_cap,
            tail_cap=tail_cap,
        )
        return _FinalizeFaultWriter(
            writer,
            fault=(stage == "stdout_finalize" and name == STDOUT_SIDECAR_NAME)
            or (stage == "stderr_finalize" and name == STDERR_SIDECAR_NAME),
            errno=error_number,
        )

    if stage == "manifest_publish":

        def publish(_directory: DocumentDirectory, _manifest: object) -> None:
            try:
                raise OSError(error_number, os.strerror(error_number))
            except OSError as error:
                try:
                    raise DocumentPublishError(
                        _directory.path / MANIFEST_NAME,
                        PublicationStage.WRITE_TEMP,
                        replacement_state=ReplacementState.NOT_REPLACED,
                    ) from error
                except DocumentPublishError as document_error:
                    raise ManifestPublishError(
                        _directory.path / MANIFEST_NAME,
                        PublicationStage.WRITE_TEMP,
                        replacement_state=ReplacementState.NOT_REPLACED,
                    ) from document_error

        monkeypatch.setattr(DocumentDirectory, "publish", publish)
    else:
        monkeypatch.setattr(DocumentDirectory, "open_sidecar", open_sidecar)


def _recoverable_encodings(secret: str | bytes, /) -> frozenset[bytes]:
    raw = secret.encode() if isinstance(secret, str) else secret
    text = raw.decode()
    identity = build_identity_document(
        schema="dr_exec.secret_canary",
        schema_version=1,
        payload={"secret": text},
    )
    return frozenset(
        {
            raw,
            urlsafe_b64encode(raw),
            json.dumps(text, ensure_ascii=True)[1:-1].encode(),
            canonical_identity_json_bytes(identity),
        }
    )


def _assert_no_recoverable_canaries(
    manifest: bytes,
    canaries: tuple[str | bytes, ...],
    /,
) -> None:
    for canary in canaries:
        for representation in _recoverable_encodings(canary):
            assert representation not in manifest


def _leaf_key_paths(value: object, prefix: str = "") -> frozenset[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(child, dict) and child:
                paths.update(_leaf_key_paths(child, path))
            elif isinstance(child, list):
                paths.add(path)
                for member in child:
                    if isinstance(member, dict):
                        paths.update(_leaf_key_paths(member, f"{path}[]"))
            else:
                paths.add(path)
    return frozenset(paths)


_PREPARED_LEAF_KEY_PATHS = frozenset(
    [
        "declaration.budgets.cpu_time.kind",
        "declaration.budgets.disk_bytes.kind",
        "declaration.budgets.file_size_bytes.kind",
        "declaration.budgets.input_bytes.kind",
        "declaration.budgets.memory_bytes.kind",
        "declaration.budgets.open_file_count.kind",
        "declaration.budgets.payload_output.kind",
        "declaration.budgets.process_count.kind",
        "declaration.budgets.wall_time.kind",
        "declaration.env.canonical_values_sha256",
        "declaration.env.excluded_var_names",
        "declaration.env.kind",
        "declaration.env.var_names",
        "declaration.execution_id.attempt_id",
        "declaration.execution_id.job_id",
        "declaration.target.canonical_declaration_sha256",
        "declaration.target.kind",
        "header.executor_config_identity.payload.join_time.kind",
        "header.executor_config_identity.payload.json_depth.kind",
        "header.executor_config_identity.payload.protocol_frame_bytes.kind",
        "header.executor_config_identity.payload.protocol_output_count.kind",
        "header.executor_config_identity.payload.protocol_total_bytes.kind",
        "header.executor_config_identity.payload.startup_time.kind",
        "header.executor_config_identity.payload.termination_time.kind",
        "header.executor_config_identity.schema",
        "header.executor_config_identity.schema_version",
        "header.executor_identity.payload.kind",
        "header.executor_identity.payload.package_version",
        "header.executor_identity.payload.session_id",
        "header.executor_identity.payload.source_commit",
        "header.executor_identity.payload.source_state",
        "header.executor_identity.schema",
        "header.executor_identity.schema_version",
        "header.prepared_at",
        "header.schema_version",
        "state",
    ]
)

_FINALIZED_LEAF_KEY_PATHS = _PREPARED_LEAF_KEY_PATHS | frozenset(
    [
        "outputs.stderr.relative_path",
        "outputs.stderr.sha256",
        "outputs.stderr.size_bytes",
        "outputs.stdout.relative_path",
        "outputs.stdout.sha256",
        "outputs.stdout.size_bytes",
        "result.attribution.detail",
        "result.attribution.owner",
        "result.execution_id.attempt_id",
        "result.execution_id.job_id",
        "result.measurements.duration_ns",
        "result.measurements.finished_at",
        "result.measurements.input_bytes",
        "result.measurements.protocol_bytes_received",
        "result.measurements.started_at",
        "result.measurements.teardown_duration_ns",
        "result.outcome.exit_code",
        "result.outcome.kind",
        "result.payload_outputs.stderr.dropped_bytes",
        "result.payload_outputs.stderr.head_bytes",
        "result.payload_outputs.stderr.produced_bytes",
        "result.payload_outputs.stderr.tail_bytes",
        "result.payload_outputs.stdout.dropped_bytes",
        "result.payload_outputs.stdout.head_bytes",
        "result.payload_outputs.stdout.produced_bytes",
        "result.payload_outputs.stdout.tail_bytes",
        "result.protocol_outputs",
    ]
)

_TRUSTED_TARGET_LEAF_KEY_PATHS = frozenset(
    {"kind", "canonical_declaration_sha256"}
)
_UNTRUSTED_COMMAND_TARGET_LEAF_KEY_PATHS = _TRUSTED_TARGET_LEAF_KEY_PATHS | {
    "containment_profile"
}
_UNTRUSTED_PYTHON_TARGET_LEAF_KEY_PATHS = frozenset(
    {
        "kind",
        "canonical_declaration_sha256",
        "request_id_sha256",
        "containment_profile",
        "runtime.kind",
        "runtime.resolved_executable",
        "runtime.id_doc.schema",
        "runtime.id_doc.schema_version",
        "runtime.id_doc.payload.kind",
        "runtime.id_doc.payload.resolved_executable",
        "runtime.id_doc.payload.implementation",
        "runtime.id_doc.payload.python_version",
        "runtime.id_doc.payload.cache_tag",
        "runtime.id_doc.payload.platform",
    }
)
_TRUSTED_PYTHON_TARGET_LEAF_KEY_PATHS = (
    _UNTRUSTED_PYTHON_TARGET_LEAF_KEY_PATHS - {"containment_profile"}
)


def _with_target_leaf_key_paths(
    record_paths: frozenset[str],
    target_paths: frozenset[str],
    /,
) -> frozenset[str]:
    return frozenset(
        path
        for path in record_paths
        if not path.startswith("declaration.target.")
    ) | frozenset(f"declaration.target.{path}" for path in target_paths)


def _with_outcome_leaf_key_paths(
    record_paths: frozenset[str],
    outcome_paths: frozenset[str],
    /,
) -> frozenset[str]:
    return frozenset(
        path for path in record_paths if not path.startswith("result.outcome.")
    ) | frozenset(f"result.outcome.{path}" for path in outcome_paths)


def test_prepare_publishes_a_complete_prepared_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_record = _prepared_record(execution_id)
    run = store.prepare(prepared_record)

    assert run.execution_id == execution_id
    assert _record_dir(store, run).parent == store.root
    assert _record_dir(store, run).name.startswith(
        f"{RECORD_DIRECTORY_PREFIX}-"
    )
    assert store.load(run.reference) == prepared_record


def test_prepare_for_the_same_job_id_targets_the_same_record_directory(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    first = store.prepare(_prepared_record(execution_id))

    with pytest.raises(ExecutorFailure, match="prepare the run record"):
        store.prepare(_prepared_record(execution_id))

    assert (
        first.reference.record_id
        == record_reference_for_job(execution_id.job_id).record_id
    )


def test_a_failed_prepare_reclaims_its_orphan_and_allows_retry(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = record_reference_for_job(execution_id.job_id)
    record_dir = (
        store.root / f"{RECORD_DIRECTORY_PREFIX}-{reference.record_id}"
    )
    _install_finalization_fault(
        monkeypatch,
        stage="manifest_publish",
        error_number=errno.ENOSPC,
    )

    with pytest.raises(ExecutorFailure, match="prepare the run record"):
        store.prepare(_prepared_record(execution_id))

    assert not record_dir.exists()

    monkeypatch.undo()

    run = store.prepare(_prepared_record(execution_id))
    assert run.reference == reference
    assert store.load(run.reference).state == RecordState.PREPARED


def test_an_empty_orphan_at_a_deterministic_path_is_reclaimed_on_prepare(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    reference = record_reference_for_job(execution_id.job_id)
    record_dir = (
        store.root / f"{RECORD_DIRECTORY_PREFIX}-{reference.record_id}"
    )
    record_dir.mkdir()

    run = store.prepare(_prepared_record(execution_id))

    assert run.reference == reference
    assert store.load(run.reference).state == RecordState.PREPARED


def test_a_relative_root_is_normalized_for_the_store_and_allocated_run(
    tmp_path: Path,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    relative_root = Path("records")
    relative_root.mkdir()
    store = DirectoryRunStore(root=relative_root)

    run = store.prepare(_prepared_record(execution_id))

    assert store.root == tmp_path / relative_root
    assert _record_dir(store, run).parent == store.root


def test_load_rejects_a_reference_owned_by_another_root(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    tmp_path: Path,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    other_root = tmp_path / "other-records"
    other_root.mkdir()

    with pytest.raises(RecordLoadError, match="could not resolve"):
        DirectoryRunStore(root=other_root).load(run.reference)


def test_load_rejects_nonreference_paths_without_fallback(
    store: DirectoryRunStore,
) -> None:
    with pytest.raises(RecordLoadError, match="unsupported"):
        store.load(cast("RunRecordReference", store.root))


def test_load_rejects_an_unsupported_backend_without_fallback(
    store: DirectoryRunStore,
) -> None:
    reference = RunRecordReference.model_construct(
        backend="other", record_id=UUID(int=1)
    )

    with pytest.raises(RecordLoadError, match="unsupported"):
        store.load(reference)


def test_load_rejects_a_malformed_identifier_without_path_interpretation(
    store: DirectoryRunStore,
) -> None:
    reference = RunRecordReference.model_construct(
        backend="directory", record_id="../outside"
    )

    with pytest.raises(RecordLoadError, match="malformed"):
        store.load(reference)


def test_mark_running_publishes_the_process_bearing_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_record = _prepared_record(execution_id)
    prepared_run = store.prepare(prepared_record)
    process = ProcessRecord(pid=4242, started_at=STARTED_AT)

    running_run = store.mark_running(prepared_run, process)

    assert running_run.reference == prepared_run.reference
    loaded = store.load(running_run.reference)
    assert isinstance(loaded, RunningRecord)
    assert loaded.process == process
    assert loaded.declaration == prepared_record.declaration
    assert loaded.header == prepared_record.header


def test_finalize_from_running_completes_the_lifecycle(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )

    receipt = store.finalize(running_run, _result(execution_id))

    assert isinstance(receipt, CompleteRecordReceipt)
    assert receipt.latest_state == RecordState.FINALIZED
    assert receipt.execution_id == execution_id
    assert isinstance(store.load(running_run.reference), FinalizedRecord)


def test_a_recognized_pre_child_outcome_finalizes_from_prepared(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))

    receipt = store.finalize(
        prepared_run,
        _result(execution_id, outcome=CancelledOutcome()),
    )

    assert isinstance(receipt, CompleteRecordReceipt)
    finalized = store.load(prepared_run.reference)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.outcome.kind == CancelledOutcome().kind


def test_mark_running_publishes_from_the_handle_without_reloading(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    (_record_dir(store, prepared_run) / MANIFEST_NAME).write_bytes(b"{")

    running_run = store.mark_running(
        prepared_run,
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )

    assert running_run.header == prepared_run.header
    assert running_run.declaration == prepared_run.declaration
    loaded = store.load(running_run.reference)
    assert isinstance(loaded, RunningRecord)
    assert loaded.process.pid == 4242


def test_finalizing_twice_degrades_rather_than_replacing_the_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    store.finalize(prepared_run, _result(execution_id, stdout=_stream(b"a")))
    first = _manifest_bytes(_record_dir(store, prepared_run))

    receipt = store.finalize(
        prepared_run, _result(execution_id, stdout=_stream(b"bbbb"))
    )

    assert isinstance(receipt, DegradedRecordReceipt)
    assert _manifest_bytes(_record_dir(store, prepared_run)) == first
    # The handle proves only a lower bound; the receipt must not
    # understate the finalized record that is durably on disk.
    assert receipt.latest_state == RecordState.FINALIZED
    assert receipt.latest_state == store.load(prepared_run.reference).state


@pytest.mark.parametrize(
    ("commit_state", "expected_state"),
    [
        pytest.param(
            RecordState.PREPARED,
            RecordState.PREPARED,
            marks=(pytest.mark.subprocess, pytest.mark.serial_fork),
            id="prepared",
        ),
        pytest.param(
            RecordState.RUNNING,
            RecordState.RUNNING,
            marks=(pytest.mark.subprocess, pytest.mark.serial_fork),
            id="running",
        ),
    ],
)
def test_a_record_committed_before_parent_death_recovers_as_incomplete(
    tmp_path: Path,
    execution_id: ExecutionId,
    commit_state: RecordState,
    expected_state: RecordState,
) -> None:
    root = tmp_path / "records"
    root.mkdir()
    handoff = tmp_path / "record-dir.txt"
    read_end, write_end = os.pipe()

    child_pid = os.fork()
    if child_pid == 0:  # pragma: no cover - the child never returns
        os.close(read_end)
        try:
            store = DirectoryRunStore(root=root)
            run = store.prepare(_prepared_record(execution_id))
            if commit_state is RecordState.RUNNING:
                store.mark_running(
                    run,
                    ProcessRecord(pid=os.getpid(), started_at=STARTED_AT),
                )
            handoff.write_text(str(run.reference.record_id))
            os.write(write_end, b"committed")
        finally:
            os._exit(0)

    os.close(write_end)
    try:
        assert os.read(read_end, len(b"committed")) == b"committed"
    finally:
        os.close(read_end)
    _, status = os.waitpid(child_pid, 0)
    assert os.WIFEXITED(status)

    reference = RunRecordReference(record_id=UUID(handoff.read_text()))
    recovered = DirectoryRunStore(root=root).load(reference)
    assert recovered.state == expected_state
    assert recovered.state is not RecordState.FINALIZED


def test_recovery_never_infers_completion_from_sidecars_on_disk(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    (_record_dir(store, running_run) / STDOUT_SIDECAR_NAME).write_bytes(
        b"partial"
    )
    (_record_dir(store, running_run) / STDERR_SIDECAR_NAME).write_bytes(
        b"partial"
    )

    recovered = store.load(running_run.reference)

    assert recovered.state == RecordState.RUNNING


def test_finalization_stores_digest_matching_retrievable_sidecars(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    stdout = _stream(head=b"out-head", tail=b"out-tail", dropped_bytes=5)
    stderr = _stream(head=b"err-head")

    store.finalize(
        prepared_run, _result(execution_id, stdout=stdout, stderr=stderr)
    )

    finalized = store.load(prepared_run.reference)
    assert isinstance(finalized, FinalizedRecord)
    stored_stdout = (
        _record_dir(store, prepared_run) / STDOUT_SIDECAR_NAME
    ).read_bytes()
    assert stored_stdout == stdout.head + stdout.tail
    assert finalized.outputs.stdout.size_bytes == len(stored_stdout)
    assert finalized.outputs.stdout.relative_path == Path(STDOUT_SIDECAR_NAME)
    assert finalized.outputs.stderr.relative_path == Path(STDERR_SIDECAR_NAME)


def test_a_reference_recovers_complete_verified_artifact_bytes(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    expected = b"descriptor-pinned artifact"
    store.finalize(run, _result(execution_id, stdout=_stream(head=expected)))
    record = store.load(run.reference)
    assert isinstance(record, FinalizedRecord)

    def path_read_is_not_the_artifact_path(*_: object, **__: object) -> bytes:
        raise AssertionError("artifact reads must not use Path.read_bytes")

    monkeypatch.setattr(Path, "read_bytes", path_read_is_not_the_artifact_path)

    assert (
        store.read_artifact(
            run.reference,
            record.outputs.stdout,
            max_bytes=len(expected),
        )
        == expected
    )


def test_artifact_size_is_preflighted_against_the_caller_bound(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id, stdout=_stream(head=b"data")))
    record = store.load(run.reference)
    assert isinstance(record, FinalizedRecord)
    original_open = os.open

    def unexpected_artifact_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if path == STDOUT_SIDECAR_NAME:
            raise AssertionError("oversized declared artifact was opened")
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", unexpected_artifact_open)

    with pytest.raises(RecordLoadError, match="read limit"):
        store.read_artifact(
            run.reference,
            record.outputs.stdout,
            max_bytes=record.outputs.stdout.size_bytes - 1,
        )


def test_artifact_reads_require_a_finalized_owned_artifact(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    foreign = OutputArtifactRecord(
        relative_path=Path(STDOUT_SIDECAR_NAME),
        size_bytes=0,
        sha256=Sha256Digest("0" * 64),
    )

    with pytest.raises(RecordLoadError, match="not finalized"):
        store.read_artifact(run.reference, foreign, max_bytes=0)

    store.finalize(run, _result(execution_id))
    with pytest.raises(RecordLoadError, match="not owned"):
        store.read_artifact(run.reference, foreign, max_bytes=0)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda path: path.write_bytes(b"corrupt!"), id="digest"),
        pytest.param(lambda path: path.write_bytes(b"short"), id="truncated"),
        pytest.param(lambda path: path.unlink(), id="missing"),
    ],
)
def test_artifact_reads_return_no_bytes_for_corrupt_or_missing_data(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    mutate: Callable[[Path], object],
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    expected = b"original"
    store.finalize(run, _result(execution_id, stdout=_stream(head=expected)))
    record = store.load(run.reference)
    assert isinstance(record, FinalizedRecord)
    mutate(_record_dir(store, run) / STDOUT_SIDECAR_NAME)

    with pytest.raises(RecordLoadError):
        store.read_artifact(
            run.reference,
            record.outputs.stdout,
            max_bytes=len(expected),
        )


def test_artifact_reads_reject_a_no_follow_symlink(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    tmp_path: Path,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    expected = b"original"
    store.finalize(run, _result(execution_id, stdout=_stream(head=expected)))
    record = store.load(run.reference)
    assert isinstance(record, FinalizedRecord)
    artifact_path = _record_dir(store, run) / STDOUT_SIDECAR_NAME
    external = tmp_path / "external.bin"
    external.write_bytes(expected)
    artifact_path.unlink()
    artifact_path.symlink_to(external)

    with pytest.raises(RecordLoadError):
        store.read_artifact(
            run.reference,
            record.outputs.stdout,
            max_bytes=len(expected),
        )


def test_head_and_tail_segments_recover_exactly_with_their_counts(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    stdout = _stream(head=b"HEAD", tail=b"TAILTAIL", dropped_bytes=17)

    store.finalize(prepared_run, _result(execution_id, stdout=stdout))

    finalized = store.load(prepared_run.reference)
    assert isinstance(finalized, FinalizedRecord)
    stream_record = finalized.result.payload_outputs.stdout
    assert stream_record.head_bytes == len(stdout.head)
    assert stream_record.tail_bytes == len(stdout.tail)
    assert stream_record.produced_bytes == stdout.produced_bytes
    assert stream_record.dropped_bytes == stdout.dropped_bytes
    stored = (
        _record_dir(store, prepared_run) / STDOUT_SIDECAR_NAME
    ).read_bytes()
    assert stored[: stream_record.head_bytes] == stdout.head
    assert stored[stream_record.head_bytes :] == stdout.tail


def test_accepted_protocol_outputs_stay_inline_and_complete(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    request_document: IdentityDocument,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    outputs = (request_document, request_document)

    store.finalize(
        prepared_run,
        _result(
            execution_id,
            outcome=ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
                failure_detail=SECRET_ERROR_DETAIL,
                accepted_output_count=len(outputs),
            ),
            protocol_outputs=outputs,
            attribution=ExecutionAttribution(owner=FailureOwner.PAYLOAD),
        ),
    )

    finalized = store.load(prepared_run.reference)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.protocol_outputs == outputs


@pytest.mark.parametrize(
    ("target", "env_value", "canaries", "expected_target_paths"),
    [
        pytest.param(
            TrustedCommandTarget(
                argv=("/bin/echo", TRUSTED_ARGV_CANARY),
                stdin=TRUSTED_STDIN_CANARY,
            ),
            TRUSTED_ENV_CANARY,
            (
                TRUSTED_ARGV_CANARY,
                TRUSTED_STDIN_CANARY,
                TRUSTED_ENV_CANARY,
            ),
            _TRUSTED_TARGET_LEAF_KEY_PATHS,
            id="trusted-command",
        ),
        pytest.param(
            UntrustedCommandTarget(
                argv=("/bin/echo", UNTRUSTED_ARGV_CANARY),
                stdin=UNTRUSTED_STDIN_CANARY,
                containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
            ),
            UNTRUSTED_ENV_CANARY,
            (
                UNTRUSTED_ARGV_CANARY,
                UNTRUSTED_STDIN_CANARY,
                UNTRUSTED_ENV_CANARY,
            ),
            _UNTRUSTED_COMMAND_TARGET_LEAF_KEY_PATHS,
            id="untrusted-command",
        ),
        pytest.param(
            TrustedPythonTarget(
                driver_source=PYTHON_DRIVER_CANARY,
                request=build_identity_document(
                    schema="dr_exec.secret_request",
                    schema_version=1,
                    payload={"secret": PYTHON_REQUEST_CANARY},
                ),
            ),
            PYTHON_ENV_CANARY,
            (
                PYTHON_DRIVER_CANARY,
                PYTHON_REQUEST_CANARY,
                PYTHON_ENV_CANARY,
            ),
            _TRUSTED_PYTHON_TARGET_LEAF_KEY_PATHS,
            id="trusted-python",
        ),
        pytest.param(
            UntrustedPythonTarget(
                driver_source=PYTHON_DRIVER_CANARY,
                request=build_identity_document(
                    schema="dr_exec.secret_request",
                    schema_version=1,
                    payload={"secret": PYTHON_REQUEST_CANARY},
                ),
                containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
            ),
            PYTHON_ENV_CANARY,
            (
                PYTHON_DRIVER_CANARY,
                PYTHON_REQUEST_CANARY,
                PYTHON_ENV_CANARY,
            ),
            _UNTRUSTED_PYTHON_TARGET_LEAF_KEY_PATHS,
            id="untrusted-python",
        ),
    ],
)
def test_each_target_producer_is_secret_free_across_the_lifecycle(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    host_runtime: IsolatedHostPythonRuntime,
    target: ExecutionTarget,
    env_value: str,
    canaries: tuple[str | bytes, ...],
    expected_target_paths: frozenset[str],
) -> None:
    prepared_record = _producer_prepared_record(
        execution_id,
        target,
        EnvGrant.fixed({"TOKEN": env_value}),
        host_runtime,
    )
    prepared_run = store.prepare(prepared_record)
    prepared_bytes = _manifest_bytes(_record_dir(store, prepared_run))
    running_run = store.mark_running(
        prepared_run,
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    running_bytes = _manifest_bytes(_record_dir(store, running_run))
    store.finalize(running_run, _result(execution_id))
    finalized_bytes = _manifest_bytes(_record_dir(store, running_run))

    expected_prepared_paths = _with_target_leaf_key_paths(
        _PREPARED_LEAF_KEY_PATHS,
        expected_target_paths,
    )
    expected_paths = (
        expected_prepared_paths,
        expected_prepared_paths | {"process.pid", "process.started_at"},
        _with_target_leaf_key_paths(
            _FINALIZED_LEAF_KEY_PATHS,
            expected_target_paths,
        ),
    )
    for manifest_bytes, paths in zip(
        (prepared_bytes, running_bytes, finalized_bytes),
        expected_paths,
        strict=True,
    ):
        _assert_no_recoverable_canaries(manifest_bytes, canaries)
        assert _leaf_key_paths(json.loads(manifest_bytes)) == paths

    prepared = json.loads(prepared_bytes)
    assert prepared["declaration"]["env"]["var_names"] == ["TOKEN"]
    assert len(prepared["declaration"]["env"]["canonical_values_sha256"]) == 64
    target_record = prepared_record.declaration.target
    if isinstance(target_record, TrustedPythonTargetRecord):
        assert "containment_profile" not in type(target_record).model_fields


@pytest.mark.parametrize(
    ("outcome", "expected_outcome_paths"),
    [
        pytest.param(
            ExitedOutcome(exit_code=0),
            frozenset({"kind", "exit_code"}),
            id="exited",
        ),
        pytest.param(
            SignaledOutcome(signal_number=9),
            frozenset({"kind", "signal_number"}),
            id="signaled",
        ),
        pytest.param(
            SpawnAbsentOutcome(executable=SPAWN_ABSENT_CANARY),
            frozenset({"kind", "executable"}),
            id="spawn-absent",
        ),
        pytest.param(
            SpawnFailedOutcome(
                errno=errno.EACCES,
                error_message=SPAWN_FAILURE_CANARY,
            ),
            frozenset({"kind", "errno", "error_message"}),
            id="spawn-failed",
        ),
        pytest.param(
            BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME),
            frozenset({"kind", "axis"}),
            id="budget-exceeded",
        ),
        pytest.param(
            ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
                failure_detail=PROTOCOL_FAILURE_CANARY,
                accepted_output_count=0,
            ),
            frozenset(
                {
                    "kind",
                    "failure_code",
                    "accepted_output_count",
                    "failure_detail",
                }
            ),
            id="protocol-failed",
        ),
        pytest.param(
            CancelledOutcome(),
            frozenset({"kind"}),
            id="cancelled",
        ),
    ],
)
def test_every_outcome_projection_has_exact_paths_and_persists_diagnostic_fields(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    host_runtime: IsolatedHostPythonRuntime,
    outcome: ExecutionOutcome,
    expected_outcome_paths: frozenset[str],
) -> None:
    prepared_run = store.prepare(
        _producer_prepared_record(
            execution_id,
            TrustedCommandTarget(argv=("/bin/true",)),
            EnvGrant.none(),
            host_runtime,
        )
    )
    running_run = store.mark_running(
        prepared_run, ProcessRecord(pid=4242, started_at=STARTED_AT)
    )
    store.finalize(
        running_run,
        _result(
            execution_id,
            outcome=outcome,
            attribution=ExecutionAttribution(
                owner=FailureOwner.EXECUTOR,
                detail=ATTRIBUTION_CANARY,
            ),
        ),
    )
    finalized_bytes = _manifest_bytes(_record_dir(store, running_run))
    finalized = json.loads(finalized_bytes)
    assert finalized["result"]["attribution"]["detail"] == ATTRIBUTION_CANARY
    persisted_outcome = finalized["result"]["outcome"]
    match outcome:
        case SpawnAbsentOutcome():
            assert persisted_outcome["executable"] == outcome.executable
        case SpawnFailedOutcome():
            assert persisted_outcome["error_message"] == outcome.error_message
        case ProtocolFailedOutcome():
            assert (
                persisted_outcome["failure_detail"] == outcome.failure_detail
            )
    assert _leaf_key_paths(finalized) == _with_outcome_leaf_key_paths(
        _FINALIZED_LEAF_KEY_PATHS,
        expected_outcome_paths,
    )


def test_result_attribution_detail_persists_while_stream_bytes_stay_retained_only(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    prepared_run = store.prepare(
        _producer_prepared_record(
            execution_id,
            TrustedCommandTarget(argv=("/bin/true",)),
            EnvGrant.none(),
            host_runtime,
        )
    )
    running_run = store.mark_running(
        prepared_run,
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    store.finalize(
        running_run,
        _result(
            execution_id,
            attribution=ExecutionAttribution(
                owner=FailureOwner.EXECUTOR,
                detail=ATTRIBUTION_CANARY,
            ),
            stdout=_stream(
                head=STDOUT_HEAD_CANARY,
                tail=STDOUT_TAIL_CANARY,
            ),
            stderr=_stream(
                head=STDERR_HEAD_CANARY,
                tail=STDERR_TAIL_CANARY,
            ),
        ),
    )

    manifest_bytes = _manifest_bytes(_record_dir(store, running_run))
    finalized = json.loads(manifest_bytes)
    assert finalized["result"]["attribution"]["detail"] == ATTRIBUTION_CANARY
    _assert_no_recoverable_canaries(
        manifest_bytes,
        (
            STDOUT_HEAD_CANARY,
            STDOUT_TAIL_CANARY,
            STDERR_HEAD_CANARY,
            STDERR_TAIL_CANARY,
        ),
    )


def test_the_manifest_excludes_pool_queue_and_lease_context(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))

    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))

    text = json.dumps(manifest)
    for excluded in ("capacity", "queue", "lease", "worker", "pool"):
        assert excluded not in text
    assert set(manifest) == {
        "state",
        "header",
        "declaration",
        "result",
        "outputs",
    }


def test_an_unwritable_run_directory_degrades_the_receipt(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    _record_dir(store, running_run).chmod(0o500)

    try:
        receipt = store.finalize(running_run, _result(execution_id))
    finally:
        _record_dir(store, running_run).chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.RUNNING
    assert receipt.reference == running_run.reference
    assert len(receipt.failures) == 1
    assert receipt.failures[0].operation == "finalize"


@pytest.mark.parametrize(
    ("stage", "error_number", "expected_files"),
    [
        pytest.param(
            "stdout_finalize",
            errno.EIO,
            {MANIFEST_NAME, STDOUT_SIDECAR_NAME},
            id="stdout-finalize",
        ),
        pytest.param(
            "stderr_open",
            errno.ENOSPC,
            {MANIFEST_NAME, STDOUT_SIDECAR_NAME},
            id="stderr-open-enospc",
        ),
        pytest.param(
            "stderr_finalize",
            errno.EIO,
            {MANIFEST_NAME, STDOUT_SIDECAR_NAME, STDERR_SIDECAR_NAME},
            id="stderr-finalize",
        ),
        pytest.param(
            "manifest_publish",
            errno.ENOSPC,
            {MANIFEST_NAME, STDOUT_SIDECAR_NAME, STDERR_SIDECAR_NAME},
            id="manifest-publish-enospc",
        ),
    ],
)
def test_finalization_faults_preserve_the_latest_manifest_and_degrade(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
    stage: Literal[
        "stdout_finalize",
        "stderr_open",
        "stderr_finalize",
        "manifest_publish",
    ],
    error_number: int,
    expected_files: set[str],
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    committed = _manifest_bytes(_record_dir(store, running_run))
    _install_finalization_fault(
        monkeypatch,
        stage=stage,
        error_number=error_number,
    )

    receipt = store.finalize(
        running_run,
        _result(
            execution_id,
            outcome=SpawnFailedOutcome(
                errno=error_number,
                error_message=SECRET_ERROR_DETAIL,
            ),
            stdout=_stream(head=b"stdout"),
            stderr=_stream(head=b"stderr"),
        ),
    )

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.RUNNING
    assert receipt.reference == running_run.reference
    assert len(receipt.failures) == 1
    failure = receipt.failures[0]
    assert failure.operation == "finalize"
    assert failure.errno == error_number
    assert failure.detail.isidentifier()
    assert SECRET_ERROR_DETAIL not in failure.detail
    assert _manifest_bytes(_record_dir(store, running_run)) == committed
    assert {
        entry.name for entry in _record_dir(store, running_run).iterdir()
    } == (expected_files)
    assert store.load(running_run.reference).state == RecordState.RUNNING


def test_an_unwritable_directory_fails_mark_running_without_losing_prepared(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    committed = _manifest_bytes(_record_dir(store, prepared_run))
    _record_dir(store, prepared_run).chmod(0o500)

    try:
        with pytest.raises(ExecutorFailure) as raised:
            store.mark_running(
                prepared_run,
                ProcessRecord(pid=4242, started_at=STARTED_AT),
            )
    finally:
        _record_dir(store, prepared_run).chmod(0o700)

    assert isinstance(raised.value.__cause__, DocumentDirectoryError)
    assert _manifest_bytes(_record_dir(store, prepared_run)) == committed
    assert store.load(prepared_run.reference).state == RecordState.PREPARED


def test_degradation_preserves_the_last_valid_on_disk_state(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    committed = _manifest_bytes(_record_dir(store, running_run))
    _record_dir(store, running_run).chmod(0o500)

    try:
        store.finalize(running_run, _result(execution_id))
    finally:
        _record_dir(store, running_run).chmod(0o700)

    assert _manifest_bytes(_record_dir(store, running_run)) == committed
    assert store.load(running_run.reference).state == RecordState.RUNNING


def test_a_degraded_receipt_from_prepared_reports_the_prepared_state(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    _record_dir(store, prepared_run).chmod(0o500)

    try:
        receipt = store.finalize(prepared_run, _result(execution_id))
    finally:
        _record_dir(store, prepared_run).chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.PREPARED


def test_a_missing_run_directory_degrades_rather_than_raising(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_record = _prepared_record(execution_id)
    missing = PreparedRun(
        execution_id=execution_id,
        reference=RunRecordReference(record_id=uuid4()),
        header=prepared_record.header,
        declaration=prepared_record.declaration,
    )

    receipt = store.finalize(missing, _result(execution_id))

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.PREPARED
    with pytest.raises(RecordLoadError):
        store.load(missing.reference)


def test_a_recording_failure_names_no_rejected_value(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    _record_dir(store, running_run).chmod(0o500)

    try:
        receipt = store.finalize(
            running_run,
            _result(
                execution_id,
                outcome=SpawnAbsentOutcome(executable=SECRET_EXECUTABLE),
                stdout=_stream(head=SECRET_STDIN),
            ),
        )
    finally:
        _record_dir(store, running_run).chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    failure = receipt.failures[0]
    assert SECRET_EXECUTABLE not in failure.detail
    assert SECRET_STDIN.decode() not in failure.detail
    # The detail is a sanitized category, not the dependency's message.
    assert failure.detail.isidentifier()
    assert failure.errno == errno.EACCES


def test_prepare_failure_raises_so_no_child_is_spawned(
    tmp_path: Path,
    execution_id: ExecutionId,
) -> None:
    store = DirectoryRunStore(root=tmp_path / "never-created")

    with pytest.raises(ExecutorFailure) as raised:
        store.prepare(_prepared_record(execution_id))
    assert isinstance(raised.value.__cause__, DocumentDirectoryError)


def test_concurrent_writers_allocate_collision_free_directories(
    store: DirectoryRunStore,
) -> None:
    writer_count = 16

    def prepare_one(_: int) -> PreparedRun:
        execution_id = ExecutionId(
            job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4())
        )
        return store.prepare(_prepared_record(execution_id))

    with ThreadPoolExecutor(max_workers=writer_count) as pool:
        runs = list(pool.map(prepare_one, range(writer_count)))

    directories = {_record_dir(store, run) for run in runs}
    assert len(directories) == writer_count
    assert {path for path in store.root.iterdir()} == directories
    for run in runs:
        loaded = store.load(run.reference)
        assert loaded.declaration.execution_id == run.execution_id


@pytest.mark.parametrize(
    ("manifest", "expected_message"),
    [
        pytest.param(b"{", "not canonical JSON bytes", id="malformed-json"),
        pytest.param(
            b"\xff\xfe", "not canonical JSON bytes", id="invalid-utf8"
        ),
        pytest.param(
            b'{"b":1,"a":2}', "not canonical JSON bytes", id="non-canonical"
        ),
        pytest.param(b'{"state":"gone"}', "not a valid", id="unknown-state"),
        pytest.param(b"{}", "not a valid", id="missing-discriminant"),
    ],
)
def test_load_rejects_a_manifest_that_is_not_a_valid_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    manifest: bytes,
    expected_message: str,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    (_record_dir(store, run) / MANIFEST_NAME).write_bytes(manifest)

    with pytest.raises(RecordLoadError, match=expected_message) as raised:
        store.load(run.reference)

    assert raised.value.__cause__ is not None


@pytest.mark.parametrize(
    "corrupt",
    [
        pytest.param(
            lambda document: {**document, "extra": 1}, id="extra-field"
        ),
        pytest.param(
            lambda document: {
                key: value
                for key, value in document.items()
                if key != "schema_version"
            },
            id="missing-schema-version",
        ),
        pytest.param(
            lambda document: {**document, "schema": 1}, id="non-string-schema"
        ),
        pytest.param(lambda document: "not-a-document", id="non-object"),
    ],
)
def test_load_rejects_a_corrupted_embedded_identity_document(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    corrupt: Callable[[Jsonable], Jsonable],
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    payload = json.loads(_manifest_bytes(_record_dir(store, run)))
    header = payload["header"]
    header["executor_identity"] = corrupt(header["executor_identity"])
    (_record_dir(store, run) / MANIFEST_NAME).write_bytes(
        canonical_json_bytes(payload)
    )

    with pytest.raises(RecordLoadError, match="not a valid") as raised:
        store.load(run.reference)

    assert raised.value.__cause__ is not None


def test_load_rejects_a_canonical_manifest_with_a_coercible_scalar(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))
    manifest["header"]["schema_version"] = "1"
    (_record_dir(store, run) / MANIFEST_NAME).write_bytes(
        canonical_json_bytes(manifest)
    )

    with pytest.raises(RecordLoadError, match="not a valid"):
        store.load(run.reference)


def test_the_manifest_byte_ceiling_is_exactly_pinned() -> None:
    assert STRUCTURAL_MANIFEST_BYTE_CEILING == 256 * 1024 * 1024


def test_load_rejects_an_oversized_manifest(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    stored = _manifest_bytes(_record_dir(store, run))
    monkeypatch.setattr(
        dr_exec.recording.store,
        "STRUCTURAL_MANIFEST_BYTE_CEILING",
        len(stored) - 1,
    )

    with pytest.raises(RecordLoadError):
        store.load(run.reference)


def test_a_manifest_exactly_at_the_byte_ceiling_is_accepted(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    stored = _manifest_bytes(_record_dir(store, run))
    monkeypatch.setattr(
        dr_exec.recording.store,
        "STRUCTURAL_MANIFEST_BYTE_CEILING",
        len(stored),
    )

    assert store.load(run.reference).state == RecordState.PREPARED


def test_a_manifest_exactly_at_the_depth_ceiling_is_accepted(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared_record = _prepared_record(execution_id)
    run = store.prepare(prepared_record)
    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))
    monkeypatch.setattr(
        dr_exec.recording.store,
        "STRUCTURAL_DEPTH_CEILING",
        _container_depth(manifest),
    )

    assert store.load(run.reference) == prepared_record


def test_load_rejects_a_manifest_over_the_depth_ceiling(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))
    monkeypatch.setattr(
        dr_exec.recording.store,
        "STRUCTURAL_DEPTH_CEILING",
        _container_depth(manifest) - 1,
    )

    with pytest.raises(RecordLoadError):
        store.load(run.reference)


def test_load_rejects_a_missing_manifest(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    (_record_dir(store, run) / MANIFEST_NAME).unlink()

    with pytest.raises(RecordLoadError, match="could not read"):
        store.load(run.reference)


def test_load_rejects_an_external_manifest_symlink(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    tmp_path: Path,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    manifest_path = _record_dir(store, run) / MANIFEST_NAME
    external_path = tmp_path / "external-record.json"
    external_path.write_bytes(manifest_path.read_bytes())
    manifest_path.unlink()
    manifest_path.symlink_to(external_path)

    with pytest.raises(RecordLoadError) as raised:
        store.load(run.reference)

    assert isinstance(raised.value.__cause__, DocumentDirectoryError)


def test_load_translates_directory_disappearance_during_sidecar_verification(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))
    load_record = dr_exec.recording.store._load_record

    def load_then_remove_directory(record_dir: Path) -> object:
        record = load_record(record_dir)
        for entry in record_dir.iterdir():
            entry.unlink()
        record_dir.rmdir()
        return record

    monkeypatch.setattr(
        dr_exec.recording.store,
        "_load_record",
        load_then_remove_directory,
    )

    with pytest.raises(RecordLoadError) as raised:
        store.load(run.reference)

    assert isinstance(raised.value.__cause__, DocumentDirectoryError)


@pytest.mark.parametrize(
    "replacement",
    [
        pytest.param(b"tampered-same-len", id="digest-mismatch"),
        pytest.param(b"short", id="length-mismatch"),
    ],
)
def test_load_rejects_a_sidecar_that_drifted_from_its_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    replacement: bytes,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(
        run, _result(execution_id, stdout=_stream(head=b"original-stdout!!"))
    )
    (_record_dir(store, run) / STDOUT_SIDECAR_NAME).write_bytes(replacement)

    with pytest.raises(RecordLoadError, match="does not match its record"):
        store.load(run.reference)


def test_load_rejects_a_missing_sidecar(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id, stderr=_stream(head=b"err")))
    (_record_dir(store, run) / STDERR_SIDECAR_NAME).unlink()

    with pytest.raises(RecordLoadError, match=STDERR_SIDECAR_NAME):
        store.load(run.reference)


def test_load_rejects_an_unsafe_artifact_path_in_the_manifest(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))
    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))
    manifest["outputs"]["stdout"]["relative_path"] = "../escaped.bin"
    (_record_dir(store, run) / MANIFEST_NAME).write_bytes(
        json.dumps(manifest, separators=(",", ":")).encode()
    )

    with pytest.raises(RecordLoadError, match="not a valid"):
        store.load(run.reference)


@pytest.mark.parametrize(
    ("artifact_name", "unexpected_path"),
    [
        pytest.param("stdout", "other-stdout.bin", id="stdout"),
        pytest.param("stderr", "other-stderr.bin", id="stderr"),
    ],
)
def test_load_requires_the_two_exact_artifact_paths(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    artifact_name: Literal["stdout", "stderr"],
    unexpected_path: str,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))
    manifest = json.loads(_manifest_bytes(_record_dir(store, run)))
    manifest["outputs"][artifact_name]["relative_path"] = unexpected_path
    (_record_dir(store, run) / unexpected_path).write_bytes(b"")
    (_record_dir(store, run) / MANIFEST_NAME).write_bytes(
        canonical_json_bytes(manifest)
    )

    with pytest.raises(RecordLoadError, match="unexpected .* artifact path"):
        store.load(run.reference)


def test_load_rejects_an_equal_content_external_sidecar_symlink(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    tmp_path: Path,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    stdout = _stream(head=b"equal-content")
    store.finalize(run, _result(execution_id, stdout=stdout))
    stdout_path = _record_dir(store, run) / STDOUT_SIDECAR_NAME
    external_path = tmp_path / "external-stdout.bin"
    external_path.write_bytes(stdout_path.read_bytes())
    stdout_path.unlink()
    stdout_path.symlink_to(external_path)

    with pytest.raises(RecordLoadError, match="does not match its record"):
        store.load(run.reference)


@pytest.mark.parametrize(
    ("outcome", "owner"),
    [
        pytest.param(
            ExitedOutcome(exit_code=0), FailureOwner.NONE, id="success"
        ),
        pytest.param(
            ExitedOutcome(exit_code=1), FailureOwner.PAYLOAD, id="nonzero-exit"
        ),
        pytest.param(
            SpawnAbsentOutcome(executable=SECRET_EXECUTABLE),
            FailureOwner.EXECUTOR,
            id="spawn-absent",
        ),
    ],
)
def test_successful_and_failed_runs_both_produce_complete_records(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    outcome: ExecutionOutcome,
    owner: FailureOwner,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    receipt = store.finalize(
        run,
        _result(
            execution_id,
            outcome=outcome,
            attribution=ExecutionAttribution(owner=owner),
        ),
    )

    assert isinstance(receipt, CompleteRecordReceipt)
    finalized = store.load(run.reference)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.attribution.owner == owner
    assert finalized.result.execution_id == execution_id


def test_the_finalized_record_binds_the_declaration_to_its_result(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    other = ExecutionId(job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4()))

    with pytest.raises(ValidationError):
        store.finalize(run, _result(other))

    assert store.load(run.reference).state == RecordState.PREPARED


def test_the_on_disk_layout_literals_are_exactly_pinned() -> None:
    assert RECORD_DIRECTORY_PREFIX == "run"
    assert MANIFEST_NAME == "record.json"
    assert STDOUT_SIDECAR_NAME == "stdout.bin"
    assert STDERR_SIDECAR_NAME == "stderr.bin"


def test_an_allocated_run_directory_uses_the_owned_prefix_and_root(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    assert _record_dir(store, run).parent == store.root
    assert _record_dir(store, run).name.startswith(
        f"{RECORD_DIRECTORY_PREFIX}-"
    )


def test_a_finalized_run_directory_contains_exactly_the_pinned_files(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    store.finalize(run, _result(execution_id))

    assert {entry.name for entry in _record_dir(store, run).iterdir()} == {
        "record.json",
        "stdout.bin",
        "stderr.bin",
    }


def test_the_lifecycle_state_literals_are_exactly_pinned() -> None:
    assert RecordState.PREPARED == "prepared"
    assert RecordState.RUNNING == "running"
    assert RecordState.FINALIZED == "finalized"


def test_the_outcome_kind_literals_are_exactly_pinned() -> None:
    assert OutcomeKind.EXITED == "exited"
    assert OutcomeKind.SIGNALED == "signaled"
    assert OutcomeKind.SPAWN_ABSENT == "spawn_absent"
    assert OutcomeKind.SPAWN_FAILED == "spawn_failed"
    assert OutcomeKind.BUDGET_EXCEEDED == "budget_exceeded"
    assert OutcomeKind.PROTOCOL_FAILED == "protocol_failed"
    assert OutcomeKind.CANCELLED == "cancelled"


def test_the_failure_owner_literals_are_exactly_pinned() -> None:
    assert FailureOwner.NONE == "none"
    assert FailureOwner.PAYLOAD == "payload"
    assert FailureOwner.EXECUTOR == "executor"
    assert FailureOwner.MACHINE == "machine"


def test_the_receipt_kind_literals_are_exactly_pinned() -> None:
    assert RecordReceiptKind.COMPLETE == "complete"
    assert RecordReceiptKind.DEGRADED == "degraded"
    assert RecordReceiptKind.NOT_APPLICABLE == "not_applicable"


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        pytest.param(RecordState.PREPARED, "prepared", id="prepared"),
        pytest.param(RecordState.RUNNING, "running", id="running"),
        pytest.param(RecordState.FINALIZED, "finalized", id="finalized"),
    ],
)
def test_each_lifecycle_state_lands_in_the_manifest_verbatim(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    state: RecordState,
    expected: str,
) -> None:
    run: PreparedRun | RunningRun = store.prepare(
        _prepared_record(execution_id)
    )
    if state is not RecordState.PREPARED:
        run = store.mark_running(
            run,
            ProcessRecord(pid=4242, started_at=STARTED_AT),
        )
    if state is RecordState.FINALIZED:
        store.finalize(run, _result(execution_id))

    stored = json.loads(_manifest_bytes(_record_dir(store, run)))
    assert stored["state"] == expected


def test_a_finalized_manifest_spells_its_outcome_discriminant_verbatim(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    store.finalize(
        run,
        _result(execution_id, outcome=SpawnAbsentOutcome(executable="x")),
    )

    stored = json.loads(_manifest_bytes(_record_dir(store, run)))
    assert stored["state"] == "finalized"
    assert stored["result"]["outcome"]["kind"] == "spawn_absent"
