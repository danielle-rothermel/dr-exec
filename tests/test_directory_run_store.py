"""Lifecycle, degradation, secret-safety, and recovery of the run store.

Filesystem durability mechanics -- temp-write, flush, atomic replace --
are qualified in the pinned dr-store primitive and are not re-proven
here. These cases synchronize on published lifecycle state and terminal
outcomes; no case uses a sleep or elapsed time as evidence.
"""

from __future__ import annotations

import errno
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
from dr_serialize import canonical_json_bytes
from dr_store.docdir import DocumentDirectory
from dr_store.errors import DocumentDirectoryError
from pydantic import ValidationError

from dr_exec import (
    AttemptId,
    Budgets,
    CancelledOutcome,
    CompleteRecordReceipt,
    DegradedRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionAttribution,
    ExecutionId,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExecutorFailure,
    ExitedOutcome,
    FailureOwner,
    FinalizedRecord,
    JobId,
    OutcomeKind,
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
    SpawnAbsentOutcome,
    TrustedCommandTargetRecord,
)
from dr_exec._identity import (
    _build_env_grant_record,
    _build_executor_config_identity,
    _build_executor_identity,
    _canonical_declaration_digest,
)
from dr_exec._provenance import ExecutorSourceSnapshot
from dr_exec.declare import ExecutorSelfBudgets, TrustedCommandTarget
from dr_exec.store import (
    MANIFEST_NAME,
    RECORD_DIRECTORY_PREFIX,
    STDERR_SIDECAR_NAME,
    STDOUT_SIDECAR_NAME,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dr_serialize import IdentityDocument, Jsonable

SECRET_ARGUMENT = "hunter2-argv-secret"
SECRET_STDIN = b"hunter2-stdin-secret"
SECRET_ENV_VALUE = "hunter2-env-secret"
SECRET_EXECUTABLE = "/nonexistent/hunter2-executable-secret"
SECRET_ERROR_DETAIL = "hunter2-diagnostic-secret"
PREPARED_AT = datetime(2026, 8, 4, 12, 0, 0, 500000, tzinfo=UTC)
STARTED_AT = datetime(2026, 8, 4, 12, 0, 1, 500000, tzinfo=UTC)
FINISHED_AT = datetime(2026, 8, 4, 12, 0, 2, 500000, tzinfo=UTC)


@pytest.fixture
def store(tmp_path: Path) -> DirectoryRunStore:
    root = tmp_path / "records"
    root.mkdir()
    return DirectoryRunStore(root=root)


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


def _declaration(execution_id: ExecutionId) -> RunDeclaration:
    target = TrustedCommandTarget(
        argv=("/bin/echo", SECRET_ARGUMENT),
        stdin=SECRET_STDIN,
    )
    return RunDeclaration(
        execution_id=execution_id,
        target=TrustedCommandTargetRecord(
            canonical_declaration_sha256=_canonical_declaration_digest(target)
        ),
        env=_build_env_grant_record(
            EnvGrant.fixed({"TOKEN": SECRET_ENV_VALUE})
        ),
        budgets=Budgets.unbudgeted(),
    )


def _prepared_record(execution_id: ExecutionId) -> PreparedRecord:
    return PreparedRecord(
        header=_header(),
        declaration=_declaration(execution_id),
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


# --- valid prepared, running, finalized transitions -------------------


def test_prepare_publishes_a_complete_prepared_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_record = _prepared_record(execution_id)
    run = store.prepare(prepared_record)

    assert run.execution_id == execution_id
    assert run.record_dir.parent == store.root
    assert run.record_dir.name.startswith(f"{RECORD_DIRECTORY_PREFIX}-")
    assert store.load(run.record_dir) == prepared_record


def test_mark_running_publishes_the_process_bearing_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_record = _prepared_record(execution_id)
    prepared_run = store.prepare(prepared_record)
    process = ProcessRecord(pid=4242, started_at=STARTED_AT)

    running_run = store.mark_running(prepared_run, process)

    assert running_run.record_dir == prepared_run.record_dir
    loaded = store.load(running_run.record_dir)
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
    assert isinstance(store.load(running_run.record_dir), FinalizedRecord)


def test_a_recognized_pre_child_outcome_finalizes_from_prepared(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Pre-spawn cancellation never publishes a `running` state."""
    prepared_run = store.prepare(_prepared_record(execution_id))

    receipt = store.finalize(
        prepared_run,
        _result(execution_id, outcome=CancelledOutcome()),
    )

    assert isinstance(receipt, CompleteRecordReceipt)
    finalized = store.load(prepared_run.record_dir)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.outcome.kind == CancelledOutcome().kind


def test_mark_running_rejects_a_handle_whose_record_is_finalized(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Every ``mark_running`` failure leaves as the documented type.

    The engine converts this post-start publication failure into a
    degraded receipt, so the read half of the operation must not raise a
    different class than the write half.
    """
    prepared_run = store.prepare(_prepared_record(execution_id))
    store.finalize(prepared_run, _result(execution_id))

    with pytest.raises(ExecutorFailure) as raised:
        store.mark_running(
            prepared_run,
            ProcessRecord(pid=4242, started_at=STARTED_AT),
        )

    assert isinstance(raised.value.__cause__, RecordLoadError)
    assert "not in the prepared state" in str(raised.value.__cause__)


def test_mark_running_reports_an_unreadable_manifest_as_executor_failure(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """A corrupt manifest is a read failure, not a different taxonomy."""
    prepared_run = store.prepare(_prepared_record(execution_id))
    (prepared_run.record_dir / MANIFEST_NAME).write_bytes(b"{")

    with pytest.raises(ExecutorFailure) as raised:
        store.mark_running(
            prepared_run,
            ProcessRecord(pid=4242, started_at=STARTED_AT),
        )

    assert isinstance(raised.value.__cause__, RecordLoadError)


def test_finalizing_twice_degrades_rather_than_replacing_the_record(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    store.finalize(prepared_run, _result(execution_id, stdout=_stream(b"a")))
    first = _manifest_bytes(prepared_run.record_dir)

    receipt = store.finalize(
        prepared_run, _result(execution_id, stdout=_stream(b"bbbb"))
    )

    assert isinstance(receipt, DegradedRecordReceipt)
    assert _manifest_bytes(prepared_run.record_dir) == first
    # The handle proves only a lower bound; the receipt must not
    # understate the finalized record that is durably on disk.
    assert receipt.latest_state == RecordState.FINALIZED
    assert receipt.latest_state == store.load(prepared_run.record_dir).state


# --- abrupt parent death and valid incomplete recovery ----------------


@pytest.mark.parametrize(
    ("commit_state", "expected_state"),
    [
        pytest.param(
            RecordState.PREPARED, RecordState.PREPARED, id="prepared"
        ),
        pytest.param(RecordState.RUNNING, RecordState.RUNNING, id="running"),
    ],
)
def test_a_record_committed_before_parent_death_recovers_as_incomplete(
    tmp_path: Path,
    execution_id: ExecutionId,
    commit_state: RecordState,
    expected_state: RecordState,
) -> None:
    """The committed state is the recovery evidence, not a wall clock.

    A child process publishes exactly one lifecycle state and then dies
    abruptly via ``os._exit``, skipping every cleanup path. The parent
    synchronizes on that terminal exit, so the on-disk state is
    whatever was committed before death -- never a partial manifest.
    """
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
            handoff.write_text(run.record_dir.name)
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

    record_dir = root / handoff.read_text()
    recovered = DirectoryRunStore(root=root).load(record_dir)
    assert recovered.state == expected_state
    assert recovered.state is not RecordState.FINALIZED


def test_recovery_never_infers_completion_from_sidecars_on_disk(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Sidecar bytes without a finalized manifest prove nothing."""
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    (running_run.record_dir / STDOUT_SIDECAR_NAME).write_bytes(b"partial")
    (running_run.record_dir / STDERR_SIDECAR_NAME).write_bytes(b"partial")

    recovered = store.load(running_run.record_dir)

    assert recovered.state == RecordState.RUNNING


# --- atomic finalization with digest-matching sidecars ----------------


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

    finalized = store.load(prepared_run.record_dir)
    assert isinstance(finalized, FinalizedRecord)
    stored_stdout = (
        prepared_run.record_dir / STDOUT_SIDECAR_NAME
    ).read_bytes()
    assert stored_stdout == stdout.head + stdout.tail
    assert finalized.outputs.stdout.size_bytes == len(stored_stdout)
    assert finalized.outputs.stdout.relative_path == Path(STDOUT_SIDECAR_NAME)
    assert finalized.outputs.stderr.relative_path == Path(STDERR_SIDECAR_NAME)


def test_head_and_tail_segments_recover_exactly_with_their_counts(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Readers get segment lengths, never an inferred contiguous stream."""
    prepared_run = store.prepare(_prepared_record(execution_id))
    stdout = _stream(head=b"HEAD", tail=b"TAILTAIL", dropped_bytes=17)

    store.finalize(prepared_run, _result(execution_id, stdout=stdout))

    finalized = store.load(prepared_run.record_dir)
    assert isinstance(finalized, FinalizedRecord)
    stream_record = finalized.result.payload_outputs.stdout
    assert stream_record.head_bytes == len(stdout.head)
    assert stream_record.tail_bytes == len(stdout.tail)
    assert stream_record.produced_bytes == stdout.produced_bytes
    assert stream_record.dropped_bytes == stdout.dropped_bytes
    stored = (prepared_run.record_dir / STDOUT_SIDECAR_NAME).read_bytes()
    assert stored[: stream_record.head_bytes] == stdout.head
    assert stored[stream_record.head_bytes :] == stdout.tail


def test_accepted_protocol_outputs_stay_inline_and_complete(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
    request_document: IdentityDocument,
) -> None:
    """A later protocol failure never discards earlier accepted outputs."""
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

    finalized = store.load(prepared_run.record_dir)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.protocol_outputs == outputs


# --- secret-safe durable evidence -------------------------------------


def test_no_lifecycle_manifest_exposes_a_recoverable_secret(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    prepared_bytes = _manifest_bytes(prepared_run.record_dir)
    running_run = store.mark_running(
        prepared_run, ProcessRecord(pid=4242, started_at=STARTED_AT)
    )
    running_bytes = _manifest_bytes(running_run.record_dir)
    store.finalize(
        running_run,
        _result(
            execution_id,
            outcome=SpawnAbsentOutcome(executable=SECRET_EXECUTABLE),
            attribution=ExecutionAttribution(
                owner=FailureOwner.EXECUTOR, detail=SECRET_ERROR_DETAIL
            ),
        ),
    )
    finalized_bytes = _manifest_bytes(running_run.record_dir)

    for manifest in (prepared_bytes, running_bytes, finalized_bytes):
        assert SECRET_ARGUMENT.encode() not in manifest
        assert SECRET_STDIN not in manifest
        assert SECRET_ENV_VALUE.encode() not in manifest
        assert SECRET_EXECUTABLE.encode() not in manifest
        assert SECRET_ERROR_DETAIL.encode() not in manifest


def test_the_manifest_keeps_grant_identity_without_grant_values(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    manifest = json.loads(_manifest_bytes(run.record_dir))

    env = manifest["declaration"]["env"]
    assert env["var_names"] == ["TOKEN"]
    assert len(env["canonical_values_sha256"]) == 64
    assert SECRET_ENV_VALUE not in json.dumps(manifest)


def test_the_manifest_excludes_pool_queue_and_lease_context(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))

    manifest = json.loads(_manifest_bytes(run.record_dir))

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


def test_the_manifest_records_secret_free_invocation_evidence(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    target = json.loads(_manifest_bytes(run.record_dir))["declaration"][
        "target"
    ]

    assert set(target) == {"kind", "canonical_declaration_sha256"}
    assert target["kind"] == "trusted_command"
    assert len(target["canonical_declaration_sha256"]) == 64


# --- degradation without changed attribution --------------------------


def test_an_unwritable_run_directory_degrades_the_receipt(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    running_run.record_dir.chmod(0o500)

    try:
        receipt = store.finalize(running_run, _result(execution_id))
    finally:
        running_run.record_dir.chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.RUNNING
    assert receipt.record_dir == running_run.record_dir
    assert len(receipt.failures) == 1
    assert receipt.failures[0].operation == "finalize"


def test_an_unwritable_directory_fails_mark_running_without_losing_prepared(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Post-start publication failure raises dr-exec's typed error.

    ``mark_running`` returns a lifecycle handle, not a receipt, so the
    engine is what converts this into a degraded receipt. The
    ``prepared`` manifest stays intact and loadable on disk, so
    ``prepared`` remains the latest valid state.
    """
    prepared_run = store.prepare(_prepared_record(execution_id))
    committed = _manifest_bytes(prepared_run.record_dir)
    prepared_run.record_dir.chmod(0o500)

    try:
        with pytest.raises(ExecutorFailure) as raised:
            store.mark_running(
                prepared_run,
                ProcessRecord(pid=4242, started_at=STARTED_AT),
            )
    finally:
        prepared_run.record_dir.chmod(0o700)

    assert isinstance(raised.value.__cause__, DocumentDirectoryError)
    assert _manifest_bytes(prepared_run.record_dir) == committed
    assert store.load(prepared_run.record_dir).state == RecordState.PREPARED


def test_degradation_preserves_the_last_valid_on_disk_state(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    committed = _manifest_bytes(running_run.record_dir)
    running_run.record_dir.chmod(0o500)

    try:
        store.finalize(running_run, _result(execution_id))
    finally:
        running_run.record_dir.chmod(0o700)

    assert _manifest_bytes(running_run.record_dir) == committed
    assert store.load(running_run.record_dir).state == RecordState.RUNNING


def test_a_degraded_receipt_from_prepared_reports_the_prepared_state(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    prepared_run = store.prepare(_prepared_record(execution_id))
    prepared_run.record_dir.chmod(0o500)

    try:
        receipt = store.finalize(prepared_run, _result(execution_id))
    finally:
        prepared_run.record_dir.chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.PREPARED


def test_a_missing_run_directory_degrades_rather_than_raising(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Finalization degrades even when nothing valid remains on disk.

    No lifecycle state is readable here, so the handle's own state is
    the closest remaining claim the receipt can make.
    """
    missing = PreparedRun(
        execution_id=execution_id,
        record_dir=store.root / "run-absent",
    )

    receipt = store.finalize(missing, _result(execution_id))

    assert isinstance(receipt, DegradedRecordReceipt)
    assert receipt.latest_state == RecordState.PREPARED
    with pytest.raises(RecordLoadError):
        store.load(missing.record_dir)


def test_a_recording_failure_names_no_rejected_value(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    running_run = store.mark_running(
        store.prepare(_prepared_record(execution_id)),
        ProcessRecord(pid=4242, started_at=STARTED_AT),
    )
    running_run.record_dir.chmod(0o500)

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
        running_run.record_dir.chmod(0o700)

    assert isinstance(receipt, DegradedRecordReceipt)
    failure = receipt.failures[0]
    assert SECRET_EXECUTABLE not in failure.detail
    assert SECRET_STDIN.decode() not in failure.detail
    # The detail is the failing type alone. Borrowing the underlying
    # message would make dr-exec's secret-safety depend on how a pinned
    # dependency happens to word its errors.
    # Sidecars are flushed before the manifest, so an unwritable run
    # directory faults on opening the first sidecar.
    assert failure.detail == "AllocationError"
    assert failure.errno == errno.EACCES


def test_prepare_failure_raises_so_no_child_is_spawned(
    tmp_path: Path,
    execution_id: ExecutionId,
) -> None:
    """Prepare has no degraded receipt: it precedes the attempt.

    The primitive's allocation error is translated into dr-exec's own
    taxonomy and preserved as ``__cause__``.
    """
    store = DirectoryRunStore(root=tmp_path / "never-created")

    with pytest.raises(ExecutorFailure) as raised:
        store.prepare(_prepared_record(execution_id))
    assert isinstance(raised.value.__cause__, DocumentDirectoryError)


# --- concurrent collision-free writers --------------------------------


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

    directories = {run.record_dir for run in runs}
    assert len(directories) == writer_count
    assert {path for path in store.root.iterdir()} == directories
    for run in runs:
        loaded = store.load(run.record_dir)
        assert loaded.declaration.execution_id == run.execution_id


# --- malformed manifest and sidecar rejection -------------------------


@pytest.mark.parametrize(
    ("manifest", "expected_message"),
    [
        pytest.param(b"{", "could not read", id="malformed-json"),
        pytest.param(b"\xff\xfe", "could not read", id="invalid-utf8"),
        pytest.param(b'{"b":1,"a":2}', "could not read", id="non-canonical"),
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
    (run.record_dir / MANIFEST_NAME).write_bytes(manifest)

    with pytest.raises(RecordLoadError, match=expected_message) as raised:
        store.load(run.record_dir)

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
    """A bad identity document is a load failure, not an escaping error.

    The manifest stays canonical, so only the embedded document's shape
    is invalid: the shared validator's error must arrive as
    ``RecordLoadError`` with the original preserved as ``__cause__``.
    """
    run = store.prepare(_prepared_record(execution_id))
    payload = json.loads(_manifest_bytes(run.record_dir))
    header = payload["header"]
    header["executor_identity"] = corrupt(header["executor_identity"])
    (run.record_dir / MANIFEST_NAME).write_bytes(canonical_json_bytes(payload))

    with pytest.raises(RecordLoadError, match="not a valid") as raised:
        store.load(run.record_dir)

    assert raised.value.__cause__ is not None


def test_the_manifest_read_reproduces_the_stored_canonical_bytes(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """Strict validation runs on bytes equal to the ones on disk.

    The primitive returns a decoded payload rather than the bytes it
    verified, so the load path re-encodes it. This pins the two pinned
    packages' canonical profiles agreeing, for every lifecycle state.
    """

    def stored_bytes_round_trip() -> bytes:
        raw = _manifest_bytes(run.record_dir)
        decoded = DocumentDirectory.read_manifest(
            run.record_dir, manifest_name=MANIFEST_NAME
        )
        assert canonical_json_bytes(decoded) == raw
        return raw

    run = store.prepare(_prepared_record(execution_id))
    prepared_bytes = stored_bytes_round_trip()
    running = store.mark_running(
        run, ProcessRecord(pid=4242, started_at=STARTED_AT)
    )
    running_bytes = stored_bytes_round_trip()
    store.finalize(running, _result(execution_id, stdout=_stream(b"a")))
    finalized_bytes = stored_bytes_round_trip()

    assert len({prepared_bytes, running_bytes, finalized_bytes}) == 3


def test_load_rejects_a_missing_manifest(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    (run.record_dir / MANIFEST_NAME).unlink()

    with pytest.raises(RecordLoadError, match="could not read"):
        store.load(run.record_dir)


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
    (run.record_dir / STDOUT_SIDECAR_NAME).write_bytes(replacement)

    with pytest.raises(RecordLoadError, match="does not match its record"):
        store.load(run.record_dir)


def test_load_rejects_a_missing_sidecar(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id, stderr=_stream(head=b"err")))
    (run.record_dir / STDERR_SIDECAR_NAME).unlink()

    with pytest.raises(RecordLoadError, match=STDERR_SIDECAR_NAME):
        store.load(run.record_dir)


def test_load_rejects_an_unsafe_artifact_path_in_the_manifest(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """An escaping relative path fails validation, never a read attempt."""
    run = store.prepare(_prepared_record(execution_id))
    store.finalize(run, _result(execution_id))
    manifest = json.loads(_manifest_bytes(run.record_dir))
    manifest["outputs"]["stdout"]["relative_path"] = "../escaped.bin"
    (run.record_dir / MANIFEST_NAME).write_bytes(
        json.dumps(manifest, separators=(",", ":")).encode()
    )

    with pytest.raises(RecordLoadError, match="not a valid"):
        store.load(run.record_dir)


# --- records for successful and failed real runs ----------------------


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
    finalized = store.load(run.record_dir)
    assert isinstance(finalized, FinalizedRecord)
    assert finalized.result.attribution.owner == owner
    assert finalized.result.execution_id == execution_id


def test_the_finalized_record_binds_the_declaration_to_its_result(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    """A result from another run is a caller defect, not a disk failure.

    Degradation describes the storage medium failing. An invalid record
    construction is dr-exec's own invariant breaking, so it raises
    rather than being reported as a recording problem.
    """
    run = store.prepare(_prepared_record(execution_id))
    other = ExecutionId(job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4()))

    with pytest.raises(ValidationError):
        store.finalize(run, _result(other))

    assert store.load(run.record_dir).state == RecordState.PREPARED


# --- pinned persisted literals ----------------------------------------


def test_the_on_disk_layout_literals_are_exactly_pinned() -> None:
    """The four layout names are the on-disk contract, spelled out.

    Reading these symbolically everywhere else means only this test
    stands between a rename and silent drift of the stored layout.
    """
    assert RECORD_DIRECTORY_PREFIX == "run"
    assert MANIFEST_NAME == "record.json"
    assert STDOUT_SIDECAR_NAME == "stdout.bin"
    assert STDERR_SIDECAR_NAME == "stderr.bin"


def test_an_allocated_run_directory_is_named_by_the_pinned_pattern(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    assert re.fullmatch(
        r"run-\d{8}T\d{12}Z-"
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12}",
        run.record_dir.name,
    )


def test_a_finalized_run_directory_contains_exactly_the_pinned_files(
    store: DirectoryRunStore,
    execution_id: ExecutionId,
) -> None:
    run = store.prepare(_prepared_record(execution_id))

    store.finalize(run, _result(execution_id))

    assert {entry.name for entry in run.record_dir.iterdir()} == {
        "record.json",
        "stdout.bin",
        "stderr.bin",
    }


def test_the_lifecycle_state_literals_are_exactly_pinned() -> None:
    """Lifecycle states are persisted identity in every manifest."""
    assert RecordState.PREPARED == "prepared"
    assert RecordState.RUNNING == "running"
    assert RecordState.FINALIZED == "finalized"
    assert [state.value for state in RecordState] == [
        "prepared",
        "running",
        "finalized",
    ]


def test_the_outcome_kind_literals_are_exactly_pinned() -> None:
    assert [kind.value for kind in OutcomeKind] == [
        "exited",
        "signaled",
        "spawn_absent",
        "spawn_failed",
        "budget_exceeded",
        "protocol_failed",
        "cancelled",
    ]


def test_the_failure_owner_literals_are_exactly_pinned() -> None:
    assert [owner.value for owner in FailureOwner] == [
        "none",
        "payload",
        "executor",
        "machine",
    ]


def test_the_receipt_kind_literals_are_exactly_pinned() -> None:
    assert [kind.value for kind in RecordReceiptKind] == [
        "complete",
        "degraded",
        "not_applicable",
    ]


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
    """The stored bytes, not the enum, are what a reader must see."""
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

    stored = json.loads(_manifest_bytes(run.record_dir))
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

    stored = json.loads(_manifest_bytes(run.record_dir))
    assert stored["state"] == "finalized"
    assert stored["result"]["outcome"]["kind"] == "spawn_absent"
