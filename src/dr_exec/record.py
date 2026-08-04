from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

from dr_serialize import IdentityDocument
from pydantic import (
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)

from dr_exec._identity import (
    _validate_executor_config_identity,
    _validate_executor_identity,
)
from dr_exec._model import (
    ContractModel,
    UtcDatetime,
    _validate_sha256_digest,
)
from dr_exec.declare import Budgets, EnvGrantRecord
from dr_exec.kinds import (
    BudgetAxis,
    ContainmentProfile,
    ExecutionTargetKind,
    FailureOwner,
    OutcomeKind,
    ProtocolFailureCode,
    RecordReceiptKind,
    RecordState,
)
from dr_exec.names import ExecutionId
from dr_exec.runtime import RuntimeRecord


class RunRecordHeader(ContractModel):
    schema_version: Literal[1] = 1
    executor_identity: IdentityDocument
    executor_config_identity: IdentityDocument
    prepared_at: UtcDatetime

    _validated_executor_identity = field_validator("executor_identity")(
        _validate_executor_identity
    )
    _validated_executor_config_identity = field_validator(
        "executor_config_identity"
    )(_validate_executor_config_identity)


class TrustedCommandTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_COMMAND] = (
        ExecutionTargetKind.TRUSTED_COMMAND
    )
    canonical_declaration_sha256: str

    _validated_canonical_declaration_sha256 = field_validator(
        "canonical_declaration_sha256"
    )(_validate_sha256_digest)


class UntrustedCommandTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = (
        ExecutionTargetKind.UNTRUSTED_COMMAND
    )
    canonical_declaration_sha256: str
    containment_profile: ContainmentProfile

    _validated_canonical_declaration_sha256 = field_validator(
        "canonical_declaration_sha256"
    )(_validate_sha256_digest)


class UntrustedPythonTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = (
        ExecutionTargetKind.UNTRUSTED_PYTHON
    )
    canonical_declaration_sha256: str
    request_id_sha256: str
    containment_profile: ContainmentProfile
    runtime: RuntimeRecord

    _validated_digests = field_validator(
        "canonical_declaration_sha256", "request_id_sha256"
    )(_validate_sha256_digest)


type ExecutionTargetRecord = Annotated[
    TrustedCommandTargetRecord
    | UntrustedCommandTargetRecord
    | UntrustedPythonTargetRecord,
    Field(discriminator="kind"),
]


class RunDeclaration(ContractModel):
    execution_id: ExecutionId
    target: ExecutionTargetRecord
    env: EnvGrantRecord
    budgets: Budgets


class ProcessRecord(ContractModel):
    pid: PositiveInt
    started_at: UtcDatetime


class OutputArtifactRecord(ContractModel):
    relative_path: Path
    size_bytes: NonNegativeInt
    sha256: str

    _validated_sha256 = field_validator("sha256")(_validate_sha256_digest)

    @field_validator("relative_path", mode="before")
    @classmethod
    def spelling_must_be_normalized_relative(cls, path: object) -> object:
        if isinstance(path, str) and (
            path.startswith("/")
            or any(part in {"", ".", ".."} for part in path.split("/"))
        ):
            raise ValueError("relative_path must be normalized and relative")
        return path

    @field_validator("relative_path")
    @classmethod
    def path_must_be_normalized_relative(cls, path: Path) -> Path:
        spelling = path.as_posix()
        pure_path = PurePosixPath(spelling)
        if path.is_absolute() or spelling in {"", "."}:
            raise ValueError("relative_path must be a nonempty relative path")
        if any(part in {"", ".", ".."} for part in pure_path.parts):
            raise ValueError("relative_path must be normalized")
        return path


class OutputArtifactRecords(ContractModel):
    stdout: OutputArtifactRecord
    stderr: OutputArtifactRecord


class RetainedPayloadStream(ContractModel):
    head: bytes
    tail: bytes
    produced_bytes: NonNegativeInt
    dropped_bytes: NonNegativeInt

    @model_validator(mode="after")
    def retained_and_dropped_bytes_must_equal_produced_bytes(
        self,
    ) -> RetainedPayloadStream:
        if len(self.head) + len(self.tail) + self.dropped_bytes != (
            self.produced_bytes
        ):
            raise ValueError(
                "retained and dropped bytes must equal produced bytes"
            )
        return self


class PayloadOutputs(ContractModel):
    stdout: RetainedPayloadStream
    stderr: RetainedPayloadStream


class RetainedPayloadStreamRecord(ContractModel):
    head_bytes: NonNegativeInt
    tail_bytes: NonNegativeInt
    produced_bytes: NonNegativeInt
    dropped_bytes: NonNegativeInt

    @model_validator(mode="after")
    def retained_and_dropped_bytes_must_equal_produced_bytes(
        self,
    ) -> RetainedPayloadStreamRecord:
        if self.head_bytes + self.tail_bytes + self.dropped_bytes != (
            self.produced_bytes
        ):
            raise ValueError(
                "retained and dropped bytes must equal produced bytes"
            )
        return self


class PayloadOutputRecords(ContractModel):
    stdout: RetainedPayloadStreamRecord
    stderr: RetainedPayloadStreamRecord


class ExitedOutcome(ContractModel):
    kind: Literal[OutcomeKind.EXITED] = OutcomeKind.EXITED
    exit_code: int


class SignaledOutcome(ContractModel):
    kind: Literal[OutcomeKind.SIGNALED] = OutcomeKind.SIGNALED
    signal_number: PositiveInt


class SpawnAbsentOutcome(ContractModel):
    kind: Literal[OutcomeKind.SPAWN_ABSENT] = OutcomeKind.SPAWN_ABSENT
    executable: str


class SpawnFailedOutcome(ContractModel):
    kind: Literal[OutcomeKind.SPAWN_FAILED] = OutcomeKind.SPAWN_FAILED
    errno: int
    error_message: str


class BudgetExceededOutcome(ContractModel):
    kind: Literal[OutcomeKind.BUDGET_EXCEEDED] = OutcomeKind.BUDGET_EXCEEDED
    axis: BudgetAxis


class ProtocolFailedOutcome(ContractModel):
    kind: Literal[OutcomeKind.PROTOCOL_FAILED] = OutcomeKind.PROTOCOL_FAILED
    failure_code: ProtocolFailureCode
    failure_detail: str
    accepted_output_count: NonNegativeInt


class CancelledOutcome(ContractModel):
    kind: Literal[OutcomeKind.CANCELLED] = OutcomeKind.CANCELLED


type ExecutionOutcome = Annotated[
    ExitedOutcome
    | SignaledOutcome
    | SpawnAbsentOutcome
    | SpawnFailedOutcome
    | BudgetExceededOutcome
    | ProtocolFailedOutcome
    | CancelledOutcome,
    Field(discriminator="kind"),
]


class ExitedOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.EXITED] = OutcomeKind.EXITED
    exit_code: int


class SignaledOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.SIGNALED] = OutcomeKind.SIGNALED
    signal_number: PositiveInt


class SpawnAbsentOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.SPAWN_ABSENT] = OutcomeKind.SPAWN_ABSENT


class SpawnFailedOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.SPAWN_FAILED] = OutcomeKind.SPAWN_FAILED
    errno: int


class BudgetExceededOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.BUDGET_EXCEEDED] = OutcomeKind.BUDGET_EXCEEDED
    axis: BudgetAxis


class ProtocolFailedOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.PROTOCOL_FAILED] = OutcomeKind.PROTOCOL_FAILED
    failure_code: ProtocolFailureCode
    accepted_output_count: NonNegativeInt


class CancelledOutcomeRecord(ContractModel):
    kind: Literal[OutcomeKind.CANCELLED] = OutcomeKind.CANCELLED


type ExecutionOutcomeRecord = Annotated[
    ExitedOutcomeRecord
    | SignaledOutcomeRecord
    | SpawnAbsentOutcomeRecord
    | SpawnFailedOutcomeRecord
    | BudgetExceededOutcomeRecord
    | ProtocolFailedOutcomeRecord
    | CancelledOutcomeRecord,
    Field(discriminator="kind"),
]


class ExecutionAttribution(ContractModel):
    owner: FailureOwner
    detail: str | None = None


class ExecutionAttributionRecord(ContractModel):
    owner: FailureOwner


class ExecutionMeasurements(ContractModel):
    started_at: UtcDatetime
    finished_at: UtcDatetime
    duration_ns: NonNegativeInt
    teardown_duration_ns: NonNegativeInt
    input_bytes: NonNegativeInt
    protocol_bytes_received: NonNegativeInt

    @model_validator(mode="after")
    def finished_must_not_precede_started(self) -> ExecutionMeasurements:
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not precede started_at")
        return self


class ExecutionResult(ContractModel):
    execution_id: ExecutionId
    outcome: ExecutionOutcome
    attribution: ExecutionAttribution
    protocol_outputs: tuple[IdentityDocument, ...]
    payload_outputs: PayloadOutputs
    measurements: ExecutionMeasurements

    @model_validator(mode="after")
    def protocol_output_count_must_match_outcome(self) -> ExecutionResult:
        if isinstance(
            self.outcome, ProtocolFailedOutcome
        ) and self.outcome.accepted_output_count != len(self.protocol_outputs):
            raise ValueError(
                "accepted protocol output count does not match outputs"
            )
        return self


class ExecutionResultRecord(ContractModel):
    execution_id: ExecutionId
    outcome: ExecutionOutcomeRecord
    attribution: ExecutionAttributionRecord
    protocol_outputs: tuple[IdentityDocument, ...]
    payload_outputs: PayloadOutputRecords
    measurements: ExecutionMeasurements

    @model_validator(mode="after")
    def protocol_output_count_must_match_outcome(
        self,
    ) -> ExecutionResultRecord:
        if isinstance(
            self.outcome, ProtocolFailedOutcomeRecord
        ) and self.outcome.accepted_output_count != len(self.protocol_outputs):
            raise ValueError(
                "accepted protocol output count does not match outputs"
            )
        return self


class PreparedRecord(ContractModel):
    state: Literal[RecordState.PREPARED] = RecordState.PREPARED
    header: RunRecordHeader
    declaration: RunDeclaration


class RunningRecord(ContractModel):
    state: Literal[RecordState.RUNNING] = RecordState.RUNNING
    header: RunRecordHeader
    declaration: RunDeclaration
    process: ProcessRecord


class FinalizedRecord(ContractModel):
    state: Literal[RecordState.FINALIZED] = RecordState.FINALIZED
    header: RunRecordHeader
    declaration: RunDeclaration
    result: ExecutionResultRecord
    outputs: OutputArtifactRecords

    @model_validator(mode="after")
    def execution_ids_must_match(self) -> FinalizedRecord:
        if self.declaration.execution_id != self.result.execution_id:
            raise ValueError("declaration and result execution IDs differ")
        return self


type RunRecord = Annotated[
    PreparedRecord | RunningRecord | FinalizedRecord,
    Field(discriminator="state"),
]


class RecordingFailure(ContractModel):
    operation: str
    errno: int | None
    detail: str


class CompleteRecordReceipt(ContractModel):
    kind: Literal[RecordReceiptKind.COMPLETE] = RecordReceiptKind.COMPLETE
    execution_id: ExecutionId
    record_dir: Path
    latest_state: Literal[RecordState.FINALIZED] = RecordState.FINALIZED


class DegradedRecordReceipt(ContractModel):
    kind: Literal[RecordReceiptKind.DEGRADED] = RecordReceiptKind.DEGRADED
    execution_id: ExecutionId
    record_dir: Path
    latest_state: RecordState
    failures: tuple[RecordingFailure, ...]


class FakeRecordReceipt(ContractModel):
    kind: Literal[RecordReceiptKind.NOT_APPLICABLE] = (
        RecordReceiptKind.NOT_APPLICABLE
    )
    execution_id: ExecutionId


type RealRecordReceipt = CompleteRecordReceipt | DegradedRecordReceipt
type RecordReceipt = Annotated[
    CompleteRecordReceipt | DegradedRecordReceipt | FakeRecordReceipt,
    Field(discriminator="kind"),
]


class CompletedExecution(ContractModel):
    result: ExecutionResult
    record_receipt: RecordReceipt

    @model_validator(mode="after")
    def execution_ids_must_match(self) -> CompletedExecution:
        if self.result.execution_id != self.record_receipt.execution_id:
            raise ValueError("result and record receipt execution IDs differ")
        return self


__all__ = [
    "BudgetExceededOutcome",
    "BudgetExceededOutcomeRecord",
    "CancelledOutcome",
    "CancelledOutcomeRecord",
    "CompleteRecordReceipt",
    "CompletedExecution",
    "DegradedRecordReceipt",
    "ExecutionAttribution",
    "ExecutionAttributionRecord",
    "ExecutionMeasurements",
    "ExecutionOutcome",
    "ExecutionOutcomeRecord",
    "ExecutionResult",
    "ExecutionResultRecord",
    "ExecutionTargetRecord",
    "ExitedOutcome",
    "ExitedOutcomeRecord",
    "FakeRecordReceipt",
    "FinalizedRecord",
    "OutputArtifactRecord",
    "OutputArtifactRecords",
    "PayloadOutputRecords",
    "PayloadOutputs",
    "PreparedRecord",
    "ProcessRecord",
    "ProtocolFailedOutcome",
    "ProtocolFailedOutcomeRecord",
    "RealRecordReceipt",
    "RecordReceipt",
    "RecordingFailure",
    "RetainedPayloadStream",
    "RetainedPayloadStreamRecord",
    "RunDeclaration",
    "RunRecord",
    "RunRecordHeader",
    "RunningRecord",
    "SignaledOutcome",
    "SignaledOutcomeRecord",
    "SpawnAbsentOutcome",
    "SpawnAbsentOutcomeRecord",
    "SpawnFailedOutcome",
    "SpawnFailedOutcomeRecord",
    "TrustedCommandTargetRecord",
    "UntrustedCommandTargetRecord",
    "UntrustedPythonTargetRecord",
]
