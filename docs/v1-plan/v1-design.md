# dr-exec v1 design

V1 targets high-volume HumanEval-style evaluation, self-invocation probes, and
execution fakes. Its primary workload is a dr-platform durable workflow whose
dr-graph graph produces generated-code evaluation jobs continuously. V1
implements a narrow call-scoped slice of the
[target use cases](../high-level-plan/target-usecases.md) without making deferred
containment, supervision, or fleet behavior appear partially supported. Its
shared call-scoped foundation supports later use cases without introducing a
second execution engine.

## Decision and contract status

Repository-wide accepted vocabulary and behavior live in:

- [core terminology](../../.defs/terms.toml)
- [standing contracts](../../.defs/contracts.toml)

The v1 plan is classified against a frozen revision of those contracts. These
files are structured proposals rather than standing repository authority:

- [v1 terminology](terms.toml)
- [compatible contract extensions](contract-extensions.toml)
- [contract contradictions](contract-contradictions.toml)
- [new contracts](new-contracts.toml)
- [intentional scope exclusions](intentionally-out-of-scope.toml)
- [unaddressed contracts](unaddressed-contracts.toml)

Checked items in the [review discussion](review-discussion-topics.md) are
accepted high-level v1 decisions and are incorporated below. They control the
v1 plan where a structured proposal still conflicts with them. Unchecked items
remain unresolved; their implementation and API details must not be inferred
from provisional structured clauses.

The accepted public type and protocol design lives in this document and its
settled behavior is aligned in the structured v1 contracts and terms. The
remaining serialization discussion owns exact wire schemas, persisted literals,
and any required changes to `dr-serialize`; implementation planning must not
reopen the accepted execution topology implicitly.

## Scope and consumer map

V1 serves target use cases 1 and 2 and the captured-output subset of trusted-tool
use case 4. It supplies the job-oriented local scheduling needed by the initial
HumanEval workload without claiming the full dimension-aware batch contract of
use case 3. Multi-call trusted-tool aggregation is not part of the v1 API. V1
also supplies the library-owned fake and owns the production engine's spawn-path
tests.

The primary v1 platform is macOS. V1 provides local call-scoped execution,
captured payload output, process-group lifecycle management, explicit inherited
state, and durable local recording. It does not provide filesystem or network
sandboxing, aggregate RAM enforcement, full process-tree containment,
supervised ownership, interactive multi-process orchestration, or fleet
behavior.

Consumers that require a deferred surface stay on their own execution path
until that complete surface exists:

- build hooks and packaging tests need arbitrary cwd and passthrough output;
- repository provenance capture needs arbitrary cwd;
- long-lived development servers need supervision and readiness; and
- interactive multi-process harnesses need mid-run polling and rendezvous.

## Accepted execution boundaries

### Isolated host Python

V1 has one Python execution mode: isolated host Python. The executor resolves a
concrete host interpreter to an absolute path before spawn, records that path,
and invokes it as:

```text
<interpreter> -I -c <source>
```

The pinned shape supplies Python's isolated invocation behavior, including no
user site directory, no current/script directory on `sys.path`, and no
`PYTHON*` environment influence. The executor adds no Python-specific stdio
framing and injects no undeclared environment values.

This mode does not verify the interpreter or standard-library bytes, identify
an installed package closure, restrict filesystem or network reach, or provide
general hermetic execution. V1 therefore exposes no hermetic-runtime mode or
manifest-bearing provisioned runtime.

The next runtime boundary is the platform-specific, content-verified design in
the [verified uv-provisioned Python runtime plan](../future-plans/verified-python-runtime.md).

### Workload budgets and RAM

Every workload budget axis has one visible value: finite or explicitly
unbudgeted. `Budgets.unbudgeted()` constructs the v1 default, and every workload
axis defaults to that value until a caller supplies a meaningful finite bound.
There is no unset or inferred finite state.

The v1 workload axes include wall clock, input, output, memory, CPU time,
process count, file size, open-file count, and disk where represented by the
declaration model. Unsupported enforcement axes remain explicitly unbudgeted in
the effective declaration and durable record.

Platform-derived spawn validation, such as source and aggregate argv-plus-
environment limits, remains mandatory validation rather than a caller workload
budget. Executor self-budgets used to keep startup, pipe shutdown, and teardown
from hanging are also executor mechanics rather than workload defaults.

V1 performs no default or aggregate RAM enforcement and makes no RAM-protection
claim. A future machine-protection mechanism may add a faithfully enforceable
aggregate limit, but it cannot silently reinterpret the v1 unbudgeted value.

### Containment and process lifecycle

Filesystem and network sandboxing are out of scope for v1. Every untrusted
execution target requires explicit acknowledgment of the v1
process-boundary-only profile, which grants the payload the invoking user's
filesystem, network, credential, and process-spawning reach. The profile is an
honest reach declaration, not a security sandbox.

Each spawned run starts a fresh session and process group. Before returning, the
executor performs bounded group-targeted termination and escalation when
required and reaps the direct child. This guarantee reaches the original
process group only. A descendant that creates a new session can escape that
group and may survive; v1 does not claim otherwise.

The environment grant, fresh scratch cwd, closed descriptor table, direct argv
invocation, and workload budgets still constrain child-observable state. Those
properties are reproducibility and lifecycle controls, not filesystem or
network containment.

## Package navigation

The proposed package is `dr_exec`, consumed through pinned releases:

- `dr_exec.names` — nominal identifiers and other validated scalar names;
- `dr_exec.kinds` — persisted `StrEnum` vocabularies and discriminants;
- `dr_exec.protocols` — stable behavioral capability boundaries;
- `dr_exec.declare` — jobs, execution targets, budgets, environment grants,
  and containment declarations;
- `dr_exec.runtime` — the isolated-host-Python runtime implementation;
- `dr_exec.store` — the durable directory run-store implementation;
- `dr_exec.executor` — the production process executor;
- `dr_exec.pool` — bounded batch and streaming orchestration;
- `dr_exec.record` — results, records, record receipts, and narration;
- `dr_exec.fake` — the contract-enforcing consumer test fake; and
- `dr_exec.engine` — private call-scoped spawn, I/O, lifecycle, budget,
  attribution, and recording implementation.

The package root re-exports the deliberate public surface. Private engine,
wire-frame, canonicalization, and store-transaction helpers are not re-exported.
The single-engine boundary and pinned-release rule are proposed in
[new contracts](new-contracts.toml).

## Public type and protocol design

This section is the accepted v1 Python API shape. Exact persisted field names,
enum values, schema versions, and protocol-frame limits remain part of the
structured-contract and serialization pass, but implementation must preserve
the ownership and composition shown here.

The snippets are interface fragments grouped by conceptual ownership, not one
copy-paste module; implementations use normal imports and postponed annotations
where a referenced type is defined in a later subsection.

### Representation and naming rules

Validated models use one strict frozen base at persistence, subprocess,
untrusted-input, and fixture boundaries. Live internal values that are never
serialized use frozen slotted dataclasses. `ExecutionJob` is deliberately a
live dataclass because its resolved environment contains secret values; it is
never serialized wholesale. The engine derives safe record and child-protocol
models from it.

```python
class ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )
```

The public vocabulary consistently uses the standard short forms `Id`, `Env`,
`Config`, `Var`, `argv`, `stdin`, `stdout`, `stderr`, `max`, `cpu`, `ns`,
`sha256`, and `errno`. It does not shorten `Execution`, `Protocol`, `Record`,
`Result`, or `Capacity`. Nested fields omit context already supplied by their
owning type: `ExecutionId` contains `job_id` and `attempt_id`, while containing
models use the field name `execution_id`.

All closed persisted vocabularies use `@verify(UNIQUE)` `StrEnum` definitions.
Pydantic discriminated-union variants use the owning enum member inside the
required `Literal`, rather than duplicating a raw string:

```python
@verify(UNIQUE)
class OutcomeKind(StrEnum):
    EXITED = "exited"
    SIGNALED = "signaled"


class ExitedOutcome(ContractModel):
    kind: Literal[OutcomeKind.EXITED] = OutcomeKind.EXITED
    exit_code: int
```

The `StrEnum` owns the wire vocabulary; the `Literal` pins the one value valid
for that union member and enables Pydantic's field discriminator. Persisted
payloads are never constructed by iterating an enum.

### Protocols as governance boundaries

`Executor`, `Runtime`, and `RunStore` are stable behavioral Protocols in v1.
They intentionally freeze foundational capability shapes before multiple
production implementations exist. The normal extension path is a new concrete
implementation; adding or changing a Protocol is a loud boundary change that
requires explicit contract review.

Every supported implementation runs the shared behavioral conformance suite.
Structural conformance alone establishes only method shape, not semantic or
durability qualification. Protocols model swappable behavior; serialized
variants remain closed validated models or discriminated unions.

`ExecutionPool` remains one concrete class in v1. Its bounded admission,
backpressure, and scheduling behavior are the paved-road machine-utilization
policy rather than a swappable scheduling plug-in.

### Names and execution identity

```python
JobId = NewType("JobId", UUID)
AttemptId = NewType("AttemptId", UUID)


class ExecutionId(ContractModel):
    job_id: JobId
    attempt_id: AttemptId
```

The caller supplies the stable logical `JobId`. Each physical attempt receives
a new `AttemptId`, so retrying a workflow job never makes two executions appear
to be the same attempt.

### Runtime boundary

```python
@verify(UNIQUE)
class RuntimeKind(StrEnum):
    ISOLATED_HOST_PYTHON = "isolated_host_python"


class RuntimeRecord(ContractModel):
    kind: RuntimeKind
    resolved_executable: Path
    id_doc: IdentityDocument


@dataclass(frozen=True, slots=True)
class PreparedPythonProcess:
    argv: tuple[str, ...]
    request: IdentityDocument
    runtime_record: RuntimeRecord


class Runtime(Protocol):
    def prepare(
        self,
        target: UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        ...

    def describe(self) -> RuntimeRecord:
        ...


@dataclass(frozen=True, slots=True)
class IsolatedHostPythonRuntime:
    executable: Path

    def prepare(
        self,
        target: UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        ...

    def describe(self) -> RuntimeRecord:
        ...
```

`IsolatedHostPythonRuntime` resolves and validates its absolute executable at
construction. Its `prepare()` method always produces the fixed
`<executable> -I -c <driver_source>` form. Runtime implementations do not spawn
processes, choose budgets, resolve environment grants, or write records.

The future verified uv-provisioned runtime implements the same `Runtime`
Protocol. Conformance does not by itself add that implementation to the v1
support matrix.

### Durable run-store boundary

The store Protocol operates on nominal lifecycle handles so illegal state
transitions are not expressible through one loosely typed handle.

```python
@verify(UNIQUE)
class RecordState(StrEnum):
    PREPARED = "prepared"
    RUNNING = "running"
    FINALIZED = "finalized"


class TrustedCommandTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_COMMAND] = (
        ExecutionTargetKind.TRUSTED_COMMAND
    )
    canonical_declaration_sha256: str


class UntrustedCommandTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = (
        ExecutionTargetKind.UNTRUSTED_COMMAND
    )
    canonical_declaration_sha256: str
    containment_profile: ContainmentProfile


class UntrustedPythonTargetRecord(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = (
        ExecutionTargetKind.UNTRUSTED_PYTHON
    )
    canonical_declaration_sha256: str
    request_id_sha256: str
    containment_profile: ContainmentProfile
    runtime: RuntimeRecord


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
    started_at: AwareDatetime


class OutputArtifactRecord(ContractModel):
    relative_path: Path
    size_bytes: NonNegativeInt
    sha256: str


class OutputArtifactRecords(ContractModel):
    stdout: OutputArtifactRecord
    stderr: OutputArtifactRecord


class RetainedPayloadStreamRecord(ContractModel):
    head_bytes: NonNegativeInt
    tail_bytes: NonNegativeInt
    produced_bytes: NonNegativeInt
    dropped_bytes: NonNegativeInt


class PayloadOutputRecords(ContractModel):
    stdout: RetainedPayloadStreamRecord
    stderr: RetainedPayloadStreamRecord


class ExecutionResultRecord(ContractModel):
    execution_id: ExecutionId
    outcome: ExecutionOutcome
    attribution: ExecutionAttribution
    protocol_outputs: tuple[IdentityDocument, ...]
    payload_outputs: PayloadOutputRecords
    measurements: ExecutionMeasurements


class PreparedRecord(ContractModel):
    state: Literal[RecordState.PREPARED] = RecordState.PREPARED
    declaration: RunDeclaration


class RunningRecord(ContractModel):
    state: Literal[RecordState.RUNNING] = RecordState.RUNNING
    declaration: RunDeclaration
    process: ProcessRecord


class FinalizedRecord(ContractModel):
    state: Literal[RecordState.FINALIZED] = RecordState.FINALIZED
    declaration: RunDeclaration
    result: ExecutionResultRecord
    outputs: OutputArtifactRecords


type RunRecord = Annotated[
    PreparedRecord | RunningRecord | FinalizedRecord,
    Field(discriminator="state"),
]


@dataclass(frozen=True, slots=True)
class PreparedRun:
    execution_id: ExecutionId
    record_dir: Path


@dataclass(frozen=True, slots=True)
class RunningRun:
    execution_id: ExecutionId
    record_dir: Path


type FinalizableRun = PreparedRun | RunningRun


class RunStore(Protocol):
    def prepare(
        self,
        declaration: RunDeclaration,
        /,
    ) -> PreparedRun:
        ...

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        ...

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        ...

    def load(
        self,
        record_dir: Path,
        /,
    ) -> RunRecord:
        ...


@dataclass(frozen=True, slots=True)
class DirectoryRunStore:
    root: Path

    def prepare(
        self,
        declaration: RunDeclaration,
        /,
    ) -> PreparedRun:
        ...

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        ...

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        ...

    def load(
        self,
        record_dir: Path,
        /,
    ) -> RunRecord:
        ...
```

`DirectoryRunStore` is the only qualified v1 production implementation. It
implements the crash-consistent lifecycle described below. A prepare failure
prevents spawn and raises; after an attempt begins, finalization degradation is
returned through `RecordReceipt` without replacing the execution outcome.
`RunDeclaration` is the safe persisted projection of a live `ExecutionJob`: it
contains target and request identities rather than raw stdin, source, request
payloads, or environment values.

### Environment grants

The live grant owns resolved values and never serializes them directly.

```python
@verify(UNIQUE)
class EnvGrantKind(StrEnum):
    NONE = "none"
    NAMED = "named"
    FIXED = "fixed"
    OVERLAY = "overlay"


@dataclass(frozen=True, slots=True)
class EnvVar:
    name: str
    value: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class EnvGrant:
    kind: EnvGrantKind
    variables: tuple[EnvVar, ...]
    excluded_var_names: tuple[str, ...] = ()

    @classmethod
    def none(cls) -> EnvGrant:
        ...

    @classmethod
    def named(cls, var_names: Iterable[str]) -> EnvGrant:
        ...

    @classmethod
    def fixed(cls, variables: Mapping[str, str]) -> EnvGrant:
        ...

    @classmethod
    def overlay(
        cls,
        extra_variables: Mapping[str, str],
        *,
        exclusions: Iterable[str] = (),
    ) -> EnvGrant:
        ...


class EnvGrantRecord(ContractModel):
    kind: EnvGrantKind
    var_names: tuple[str, ...]
    excluded_var_names: tuple[str, ...]
    canonical_values_sha256: str
```

Parent-derived values are snapshotted when an `EnvGrant` is constructed.
Records contain names, exclusions, and the canonical value digest, never the
secret values.

### Workload budgets

An unbudgeted axis is a first-class value rather than `None`. Unit-specific
finite models prevent byte, duration, and count values from being exchanged.

```python
@verify(UNIQUE)
class LimitKind(StrEnum):
    UNBUDGETED = "unbudgeted"
    FINITE = "finite"


class UnbudgetedLimit(ContractModel):
    kind: Literal[LimitKind.UNBUDGETED] = LimitKind.UNBUDGETED


class FiniteByteLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_bytes: PositiveInt


class FiniteDurationLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_ns: PositiveInt


class FiniteCountLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_count: PositiveInt


type ByteBudget = Annotated[
    UnbudgetedLimit | FiniteByteLimit,
    Field(discriminator="kind"),
]
type DurationBudget = Annotated[
    UnbudgetedLimit | FiniteDurationLimit,
    Field(discriminator="kind"),
]
type CountBudget = Annotated[
    UnbudgetedLimit | FiniteCountLimit,
    Field(discriminator="kind"),
]
```

Output combines its finite limit with the selected overflow behavior:

```python
@verify(UNIQUE)
class OutputOverflowPolicy(StrEnum):
    FAIL = "fail"
    MARKED_TRUNCATION = "marked_truncation"


class UnbudgetedOutput(ContractModel):
    kind: Literal[LimitKind.UNBUDGETED] = LimitKind.UNBUDGETED


class StreamRetentionBudget(ContractModel):
    head_bytes: NonNegativeInt
    tail_bytes: NonNegativeInt


class PayloadRetentionBudget(ContractModel):
    stdout: StreamRetentionBudget
    stderr: StreamRetentionBudget


class FiniteOutput(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_bytes: PositiveInt
    overflow_policy: OutputOverflowPolicy
    retention: PayloadRetentionBudget


type OutputBudget = Annotated[
    UnbudgetedOutput | FiniteOutput,
    Field(discriminator="kind"),
]


class Budgets(ContractModel):
    wall_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    input_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    payload_output: OutputBudget = Field(
        default_factory=UnbudgetedOutput,
    )
    memory_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    cpu_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    process_count: CountBudget = Field(default_factory=UnbudgetedLimit)
    file_size_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    open_file_count: CountBudget = Field(default_factory=UnbudgetedLimit)
    disk_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)

    @classmethod
    def unbudgeted(cls) -> Budgets:
        return cls()
```

Unsupported finite axes fail declaration validation before spawn. They are
never accepted or silently treated as unbudgeted.

`FiniteOutput` validates that `max_bytes` equals the sum of the four declared
head and tail allocations. This pins deterministic stdout/stderr retention
independently of drain scheduling. If aggregate payload production does not
exceed `max_bytes`, the result retains every produced byte rather than padding
or forcing the configured segment split. In `FAIL` mode the same finite total
is the termination threshold; in `MARKED_TRUNCATION` mode it is the maximum
retained-byte total while the engine continues draining and counting through
EOF.

### Execution targets and jobs

`ExecutionTarget` is the complete structured target, while
`ExecutionTargetKind` is only its classification.

```python
@verify(UNIQUE)
class ExecutionTargetKind(StrEnum):
    TRUSTED_COMMAND = "trusted_command"
    UNTRUSTED_COMMAND = "untrusted_command"
    UNTRUSTED_PYTHON = "untrusted_python"


@verify(UNIQUE)
class ContainmentProfile(StrEnum):
    PROCESS_BOUNDARY_ONLY = "process_boundary_only"


class TrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_COMMAND] = (
        ExecutionTargetKind.TRUSTED_COMMAND
    )
    argv: tuple[str, ...]
    stdin: bytes = b""


class UntrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = (
        ExecutionTargetKind.UNTRUSTED_COMMAND
    )
    argv: tuple[str, ...]
    stdin: bytes = b""
    containment_profile: ContainmentProfile


class UntrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = (
        ExecutionTargetKind.UNTRUSTED_PYTHON
    )
    driver_source: str
    request: IdentityDocument
    containment_profile: ContainmentProfile


type ExecutionTarget = Annotated[
    TrustedCommandTarget
    | UntrustedCommandTarget
    | UntrustedPythonTarget,
    Field(discriminator="kind"),
]


@dataclass(frozen=True, slots=True)
class ExecutionJob:
    job_id: JobId
    target: ExecutionTarget
    env: EnvGrant
    budgets: Budgets = field(default_factory=Budgets.unbudgeted)
```

`IdentityDocument` is supplied by `dr-serialize`. The owning domain chooses its
schema, schema version, and complete request payload after using the selected
`dr-serialize` conversion policy. The logical workflow persists its domain
request; the worker resolves live environment values and constructs the
`ExecutionJob` immediately before admission.

### Results and outcomes

```python
class RetainedPayloadStream(ContractModel):
    head: bytes
    tail: bytes
    produced_bytes: NonNegativeInt
    dropped_bytes: NonNegativeInt


class PayloadOutputs(ContractModel):
    stdout: RetainedPayloadStream
    stderr: RetainedPayloadStream
```

The raw outcome is separate from its evidence-based owner:

```python
@verify(UNIQUE)
class OutcomeKind(StrEnum):
    EXITED = "exited"
    SIGNALED = "signaled"
    SPAWN_ABSENT = "spawn_absent"
    SPAWN_FAILED = "spawn_failed"
    BUDGET_EXCEEDED = "budget_exceeded"
    PROTOCOL_FAILED = "protocol_failed"
    CANCELLED = "cancelled"


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


@verify(UNIQUE)
class BudgetAxis(StrEnum):
    WALL_TIME = "wall_time"
    INPUT_BYTES = "input_bytes"
    PAYLOAD_OUTPUT = "payload_output"
    MEMORY_BYTES = "memory_bytes"
    CPU_TIME = "cpu_time"
    PROCESS_COUNT = "process_count"
    FILE_SIZE_BYTES = "file_size_bytes"
    OPEN_FILE_COUNT = "open_file_count"
    DISK_BYTES = "disk_bytes"


class BudgetExceededOutcome(ContractModel):
    kind: Literal[OutcomeKind.BUDGET_EXCEEDED] = (
        OutcomeKind.BUDGET_EXCEEDED
    )
    axis: BudgetAxis


@verify(UNIQUE)
class ProtocolFailureCode(StrEnum):
    MALFORMED_FRAME = "malformed_frame"
    OVERSIZED_FRAME = "oversized_frame"
    UNEXPECTED_FRAME = "unexpected_frame"
    ID_MISMATCH = "id_mismatch"
    DUPLICATE_OUTPUT = "duplicate_output"
    INCOMPLETE_STREAM = "incomplete_stream"


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


@verify(UNIQUE)
class FailureOwner(StrEnum):
    NONE = "none"
    PAYLOAD = "payload"
    EXECUTOR = "executor"
    MACHINE = "machine"


class ExecutionAttribution(ContractModel):
    owner: FailureOwner
    detail: str | None = None


class ExecutionMeasurements(ContractModel):
    started_at: AwareDatetime
    finished_at: AwareDatetime
    duration_ns: NonNegativeInt
    teardown_duration_ns: NonNegativeInt
    protocol_bytes_received: NonNegativeInt


class ExecutionResult(ContractModel):
    execution_id: ExecutionId
    outcome: ExecutionOutcome
    attribution: ExecutionAttribution
    protocol_outputs: tuple[IdentityDocument, ...]
    payload_outputs: PayloadOutputs
    measurements: ExecutionMeasurements
```

The protected protocol may preserve multiple accepted incremental outputs. A
HumanEval execution ordinarily returns one aggregate output document containing
all per-test outcomes.

### Record receipts and completed executions

```python
@verify(UNIQUE)
class RecordReceiptKind(StrEnum):
    COMPLETE = "complete"
    DEGRADED = "degraded"
    NOT_APPLICABLE = "not_applicable"


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
    CompleteRecordReceipt
    | DegradedRecordReceipt
    | FakeRecordReceipt,
    Field(discriminator="kind"),
]


class CompletedExecution(ContractModel):
    result: ExecutionResult
    record_receipt: RecordReceipt
```

`CompletedExecution` validates that the result and receipt carry the same
`ExecutionId`. `ProcessExecutor` can return only complete or degraded record
receipts. `FakeRecordReceipt` exists solely so the contract-enforcing fake can
satisfy `Executor` without pretending that a run record exists; it is not a
production no-record option.

### Executor Protocol and production implementation

The stable executor capability contains one blocking, thread-safe operation:

```python
class Executor(Protocol):
    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        ...
```

The production implementation is immutable and has exactly two public fields:

```python
@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    runtime: Runtime
    run_store: RunStore

    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        ...

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]:
        ...

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        ...
```

`run()` performs one complete attempt: validate, prepare the record, create the
scratch workspace, launch one fresh child, exchange protocol messages, capture
payload output, enforce budgets, tear down and reap, finalize the record, and
return. Mutable per-run process state never lives on `ProcessExecutor`.

Recognized spawn, child, budget, and protocol outcomes return data. Invalid
pre-spawn declarations and machinery failures that prevent a trustworthy result
raise typed exceptions. A record-prepare failure prevents spawn; later
recording degradation remains in `RecordReceipt`.

```python
class DeclarationError(ValueError):
    ...


class ExecutorFailure(RuntimeError):
    ...
```

`DeclarationError` is exclusively pre-spawn. `ExecutorFailure` preserves its
underlying cause and is reserved for machinery failure that prevents a
trustworthy `ExecutionResult`; recognized process and protocol failures do not
use exception control flow.

`run_many()` and `open_pool()` are convenience methods on the concrete
production implementation, not additional methods on the foundational
`Executor` Protocol. Both delegate to the same `ExecutionPool`, which delegates
every job to `Executor.run()`. `FakeExecutor` satisfies the same one-method
Protocol and may therefore be used under the pool in consumer logic tests.

The fake's mutable script and call history remain private and synchronized:

```python
class FakeExecutor:
    _responses: deque[CompletedExecution]
    _calls: list[ExecutionJob]
    _lock: Lock

    def __init__(
        self,
        responses: Iterable[CompletedExecution] = (),
    ) -> None:
        ...

    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        ...

    @property
    def calls(self) -> tuple[ExecutionJob, ...]:
        ...
```

### Pool capacity and lifecycle

```python
@dataclass(frozen=True, slots=True)
class AutoPoolCapacity:
    pass


@dataclass(frozen=True, slots=True)
class FixedPoolCapacity:
    max_active_jobs: int

    def __post_init__(self) -> None:
        if self.max_active_jobs < 1:
            raise ValueError("max_active_jobs must be positive")


type PoolCapacity = AutoPoolCapacity | FixedPoolCapacity


@dataclass(frozen=True, slots=True)
class ExecutionPoolConfig:
    capacity: PoolCapacity = field(default_factory=AutoPoolCapacity)
    max_prefetched_jobs: int = 0

    def __post_init__(self) -> None:
        if self.max_prefetched_jobs < 0:
            raise ValueError("max_prefetched_jobs must be nonnegative")


@verify(UNIQUE)
class CapacitySource(StrEnum):
    AUTO = "auto"
    FIXED = "fixed"


class EffectivePoolCapacity(ContractModel):
    source: CapacitySource
    cpu_count: PositiveInt
    max_active_jobs: PositiveInt
    max_prefetched_jobs: NonNegativeInt
    native_threads_per_job: Literal[1] = 1


@verify(UNIQUE)
class ExecutionPoolState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"
    BROKEN = "broken"
```

`AutoPoolCapacity` resolves once when a pool opens as the usable CPU count, with
at least one active job. Every v1 job has one scheduling slot; heterogeneous
weighted jobs are outside v1. The effective values and fixed single-native-
thread policy are recorded. `ProcessExecutor.run()` adds the standard numeric-
library thread-limit variables to every submitted job's resolved `EnvGrant`,
whether called directly or through a pool. An incompatible caller-supplied
value is a declaration error rather than an implicit override. This is an
oversubscription control for known libraries, not a CPU-enforcement guarantee
against arbitrary payload code.

The default `max_prefetched_jobs=0` leaves the authoritative backlog in the
caller or durable workflow queue. A finite nonzero value admits only that many
additional jobs.

### Streaming context and `ExecutionPool`

Caller context is carried locally and never serialized by dr-exec:

```python
ContextT = TypeVar("ContextT")


@dataclass(frozen=True, slots=True)
class ExecutionSubmission(Generic[ContextT]):
    job: ExecutionJob
    context: ContextT


@dataclass(frozen=True, slots=True)
class ExecutionCompletion(Generic[ContextT]):
    completed_execution: CompletedExecution
    context: ContextT
```

`ExecutionPool` is mutable lifecycle state and therefore is not a dataclass:

```python
class ExecutionPool:
    _executor: Executor
    _config: ExecutionPoolConfig
    _effective_capacity: EffectivePoolCapacity | None
    _state: ExecutionPoolState
    _scheduler: _ExecutionScheduler | None

    def __init__(
        self,
        *,
        executor: Executor,
        config: ExecutionPoolConfig,
    ) -> None:
        ...

    @property
    def effective_capacity(self) -> EffectivePoolCapacity:
        ...

    async def __aenter__(self) -> ExecutionPool:
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        ...

    async def run_stream(
        self,
        submissions: AsyncIterable[ExecutionSubmission[ContextT]],
        /,
    ) -> AsyncIterator[ExecutionCompletion[ContextT]]:
        ...

    async def drain(self) -> None:
        ...

    async def abort(self) -> None:
        ...
```

For every available slot, `run_stream()` requests one submission, calls the
blocking `Executor.run()` under one bounded parent-side supervisor, yields the
completion in completion order, and then requests the next submission. It does
not eagerly consume the async iterable. Completed-result buffering is also
bounded, so a slow consumer eventually applies backpressure.

Each execution gets a fresh child process. The pool reuses scheduling capacity,
not interpreters or child processes, and it never adds a `ProcessPoolExecutor`
around the subprocess engine. Ordinary per-job failures remain completion data
and do not terminate the stream; a scheduler-wide failure breaks the pool.

Normal closure stops intake and drains active jobs. `abort()` stops intake and
terminates active process groups under the accepted v1 lifecycle contract. A
closed pool cannot reopen.

`run_many()` adapts a finite lazy iterable into this same scheduler, drains it,
and closes the pool. It returns results in completion order with stable job IDs;
it never materializes the entire input.

## Core engine behavior

The following details remain v1 obligations independent of the final public
signatures:

- Command resolution uses the granted environment's `PATH`. With no granted
  `PATH`, only an absolute executable resolves; a relative executable is a
  pre-spawn declaration error. Spawn-time `ENOENT` is spawn absence, while
  other spawn errors preserve their errno and receive machine attribution.
- Attribution is selected after teardown in this order: spawn absence, output
  budget, wall-clock budget, then exit-status interpretation. A recorded output
  violation wins a race with the deadline or a clean exit.
- Duration covers spawn through reap on a monotonic clock, parent setup is
  excluded, teardown duration is separate, and record timestamps are UTC.
  Output measurements count bytes produced after the retention limit so an
  overflowing run still provides sizing evidence.
- Scratch cleanup runs on every exit path. A cleanup failure is narrated and
  executor-attributed without replacing an otherwise trustworthy run result.
- Persisted digests use SHA-256 over explicitly canonicalized bytes.
  Environment identity includes sorted declared names and a digest of the
  canonical name/value payload without persisting the values themselves.
- Executor identity has distinct production and fake forms. Exact persisted
  keys, literals, canonicalization, and identity strings belong in validated
  serialization models and golden tests rather than being derived from mutable
  code field names.

## Payload retention and protocol integrity

V1 captures and returns payload stdout and stderr as separate raw byte streams.
It adds no in-band framing, decoding, or newline normalization. Caller-owned
artifact paths are recorded separately and are not an executor output-delivery
mode. Executor narration remains on a parent-owned channel and never shares
payload stdout or stderr.

Unbudgeted output retains every produced byte. When a finite payload-output
budget uses marked truncation, each stream retains a deterministic head and
tail and reports at least:

```text
head
tail
produced_bytes
dropped_bytes
```

The retained pieces remain structurally separate. The executor does not insert
a marker or concatenate them as if the omitted bytes had never existed. The
exact allocation of a finite aggregate output budget between stdout head,
stdout tail, stderr head, and stderr tail is pinned by the later API design and
cannot depend on drain-thread scheduling. The fail-on-overflow action may
terminate the run, while marked truncation continues draining through EOF; both
return the retained structural evidence and exact produced/dropped counts.

Execution protocol output is a separate driver-owned channel with separate
accounting. Its framed JSON or NDJSON prelude, output, and completion messages
must remain complete and schema-valid. Protocol bytes are never head/tail
truncated. An oversized, malformed, identity-mismatched, duplicate, or
incomplete stream is an executor protocol failure; the parent preserves every
previously accepted complete protocol output. The owning domain decides whether
those outputs constitute a complete internal result and never relies on dr-exec
to synthesize missing domain items. Payload output can therefore overflow
without corrupting trusted structured outputs.

Per-field and per-message protocol limits prevent a payload-derived value from
turning one result envelope into unbounded protocol output. The exact message
schema and limits are persisted wire contracts with golden tests.

## Durable observability and record layout

Every real run writes through one concrete `DirectoryRunStore`. Recording is
part of the production engine path, not a caller-selected delivery mode. A fake
call creates no run record, while tests of the real engine point the same store
implementation at temporary test-owned storage.

### On-disk shape

The initial store uses one collision-free directory per run:

```text
<record-root>/
  run-<utc-timestamp>-<uuid>/
    record.json
    stdout.bin
    stderr.bin
```

`record.json` is a versioned JSON manifest. The output sidecars contain the
retained payload evidence and are a recording representation, not the public
spooled-output delivery mode. When truncation occurs, each sidecar stores its
stream's head followed by its tail; the manifest records both segment lengths,
so readers never mistake their concatenation for a contiguous payload stream.
Manifest paths are relative to the run directory; normal finalization records
each sidecar's size and content digest.

The manifest contains the run identifier, schema and executor identities,
trust category, invocation or source digest, input digest, environment-grant
identity, containment profile, complete effective budgets, resolved Python
interpreter when relevant, lifecycle timestamps, outcome, attribution,
measurements, truncation metadata, and output sidecar references and digests.
Secret environment values and raw input do not enter the manifest merely
because their digests do.

### Crash-consistent lifecycle

The store publishes three valid lifecycle states:

1. `prepared` is committed before spawn and records the complete declaration.
2. `running` is committed after successful spawn.
3. `finalized` is committed after teardown and sidecar finalization.

Spawn absence or another recognized pre-child outcome finalizes directly from
`prepared`. An abrupt parent death leaves the latest successfully published
state intact: `prepared` means spawn completion is unknown, `running` means a
child started without a trustworthy final outcome, and `finalized` is the only
complete-run state. Recovery reports an incomplete record as incomplete; it
does not infer success from sidecars or manufacture a final outcome.

Every state transition writes a complete temporary manifest in the run
directory, flushes it, and replaces `record.json` atomically on the same local
filesystem. Normal finalization flushes retained output sidecars before
publishing the manifest that references their final digests. The macOS path
uses [`F_FULLFSYNC`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
where available before the
[same-filesystem atomic replacement](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html).

The v1 durability claim applies to supported local macOS filesystems. Network
mounts, cloud-synchronized directories, and filesystems without the required
flush and atomic-replace behavior are outside that claim until separately
qualified.

The accepted guarantee is:

> Every successfully published lifecycle state is valid and crash-consistent;
> normal finalization leaves digest-verified retained output artifacts; abrupt
> termination leaves a valid visibly incomplete record; and any recording
> degradation visible to the caller is machine-readable.

No storage API can promise that permissions, capacity, the operating system,
or hardware never fail. A record or sidecar failure therefore does not replace
the execution outcome or stop required pipe draining. It produces a degraded
`RecordReceipt`, preserves the last valid on-disk lifecycle state when one
exists, and emits separate executor narration. The receipt carries the run
identity, record location when allocated, latest known state, completion or
degradation status, and structured recording failure details.

Narration remains separate from payload streams and is verbose by default. V1
does not require the complete narration log to be a fourth durable artifact;
the manifest, sidecars, and machine-readable receipt own the durable contract.
Narration failure is reported as observability degradation rather than payload
failure.

The record manifest's canonical JSON and digest construction should use
`dr-serialize` if the open serialization discussion establishes the required
stable contract. V1 does not create a second general canonicalization system in
`dr-exec` merely for records.

### Durable-observability verification

The real directory-store suite covers:

- valid `prepared`, `running`, and `finalized` state transitions;
- abrupt parent death after an explicit committed-state event, leaving a valid
  incomplete record;
- atomic finalization with retrievable digest-matching sidecars;
- head/tail retained-output recovery and exact produced/dropped counts;
- unwritable, exhausted, and failed-finalization stores returning degraded
  receipts without changing execution attribution;
- concurrent writers with collision-free run directories;
- malformed or mismatched manifests and sidecars failing validation; and
- successful and failed real runs both producing records.

Concurrency and abrupt-death tests synchronize on explicit store events and
terminal outcomes. Timeouts are watchdogs only; sleeps and elapsed time are not
evidence that a lifecycle state was committed. Real-engine tests use temporary
directories on the supported filesystem and clean them through test fixture
ownership. Pure serialization units may use buffers, but buffers alone do not
prove filesystem durability.

## Batch, streaming, and fake details

### High-throughput acceptance criterion

High-throughput local evaluation is a primary v1 product requirement, not a
consumer-owned optimization. The representative target is a stream of roughly
100,000 independently generated code samples, each evaluated against a test
suite of roughly 1,000 cheap cases, on the supported Mac mini.

The paved path must let setup and interpreter startup amortize across the cases
that share one generated sample while evaluating many independent samples with
bounded machine-level concurrency. On a representative workload, local
evaluation capacity must exceed the rate at which the upstream LLM pipeline
produces samples, without unbounded active processes, threads, memory growth,
queued requests, or lost per-sample results and records.

V1 fails this criterion if its default topology starts a container or
provisioned environment per sample, starts a process per test case, evaluates
the outer sample stream sequentially, materializes the full sweep before making
progress, or requires every consumer to reconstruct admission and concurrency
control.

The release benchmark records samples per second, cases per second, active and
queued request high-water marks, child-startup share, peak memory, and the
effective concurrency configuration. The representative test-suite cost and
upstream sample-production rate are pinned with the benchmark so the acceptance
claim is reproducible rather than inferred from synthetic process-start timing
alone.

### Domain-free execution unit

One `ExecutionJob` is one independently schedulable sharing boundary. The
caller decides which work benefits from one setup, runtime, scratch workspace,
and child lifetime; dr-exec decides how many independent jobs should be active.
The pool never interprets domain concepts such as generated samples, compilers,
or test cases.

The protected Python driver retains its protocol-output handle before consumer
code can replace `sys.stdout`; direct file-descriptor writes remain a declared
hole of the process-boundary-only profile. The driver emits complete validated
outputs incrementally. It represents load-phase failure as a complete domain
output when the domain contract supports that representation; otherwise the
stream ends as an executor protocol failure. Dr-exec preserves accepted outputs
but never manufactures per-item domain results. These choices preserve
paid-for partial work without mixing protocol output with payload stdout or
stderr.

Every `ExecutionJob` receives one fresh child. Work inside that job may be
sequential so expensive setup is paid once. Persistent children reused across
unrelated jobs are outside v1 because untrusted code can poison interpreter and
process state.

### Finite batch execution

`ProcessExecutor.run_many()` accepts a lazy iterable of `ExecutionJob`s. It
opens one `ExecutionPool`, admits only its active capacity plus bounded
prefetch, yields `CompletedExecution`s in completion order, drains the finite
input, and closes. One job failure does not fail fast or erase other results.

Conceptually:

```python
results = executor.run_many(execution_jobs)
```

A caller may produce hundreds of thousands of jobs without materializing the
collection or creating one future, thread, or process per job.

### HumanEval execution shape

The HumanEval adapter constructs one job per generated code sample and complete
test suite. Inside that job's fresh child, domain code:

1. compiles or loads the generated sample once;
2. runs all test cases sequentially against that loaded sample;
3. aggregates the per-test outcomes; and
4. returns one identity-bearing HumanEval result document.

Different samples are separate jobs and therefore run concurrently. Individual
tests are not executor scheduling units. This gives sequential setup-sharing
within one sample and bounded parallelism across samples.

### Durable streaming worker

A long-lived evaluation worker keeps one `ExecutionPool` open and connects it
to dr-platform's durable workflow queue:

```python
async def submissions():
    while True:
        lease = await platform.lease_next("human-eval")
        yield ExecutionSubmission(
            job=human_eval.execution_job(lease.payload),
            context=lease,
        )


async with process_executor.open_pool() as pool:
    async for completion in pool.run_stream(submissions()):
        await platform.complete_evaluation(
            lease=completion.context,
            result=human_eval.interpret(
                completion.completed_execution.result,
            ),
        )
```

`run_stream()` asks the source for a lease only when local admission capacity
exists. dr-platform owns the durable backlog, leases, lease renewal, retry
policy, workflow transitions, and idempotent result publication. The worker
owns translation between workflow payloads and dr-exec types. dr-exec owns
local bounded execution and durable per-attempt records.

The integration assumes at-least-once delivery. Stable `JobId`s, distinct
`AttemptId`s, and idempotent workflow completion prevent a retried lease from
being confused with exactly-once physical execution.

V1 runs one machine-level pool per host. Multiple source loops may feed that
pool, but multiple pools must receive explicit non-overlapping capacity
allocations; they cannot each independently claim the host's automatic
capacity.

`FakeExecutor` supports behavior selected from the complete declaration, with
an in-order queue as a convenience. It does not execute payloads, create scratch
workspaces, or create run records. Consumer logic tests use the fake;
production-engine and consumer parity oracles use the real engine with a
temporary `DirectoryRunStore`. The fake validates declarations, records the
immutable jobs it receives for assertion, returns scripted results with
`FakeRecordReceipt`, and satisfies the same thread-safe `Executor` Protocol.

The engine suite owns lifecycle fault-injection, process-group reach,
output-retention, protocol-integrity, bounded-pool, and durable-recording cases.
Domain adapters own request and result schemas, internal item completeness, and
the interpretation of `ExecutionResult` for their workflows.

## Open high-level decisions

One design area remains open before structured-contract alignment and
implementation planning: settle complete serialization ownership. V1 retains
JSON or NDJSON for control and incremental protocol data, uses domain formats
for bulk artifacts, and builds on `dr-serialize`; the remaining discussion must
identify any missing general serialization capability that belongs in
`dr-serialize` rather than being reimplemented in dr-exec.

The machine-utilization topology and public type and Protocol design are
accepted above. An implementation plan must not reopen them implicitly or
introduce a second scheduling unit alongside `ExecutionJob`.

## Future design hooks

The following extensions remain outside v1:

- the [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md);
- filesystem or network containment profiles with concrete platform backends;
- a declared-cwd grant;
- public spooled and stdio-passthrough output delivery;
- resource enforcement for work internal to one `ExecutionJob`;
- caller-meaningful per-stream budget declarations beyond the deterministic v1
  aggregate retention allocation;
- high-volume record indexing, sharding, and retention management; and
- supervised, interactive, and fleet execution.

These are design directions, not compatibility guarantees. Any extension must
preserve the single-engine boundary and revise the relevant standing and
structured contracts before implementation.
