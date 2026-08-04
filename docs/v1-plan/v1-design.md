# dr-exec v1 design

## Accepted boundary decisions

V1 uses role-specific identity documents for semantic identities and nominal
digests for opaque value-sensitive material. Run manifests remain
execution-local and contain no pool-admission snapshot or pool-session
reference. Workload and executor self-budget axes are all finite or explicitly
unbudgeted, and every axis defaults to unbudgeted without adaptive or hidden
finite limits. The detailed serialization additions are specified in
[dr-serialize additions](dr-serialize-additions.md).

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

The accepted public type and protocol design lives in this document and its
settled behavior is aligned in the structured v1 contracts and terms. The
serialization ownership and proposed shared-library additions are recorded
below and in the [dr-serialize additions](dr-serialize-additions.md). The v1
high-level planning decisions are closed; implementation planning must preserve
these boundaries while fleshing out the exact APIs and test vectors.

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
budget. Executor self-budget axes are distinct executor mechanics, but they use
the same finite-or-explicitly-unbudgeted representation and also default to
unbudgeted. V1 has no adaptive or hidden finite executor limits.

V1 performs no default or aggregate RAM enforcement and makes no RAM-protection
claim. A future machine-protection mechanism may add a faithfully enforceable
aggregate limit, but it cannot silently reinterpret the v1 unbudgeted value.
Likewise, an unbudgeted executor axis promises no policy limit rather than
unlimited machine capacity: memory exhaustion, disk exhaustion, operating-
system limits, and machinery failure remain possible and observable.

### Containment and process lifecycle

Filesystem and network sandboxing are out of scope for v1. Every untrusted
execution target requires explicit acknowledgment of the v1
process-boundary-only profile, which grants the payload the invoking user's
filesystem, network, credential, and process-spawning reach. The profile is an
honest reach declaration, not a security sandbox.

Each spawned run starts a fresh session and process group. When the executor
returns a completed run, it has completed the configured group-targeted
termination policy and reaped the direct child. A finite termination or join
self-budget supplies escalation and return deadlines; the unbudgeted default
may wait indefinitely and therefore carries no bounded-return guarantee. This
reach extends only to the original process group. A descendant that creates a
new session can escape that group and may survive; v1 does not claim otherwise.

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

## Serialization ownership and integration

Dr-exec v1 builds on a released, pinned version of `dr-serialize`; it does not
copy or fork that package's canonicalization and identity behavior. It also
pins Pydantic because Pydantic's JSON-mode conversion is part of the bytes that
dr-exec subsequently validates and canonicalizes. The required additions and
their qualification criteria are specified in
[dr-serialize additions](dr-serialize-additions.md).

The ownership boundary is:

| Owner | V1 responsibility |
| --- | --- |
| `dr-serialize` | Strict validation of materialized JSON values; canonical JSON text and bytes; `IdentityDocument` canonicalization and hashing; bounded strict decoding of one complete JSON value; and the validated full SHA-256 value. |
| `dr-exec` | Contract models and scalar spellings; secret-safe projections; execution identity payloads; request and protocol frame schemas; frame scanning and state; lifecycle records; sidecar references; path safety; and atomic store publication. |
| Domain adapters | Request and result `IdentityDocument` schemas, domain completeness rules, bulk artifact formats, and interpretation of accepted protocol outputs. |

`Serializer.to_jsonable()` is not used for request data, protocol frames,
records, identity material, raw payload bytes, or secret-bearing values. Its
normalization behavior is intentionally lossy and therefore cannot define a
wire, identity, or persistence contract.

### V1 identity roles

V1 deliberately starts with a loose, versioned hybrid identity scheme:

- Executor, executor-config, runtime, domain-request, and protocol-output
  identities are `IdentityDocument`s because their schema and version explain
  the identity's meaning.
- Target declarations and canonical environment values use nominal full
  SHA-256 digests. Their records carry safe non-secret structure beside the
  digest rather than wrapping opaque or secret-derived values in another
  identity document.

The production executor document uses schema `dr_exec.executor`, schema version
1, and payload keys `kind`, `package_version`, `source_commit`, `source_state`,
and `session_id`. `kind` is `process_executor`. `source_commit` is the complete
Git object ID embedded at package-build time when possible; an editable-source
fallback inspects the package source checkout, never the process cwd, and
snapshots the result when `ProcessExecutor` is constructed. `source_state` is
one of `clean`, `dirty`, or `unknown`. Dirty or unknown source receives an
executor-construction `session_id` so distinct unverified source states do not
compare equal merely because they share a commit or package version.

The executor-config document uses schema `dr_exec.executor_config`, schema
version 1, and a payload containing the complete effective
`ExecutorSelfBudgets`. This keeps source provenance distinct from policy that
can change whether the same execution succeeds. Every v1 default axis is
recorded explicitly as unbudgeted.

The isolated-host runtime document uses schema
`dr_exec.isolated_host_python_runtime`, schema version 1, and payload keys
`kind`, `resolved_executable`, `implementation`, `python_version`, `cache_tag`,
and `platform`. `IsolatedHostPythonRuntime` probes those facts once from the
selected interpreter under `-I` when constructed. They distinguish ordinary
host-runtime changes but do not verify interpreter bytes, the standard library,
or installed packages.

Domain adapters own the schema, version, complete payload, and change rules for
request and protocol-output documents. Adding an identity-bearing field changes
the owning document's schema version; v1 fields never grow silently under the
same version.

### Validated write and read paths

The canonical write path is:

```text
dr-exec or domain boundary model
  -> explicit secret-safe Pydantic JSON-mode projection
  -> dr-serialize strict JSON validation
  -> dr-serialize canonical JSON bytes
  -> protected protocol write or DirectoryRunStore transaction
```

The validated read path is:

```text
bounded bytes acquired by dr-exec
  -> dr-serialize bounded strict JSON decode
  -> dr-exec Pydantic model or frame validation
  -> dr-exec protocol, identity, and lifecycle validation
```

Dr-exec bounds bytes before decoding. The shared decoder owns general JSON
failures such as invalid UTF-8, duplicate keys, non-finite numbers, malformed
or trailing data, and depth overflow. Dr-exec translates those failures into
its closed protocol or record-load taxonomy and separately enforces frame
grammar, message order, aggregate limits, identities, and lifecycle meaning.

`model_dump_json()` is not the canonical wire or persistence format. Pydantic
converts a validated model into its explicit JSON-mode projection;
`dr-serialize` validates that value and produces the canonical UTF-8 bytes.
Golden tests pin the resulting UUID, path, timestamp, duration, byte, Unicode,
integer, enum, and digest spellings under the exact pinned dependency versions.
Persisted keys and discriminants are explicit literals, not values derived by
iterating enums or reflecting over implementation field names.

### Application to v1 boundaries

- The Python request is one canonical `IdentityDocument` on stdin followed by
  EOF. Each protected protocol frame is one canonical JSON object followed by
  LF on the executor-owned descriptor. Both use closed dr-exec models and the
  shared decoder and canonical-byte path. A finite executor self-budget supplies
  a protocol limit; the default supplies none.
- Request and result documents use `IdentityDocument` only where the owning
  schema, version, and payload are themselves the boundary. Dr-exec defines
  the role-specific payload model and completeness checks; `dr-serialize`
  supplies only the generic document envelope and canonical identity digest.
- `record.json` is a closed versioned dr-exec model serialized through the same
  canonical-byte path. Lifecycle-state validation occurs after strict decode
  and model validation.
- Payload stdout and stderr remain raw bytes. `DirectoryRunStore` writes their
  retained segments directly, records exact lengths, and hashes the raw
  sidecar bytes with streaming SHA-256. They never pass through JSON
  normalization.
- Caller-owned bulk formats remain outside both libraries' generic JSON lane.
  A domain adapter records their declared media type, size, digest, and schema
  identity where its contract requires them.

### Dependency and implementation order

Serialization implementation proceeds in this order:

1. Add and adversarially qualify the required general capabilities in
   `dr-serialize`, preserving all existing canonical text and digest results.
2. Release `dr-serialize`, then pin that release and the selected Pydantic
   release in dr-exec.
3. Implement dr-exec boundary models, safe projections, scalar goldens, and
   role-specific identities.
4. Implement and qualify the request transport and protected protocol codec
   and state machine.
5. Implement and qualify the lifecycle manifest, sidecars, and
   `DirectoryRunStore` transaction protocol.
6. Run end-to-end conformance tests across canonical bytes, protocol failures,
   partial outputs, crash-consistent records, and domain-adapter completeness.

No dr-exec codec or record implementation is complete while it depends on an
unreleased sibling checkout or locally duplicates a proposed `dr-serialize`
capability.

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

### Scalar wire spellings

V1 owns explicit dependency-independent scalar spellings. Pydantic converts
validated types into these JSON-mode values, and dr-serialize supplies the
canonical JSON escaping and bytes:

- UUIDs are lowercase 36-character hexadecimal strings with hyphens in the
  `8-4-4-4-12` form.
- Absolute and relative paths are POSIX path strings. Resolved executable paths
  are absolute; run-artifact references are normalized relative paths that
  contain no empty, `.` or `..` component. Serialization never resolves a path
  or follows a symlink.
- Timestamps are UTC RFC 3339 strings with a trailing `Z` and exactly six
  fractional-second digits. Non-UTC and naive datetimes fail validation rather
  than being normalized silently.
- Durations are integer nanoseconds in fields suffixed `_ns`; ISO 8601 duration
  strings and floating-point seconds are not wire forms.
- Bytes in JSON-bearing models are padded RFC 4648 URL-safe base64 strings.
  Command stdin, payload-output, and sidecar transport bytes retain their
  separately specified byte contracts and are not base64-wrapped in transit.
- Unicode strings preserve their code-point sequence without normalization.
  Dr-serialize's canonical JSON profile determines escaping, including its
  ASCII-only wire representation.
- Integers use JSON integer syntax with no leading plus, leading zero, exponent,
  or fractional form. Boolean values never validate as integers at strict model
  boundaries.
- Enums use their exact pinned `StrEnum` values.
- SHA-256 digests are exactly 64 lowercase hexadecimal characters without a
  prefix; abbreviated display digests are never accepted at a boundary.

Golden vectors cover each scalar alone and nested in every relevant request,
frame, identity, and record model. The vectors run under the exact pinned
Pydantic and dr-serialize releases so a dependency change cannot silently alter
the bytes.

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

### Python request and protocol transports

An untrusted Python child has four inherited descriptors: stdin on fd 0,
payload stdout on fd 1, payload stderr on fd 2, and the executor-owned protocol
write pipe on fd 3. Trusted and untrusted command targets receive only fds 0,
1, and 2. Callers cannot grant arbitrary descriptors and fd 3 never appears in
`EnvGrant`.

The macOS engine creates all pipes with close-on-exec behavior, then uses
`os.posix_spawn()` file actions to duplicate only the intended child ends onto
fds 0 through 3 and close the originals. It creates the child session through
the same spawn operation. It does not mutate parent-global descriptor numbers
and does not use `preexec_fn`, so concurrent `Executor.run()` calls cannot
deadlock on Python runtime state inherited across `fork()`.

`PreparedPythonProcess.request` is serialized with
`canonical_identity_json_bytes()` and written as the complete contents of
stdin, with no byte-order mark, length prefix, delimiter, or trailing newline.
The parent then closes stdin. The canonical request byte length is validated
against the caller's input budget before spawn and is the recorded input-byte
measurement. The driver reads through EOF, performs bounded strict JSON and
`IdentityDocument` validation, and emits no protocol output if the request is
invalid.

The driver opens fd 3 before executing domain code and retains that handle even
if the payload replaces `sys.stdout` or `sys.stderr`. Payload code can still
discover, close, or write directly to inherited descriptors; malformed
protocol bytes therefore remain an executor protocol failure under the honest
process-boundary-only profile, not a claim of in-process tamper resistance.

### Durable run-store boundary

The store Protocol operates on nominal lifecycle handles so illegal state
transitions are not expressible through one loosely typed handle.

```python
@verify(UNIQUE)
class RecordState(StrEnum):
    PREPARED = "prepared"
    RUNNING = "running"
    FINALIZED = "finalized"


class RunRecordHeader(ContractModel):
    schema_version: Literal[1] = 1
    executor_identity: IdentityDocument
    executor_config_identity: IdentityDocument
    prepared_at: AwareDatetime


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
        record: PreparedRecord,
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

Every state is one complete execution-local snapshot. `RunRecordHeader` carries
only record schema and executor provenance; it contains no pool capacity, queue
depth, worker lease, or pool-session reference. Dr-platform owns durable
workflow and lease context, while worker telemetry and the release benchmark
own host-level scheduling observations.

The target record's discriminant and full
`canonical_declaration_sha256` together are the v1 durable invocation evidence.
The digest covers the complete versioned target declaration, while the record
deliberately exposes no recoverable argv, source, stdin, request-payload, or
environment-value excerpt. Python target records add the request identity,
containment profile, and runtime record without changing that rule. Projection
and decoding diagnostics identify the failed field or rule without embedding a
rejected secret-bearing value.

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

### Executor self-budgets

Executor self-budgets use the same explicit limit values without becoming
caller workload budgets:

```python
class ExecutorSelfBudgets(ContractModel):
    protocol_frame_bytes: ByteBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    protocol_total_bytes: ByteBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    protocol_output_count: CountBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    json_depth: CountBudget = Field(default_factory=UnbudgetedLimit)
    manifest_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    narration_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    recording_failure_count: CountBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    failure_detail_bytes: ByteBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    startup_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    termination_time: DurationBudget = Field(
        default_factory=UnbudgetedLimit,
    )
    join_time: DurationBudget = Field(default_factory=UnbudgetedLimit)

    @classmethod
    def unbudgeted(cls) -> ExecutorSelfBudgets:
        return cls()
```

There is no unset state, built-in finite profile, machine-derived limit, or
capacity-derived limit. `ExecutorSelfBudgets.unbudgeted()` is the production
default. A caller may later supply a deliberately finite value on any axis, and
the complete effective configuration participates in executor-config identity.

Unbudgeted affects volume and waiting policy, not validity. Canonical framing,
closed schemas, message order, request identity, secret exclusion, and
unavoidable operating-system limits remain mandatory. The request has no
second hidden executor byte cap: its actual canonical length is checked only
against the caller's `input_bytes` workload budget, while the decoder receives
that already-known finite length as its safe materialization bound.

When configured finitely, protocol volume or structure overflow is a protocol
failure that preserves earlier accepted outputs; manifest, narration, or
recording-detail exhaustion degrades observability without replacing the run
outcome. Finite startup, termination, and join values supply watchdog and
escalation deadlines. When those time axes are unbudgeted, `Executor.run()` may
wait indefinitely and v1 makes no bounded-return claim.

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

The production implementation is immutable and has three public fields. The
third remains ergonomic by defaulting every executor axis explicitly to
unbudgeted:

```python
@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = field(
        default_factory=ExecutorSelfBudgets.unbudgeted,
    )

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
- Production executor, executor-config, and runtime identities use the loose
  versioned documents defined above. Fake executions have no production run
  record and therefore do not manufacture production provenance. Exact keys,
  literals, canonicalization, and identity strings belong in validated models
  and golden tests rather than being derived from mutable code field names.

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
allocation of a finite aggregate output budget between stdout head, stdout
tail, stderr head, and stderr tail is pinned by `FiniteOutput` and cannot depend
on drain-thread scheduling. The fail-on-overflow action may
terminate the run, while marked truncation continues draining through EOF; both
return the retained structural evidence and exact produced/dropped counts.

Execution protocol output is strict canonical NDJSON on fd 3 with separate
accounting. Each frame is the dr-serialize canonical UTF-8 encoding of exactly
one closed frame model followed by one LF byte. Canonical JSON contains no raw
line breaks, so LF is an unambiguous frame boundary. The wire permits no BOM,
blank line, CRLF, leading or trailing whitespace, missing terminal LF, or bytes
after the completion frame.

The closed v1 frame sequence has this shape:

```python
@verify(UNIQUE)
class ProtocolFrameKind(StrEnum):
    PRELUDE = "prelude"
    OUTPUT = "output"
    COMPLETE = "complete"


class ProtocolPrelude(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.PRELUDE] = ProtocolFrameKind.PRELUDE
    request_id_sha256: Sha256Digest


class ProtocolOutput(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.OUTPUT] = ProtocolFrameKind.OUTPUT
    sequence: NonNegativeInt
    document: IdentityDocument


class ProtocolComplete(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.COMPLETE] = ProtocolFrameKind.COMPLETE
    output_count: NonNegativeInt


type ProtocolFrame = Annotated[
    ProtocolPrelude | ProtocolOutput | ProtocolComplete,
    Field(discriminator="kind"),
]
```

Exactly one prelude comes first and echoes the full request-identity digest.
Zero or more outputs follow with consecutive zero-based sequence numbers.
Exactly one completion comes last, and its `output_count` must equal the number
of accepted output frames. EOF is valid only after the complete frame and its
LF. Duplicate, skipped, reordered, or post-completion frames fail the protocol.

Invalid UTF-8, invalid JSON, duplicate keys, non-canonical bytes, or a frame
that fails its closed model maps to `MALFORMED_FRAME`; a prelude or frame in the
wrong position maps to `UNEXPECTED_FRAME`; a request digest mismatch maps to
`ID_MISMATCH`; a repeated sequence maps to `DUPLICATE_OUTPUT`; and EOF, a
missing terminal LF, or a mismatched completion count maps to
`INCOMPLETE_STREAM`. A configured finite protocol limit maps its overflow to
`OVERSIZED_FRAME`; with the unbudgeted default there is no size- or count-based
protocol failure.

With a finite frame budget, the parent scans for LF without acquiring beyond
that limit. With the unbudgeted default, it scans without an executor policy cap
and may exhaust machine resources. Once one finite frame has been acquired, the
parent gives the decoder its actual byte length and maximum structurally
possible depth as materialization bounds, re-encodes the decoded value and
requires byte-for-byte canonical equality, then performs Pydantic frame
validation. Protocol bytes are never head/tail truncated. An oversized under a
finite policy, malformed, non-canonical, identity-mismatched, duplicate, or
incomplete stream is an executor protocol failure; the parent preserves every
previously accepted complete protocol output. The owning domain decides whether
those outputs constitute a complete internal result and never relies on dr-exec
to synthesize missing domain items. Payload output can therefore overflow
without corrupting trusted structured outputs.

Per-frame, aggregate-byte, JSON-depth, and output-count limits apply only when
their executor self-budget axes are finite. Golden vectors pin valid zero-,
one-, and multiple-output streams plus every ordering, framing, configured
limit, identity, duplicate-key, and incomplete-stream failure.

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

The execution-local manifest contains record schema, executor, and
executor-config identities; the target record that serves as durable invocation
evidence; environment-grant identity; containment profile; complete effective
workload budgets; resolved Python interpreter when relevant; lifecycle
timestamps; outcome; attribution; measurements; accepted protocol outputs;
truncation metadata; and output sidecar references and digests. It contains no
pool capacity, queue, lease, or worker-session facts. Every accepted complete
protocol output remains inline in the finalized `record.json`; v1 does not create
separate protocol-output artifacts or replace them with digest-only references.
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

The record manifest uses the pinned `dr-serialize` canonical-byte path described
above. Dr-exec owns the manifest model, safe projection, lifecycle validation,
and transaction semantics; it does not create a second general canonicalization
system merely for records.

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
produces samples, without scheduler-created unbounded active-process, thread,
or queue growth or lost per-sample results and records. Explicitly unbudgeted
per-run data can still exhaust machine memory or disk; the benchmark must make
that exposure measurable rather than treating unbudgeted policy as capacity
protection.

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
