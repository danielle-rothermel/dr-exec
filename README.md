# dr-exec

[![CI](https://github.com/danielle-rothermel/dr-exec/actions/workflows/ci.yaml/badge.svg)](https://github.com/danielle-rothermel/dr-exec/actions/workflows/ci.yaml)
[![PyPI](https://img.shields.io/pypi/v/dr-exec.svg)](https://pypi.org/project/dr-exec/)

| [Repo Definitions](https://danielle-rothermel.github.io/dr-exec/) | [Terms TOML](https://github.com/danielle-rothermel/dr-exec/blob/main/.defs/terms.toml) | [Contracts TOML](https://github.com/danielle-rothermel/dr-exec/blob/main/.defs/contracts.toml) |
| --- | --- | --- |

**dr-exec runs local processes through explicit, typed contracts.**
Production execution currently targets macOS and is organized into these
functional areas:

Here, “untrusted” describes who controls the payload; it does not mean
sandboxed. V1's process-boundary-only profile creates a separate invocation,
session, and process group, but leaves the invoking user's filesystem,
network, credentials, and process-spawning authority unchanged. A descendant
that creates another session can outlive teardown. Isolated host Python also
does not verify interpreter, standard-library, or package bytes.

- **[Declarations](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/declarations)**
  describe trusted and untrusted command and Python targets together with their
  environment grants and resource budgets.
- **[Execution](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/execution)**
  owns process startup, input and output transport, budget enforcement,
  cancellation, teardown, and outcome attribution.
- **[Recording](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/recording)**
  represents outcomes as typed data and preserves
  declarations, process evidence, retained output, measurements, and recording
  health in durable run records.
- **[Scheduling](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/scheduling)**
  runs finite batches and asynchronous streams through a shared capacity bound
  with completion-order delivery and intake backpressure.
- **[Capabilities](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/capabilities)**
  supplies the executor, runtime, and run-store boundaries together with the
  library-owned fake executor.
- **[Runtime](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/runtime)**
  prepares isolated Python invocations and protects structured protocol
  messages from payload output.
- **[Core](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/core)**
  supplies shared names, enums, cancellation, errors, identity helpers, and
  contract-model foundations.

The abbreviated signatures below show the durable public contract shapes;
`...` marks validation and implementation detail intentionally left out.

## Core

Core owns the nominal identities and closed enums shared across functional
areas. Their types keep job, attempt, outcome, receipt, and protocol concepts
distinct at both Python and serialization boundaries.

```python
JobId = NewType("JobId", CanonicalUuid)
AttemptId = NewType("AttemptId", CanonicalUuid)


class ExecutionId(ContractModel):
    job_id: JobId
    attempt_id: AttemptId
```

```python
@verify(UNIQUE)
class RecordReceiptKind(StrEnum):
    COMPLETE = "complete"
    DEGRADED = "degraded"
    NOT_APPLICABLE = "not_applicable"


class CancelToken:
    def cancel(self) -> None: ...

    @property
    def cancelled(self) -> bool: ...
```

## Declarations

An `ExecutionJob` describes one request without choosing how it will run. Its
target is a closed, discriminated union whose variants make the workload's
trust boundary explicit.

```python
class TrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_COMMAND] = ...
    argv: tuple[str, ...]
    stdin: Base64UrlBytes = b""


class UntrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = ...
    argv: tuple[str, ...]
    stdin: Base64UrlBytes = b""
    containment_profile: ContainmentProfile


class TrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_PYTHON] = ...
    driver_source: str
    request: IdentityDocumentField


class UntrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = ...
    driver_source: str
    request: IdentityDocumentField
    containment_profile: ContainmentProfile


type ExecutionTarget = Annotated[
    TrustedCommandTarget
    | TrustedPythonTarget
    | UntrustedCommandTarget
    | UntrustedPythonTarget,
    Field(discriminator="kind"),
]
```

Command executables are either absolute paths or separator-free names resolved
only through an explicitly granted `PATH` whose entries are absolute.

Environment access and resource limits are data carried by the job alongside
the target, so the executor receives the complete declaration at one boundary.

```python
@dataclass(frozen=True, slots=True)
class EnvGrant:
    kind: EnvGrantKind
    variables: tuple[EnvVar, ...]
    excluded_var_names: tuple[str, ...] = ()

    @classmethod
    def none(cls) -> EnvGrant: ...

    @classmethod
    def fixed(cls, variables: Mapping[str, str]) -> EnvGrant: ...


class Budgets(ContractModel):
    wall_time: DurationBudget = ...
    input_bytes: ByteBudget = ...
    payload_output: OutputBudget = ...
    ...


@dataclass(frozen=True, slots=True)
class ExecutionJob:
    job_id: JobId
    target: ExecutionTarget
    env: EnvGrant
    budgets: Budgets = ...
```

V1 accepts finite workload limits only for wall time, input bytes, and
aggregate captured payload output. Memory, CPU time, process count, file size,
open-file count, and disk limits must remain explicitly unbudgeted.

### Importable JSON process jobs

The importable JSON adapter builds an ordinary Python execution job for one
installed module-level synchronous callable. It exchanges one strict JSON value
in each direction; execution, cancellation, recording, and scheduling remain
with the selected executor and pool.

```python
entry_point = ImportableEntryPoint(
    module_name="my_package.workers",
    attribute_name="evaluate",
)
job = build_untrusted_importable_json_job(
    job_id,
    entry_point,
    request,
    env=EnvGrant.none(),
    budgets=budgets,
)
completed = executor.run(job)
result = parse_importable_json_result(completed)
```

Use `build_trusted_importable_json_job` only when the effective payload is
caller-controlled. The untrusted builder always declares
`PROCESS_BOUNDARY_ONLY`; neither builder selects an operating-system sandbox.
Entrypoints must be importable by the isolated installed interpreter—source
paths, working-directory imports, expressions, and nested attribute traversal
are unsupported. Callers enforce any entrypoint allowlist before construction.

One job is one isolation, cancellation, failure, and recording unit. Its JSON
request may be a finite caller-owned batch only when all members intentionally
share that fate; the adapter does not interpret members or provide mapping,
partial results, or per-item retries. High-volume callers reuse runtime,
executor, run-store, and pool instances and configure finite input, retained
payload-output, protocol frame, protocol total-byte, JSON-depth, and one-output
limits from representative measurements. Bulk data remains caller-owned by
reference or artifact rather than traveling through the compact JSON value.

### In-process importable JSON

When the caller already trusts the entry point and only needs throughput,
`ImportableJsonExecutor` runs the same importable JSON contract synchronously
in the caller interpreter. It provides throughput, not isolation: there is no
subprocess, no durable run record, and no environment grant. Use it with
`ExecutionPool` the same way as `ProcessExecutor`.

```python
job = build_in_process_importable_json_job(
    job_id,
    entry_point,
    request,
    budgets=budgets,
)
executor = ImportableJsonExecutor()

async with executor.open_pool() as pool:
    async for item in pool.run_stream(submissions()):
        result = parse_importable_json_result(item.completed_execution)
```

`ImportableJsonExecutor.run()` never raises for entry-point failures; it
returns typed outcomes instead so pools stay healthy. That includes
`KeyboardInterrupt`: Ctrl+C during an in-process job is mapped to
`ExitedOutcome(1)` rather than propagating through `run()`, so CLI callers
should cancel through `CancelToken` when they need explicit cancellation
semantics.

Run the representative resource and throughput investigation with:

```console
uv run --with ./tests/fixtures/importable-json-fixture python scripts/benchmark_importable_json.py
```

The command writes a machine-readable JSON report. Its measurements are
observations for capacity selection, not performance pass/fail thresholds.

## Runtime

The runtime boundary turns either Python target into an invocation and a
recorded runtime description. Trusted and untrusted Python use the same child,
startup, request, and protected-protocol path; only the untrusted declaration
and record carry containment evidence. The v1 implementation resolves and
probes a host interpreter, then invokes it with isolated Python startup
controls.

```python
class Runtime(Protocol):
    def prepare(
        self,
        target: TrustedPythonTarget | UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess: ...

    def describe(self) -> RuntimeRecord: ...
```

```python
@dataclass(frozen=True, slots=True)
class IsolatedHostPythonRuntime:
    executable: Path

    def prepare(
        self,
        target: TrustedPythonTarget | UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess: ...

    def describe(self) -> RuntimeRecord: ...
```

## Execution

All execution crosses the same small capability boundary. Production uses
`ProcessExecutor`, while consumers can depend only on `Executor` when the
implementation should remain substitutable.

```python
class Executor(Protocol):
    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...
```

The production executor exposes one-job, finite-batch, and asynchronous-pool
entry points over the same execution and scheduling contracts.

```python
@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = ...

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]: ...

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool: ...
```

## Recording

Per-job outcomes are closed typed data rather than raw process status or
synthetic return codes. Each completed execution also carries a receipt for a
complete or degraded durable record or a fake result.

```python
class OutcomeKind(StrEnum):
    EXITED = "exited"
    SIGNALED = "signaled"
    SPAWN_ABSENT = "spawn_absent"
    SPAWN_FAILED = "spawn_failed"
    BUDGET_EXCEEDED = "budget_exceeded"
    PROTOCOL_FAILED = "protocol_failed"
    CANCELLED = "cancelled"


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
```

```python
class ExecutionResult(ContractModel):
    execution_id: ExecutionId
    outcome: ExecutionOutcome
    attribution: ExecutionAttribution
    protocol_outputs: tuple[IdentityDocumentField, ...]
    payload_outputs: PayloadOutputs
    measurements: ExecutionMeasurements


class CompletedExecution(ContractModel):
    result: ExecutionResult
    record_receipt: RecordReceipt
```

```python
type RecordReceipt = Annotated[
    CompleteRecordReceipt
    | DegradedRecordReceipt
    | FakeRecordReceipt,
    Field(discriminator="kind"),
]
```

The store boundary makes the durable lifecycle explicit: a run is prepared,
may become running once a process exists, and is finalized with its result.

```python
type RunRecord = Annotated[
    PreparedRecord | RunningRecord | FinalizedRecord,
    Field(discriminator="state"),
]


class RunStore(Protocol):
    def prepare(self, record: PreparedRecord, /) -> PreparedRun: ...

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun: ...

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt: ...

    def load(self, reference: RunRecordReference, /) -> RunRecord: ...

    def read_artifact(
        self,
        reference: RunRecordReference,
        artifact: OutputArtifactRecord,
        /,
        *,
        max_bytes: int,
    ) -> bytes: ...
```

`DirectoryRunStore` publishes canonical lifecycle manifests within fixed
structural byte and depth ceilings, then loads them through bounded,
descriptor-pinned reads before validating the record and its sidecars. Real
handles and receipts expose only an opaque serializable `RunRecordReference`;
the store alone resolves its directory layout. Finalized sidecars are recovered
with `read_artifact` under a required finite byte limit and verified for size
and digest during the same descriptor-pinned, no-follow read.

## Scheduling

Finite batches and asynchronous streams share one capacity model. Capacity
bounds all admitted-but-undelivered work, so completion delivery naturally
backpressures intake.

```python
@dataclass(frozen=True, slots=True)
class AutoPoolCapacity: ...


@dataclass(frozen=True, slots=True)
class FixedPoolCapacity:
    max_active_jobs: int


type PoolCapacity = AutoPoolCapacity | FixedPoolCapacity


@dataclass(frozen=True, slots=True)
class ExecutionPoolConfig:
    capacity: PoolCapacity = ...
```

Submissions carry caller context through scheduling without serializing it,
and completions return that same context paired with the completed execution.

```python
@dataclass(frozen=True, slots=True)
class ExecutionSubmission(Generic[ContextT]):
    job: ExecutionJob
    context: ContextT


@dataclass(frozen=True, slots=True)
class ExecutionCompletion(Generic[ContextT]):
    completed_execution: CompletedExecution
    context: ContextT


class ExecutionPool:
    async def __aenter__(self) -> ExecutionPool: ...

    async def run_stream(
        self,
        submissions: AsyncIterable[ExecutionSubmission[ContextT]],
        /,
    ) -> AsyncIterator[ExecutionCompletion[ContextT]]: ...

    async def drain(self) -> None: ...

    async def abort(self) -> None: ...
```

## Capabilities

Consumers can program against the small `Executor`, `Runtime`, and `RunStore`
Protocols while selecting concrete implementations separately. `FakeExecutor`
preserves shared declaration and concurrency contracts without claiming host,
process, containment, or durable-record behavior.

```python
class FakeExecutor:
    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...
```

## Development

Install the locked dependencies and repository commit hook once per clone:

```bash
uv sync --locked
uv run pre-commit install
```

The hook runs `scripts/pre-check.sh`, the same repository-wide formatting,
linting, type, test, definitions, and package-build gate used by CI.
