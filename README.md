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
  describe trusted commands, untrusted commands, and untrusted Python together
  with their environment grants and resource budgets.
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
- **Infra**
  - **[Core](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/core)**
    supplies shared names, enums, cancellation, errors, identity helpers, and
    contract-model foundations.
  - **[Runtime](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/runtime)**
    prepares isolated Python invocations and protects structured protocol
    messages from payload output.
  - **[Capabilities](https://github.com/danielle-rothermel/dr-exec/tree/main/src/dr_exec/capabilities)**
    defines the executor, runtime, and run-store boundaries together with the
    library-owned fake executor.

The abbreviated signatures below show the durable public contract shapes;
`...` marks validation and implementation detail intentionally left out.

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


class UntrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = ...
    driver_source: str
    request: IdentityDocumentField
    containment_profile: ContainmentProfile


type ExecutionTarget = Annotated[
    TrustedCommandTarget | UntrustedCommandTarget | UntrustedPythonTarget,
    Field(discriminator="kind"),
]
```

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
synthetic return codes. Each completed execution also identifies whether
durable recording completed, degraded, or was not applicable.

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

    def load(self, record_dir: Path, /) -> RunRecord: ...
```

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

## Development

Install the locked dependencies and repository commit hook once per clone:

```bash
uv sync --locked
uv run pre-commit install
```

The hook runs `scripts/pre-check.sh`, the same repository-wide formatting,
linting, type, test, definitions, and package-build gate used by CI.
