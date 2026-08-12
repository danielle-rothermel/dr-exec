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
  cancellation, teardown, and outcome attribution, across spawn-per-job,
  in-process, and worker-pool execution modes.
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

If you are here to run one Python function over many items and want to know
which execution mode to pick, start at
[Choosing an execution mode](#choosing-an-execution-mode-a-parallelism-guide).

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
completed = executor.run_blocking(job)
result = parse_importable_json_result(completed)

# In an async stage body:
completed = await executor.run(job)
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

## Choosing an execution mode: a parallelism guide

If you have a list of items and one Python function to run over each of them,
this section tells you which of the three execution modes to use. It assumes no
background in parallelism.

### The one fact that decides everything: the GIL

CPython has a **global interpreter lock (GIL)**: within a single Python
process, only one thread executes Python bytecode at a time, no matter how many
threads or CPU cores you have.

Three consequences follow, and they are the whole reason there are three modes:

- **CPU-bound Python work** (arithmetic, parsing, tokenizing, string munging,
  pure-Python transforms) gets **no speedup from threads**. Sixteen threads
  doing CPU-bound Python finish in about the same wall time as one.
- **I/O-bound work** (waiting on a network call, a disk read, a database)
  *does* benefit from threads, because a thread waiting on I/O releases the GIL
  and lets another thread run.
- To get real parallelism for CPU-bound Python you need **multiple processes**.
  Each process has its own interpreter and its own GIL, so they genuinely run
  at the same time on different cores.

The cost of processes is that they do not share memory. Anything a job needs
has to be sent to the process, and anything it produces has to be sent back —
which is why every mode below exchanges the same small, strict JSON value in
each direction.

The second fact worth knowing: starting a Python process is expensive.
Launching a fresh interpreter and importing a large package (numpy, torch,
pandas, a big first-party package) commonly costs on the order of a second.
Whether you pay that once or once *per item* is the difference between the
first and third modes below.

### Mode 1: spawn-per-job (`ProcessExecutor`)

**What actually happens.** For each job, dr-exec starts a brand-new Python
interpreter, hands it the job's JSON request on a private pipe, waits for it to
emit exactly one JSON result on a protected protocol channel, tears the process
down, and writes a durable run record. The child is a fresh session and process
group; the entry-point module is imported inside that child, for that one job.

**What it costs per job.** Interpreter startup plus the entry-point module's
imports, every time. Roughly a second when the entry point imports a large
package; a few tens of milliseconds when it imports almost nothing. Plus JSON
encode/decode on both sides.

**When to use it.** When each job is *substantial* — seconds to minutes of work
— so startup is noise. When you need a durable run record for every attempt.
When the payload is untrusted, or when you need an environment grant, a real
enforced wall-time budget, or crash containment with full evidence. This is the
only mode that gives you all of that.

**When NOT to use it.** When jobs take milliseconds. Paying a second of startup
for five milliseconds of work is a several-hundred-fold tax, and no amount of
parallelism recovers it — you are simply spending all your cores on `import`.

### Mode 2: in-process inline (`ImportableJsonExecutor`)

**What actually happens.** No subprocess at all. The executor imports the entry
point in *your* interpreter and calls it directly, then wraps the return value
in the same result envelope the other modes produce. When you drive it with
`ExecutionPool`, the pool runs those calls on worker *threads* inside your one
process.

**What it costs per job.** Almost nothing: one function call plus JSON
validation. Startup is paid once, by your own process, when the module is first
imported.

**When to use it.** Recorded inline calls: tests and fakes that need real
completion objects; I/O-bound entry points, where threads genuinely overlap;
and tiny trusted transforms where the work per item is smaller than the cost of
sending it anywhere. It is the mode that keeps the dr-exec job/completion
vocabulary while doing essentially nothing extra.

**When NOT to use it.** Be honest about two limits.

- **It provides no parallelism for CPU-bound work.** Because of the GIL, a pool
  with one worker and a pool with thirty-two workers produce the same
  throughput on CPU-bound Python. Threads here buy overlap for waiting, not for
  computing.
- **Its budgets are advisory.** A finite wall-time budget arms a cancel signal
  that is checked *before* the entry point is called and *after* it returns. It
  cannot interrupt a running call. An entry point that spins forever spins
  forever. Cancellation is cooperative, not enforced.

Also: no isolation, no durable run record, no environment grant, and a crash in
the entry point is a crash in your process.

### Mode 3: worker pool (`WorkerPoolImportableJsonExecutor`)

**What actually happens.** The pool starts N long-lived worker processes, each
a freshly spawned interpreter. Each worker imports the entry-point module
**once**, at startup, and then waits. Jobs are sent to idle workers as the same
JSON envelope over OS pipes, and results come back the same way. Workers stay
alive across jobs, so the expensive import is paid N times total instead of
once per job. Because the workers are separate processes, each has its own GIL
and CPU-bound work runs genuinely in parallel.

**What it costs per job.** Encoding the request, a pipe write, a pipe read, and
decoding the result — microseconds to low milliseconds for compact JSON values.
No interpreter startup, no re-import.

**When to use it.** This is the default choice for **trusted, CPU-bound
fan-out**: many items, milliseconds to seconds each, one function, one machine.
It is also the mode where a wall-time budget is real — if you declare one, the
pool enforces it by killing the worker, which does stop arbitrary running
Python.

**When NOT to use it.** When the payload is untrusted or you need containment
evidence — worker processes are a parallelism mechanism, not a sandbox, and
carry no containment claim. When you need a durable run record per attempt
(worker-pool executions create none). When you have only a handful of items and
each is long: mode 1 gives you records and isolation for the same wall time.
When your work is I/O-bound: you will pay for processes and pipes to get
concurrency that threads already gave you for free in mode 2. And when the
values you want to exchange are large — the transport is compact JSON, so bulk
data should travel by reference (a path, an artifact) rather than through the
envelope. And when you need to run **more than one entry point**: because the
import is paid per worker at startup rather than per job, a pool is bound to a
single entry point for its lifetime. A caller with several functions opens a
pool per function; submitting jobs naming different entry points to one pool is
not supported.

### Decision table

| Your situation | Mode | Why |
| --- | --- | --- |
| Many trusted CPU-bound items, ms–s each | **Worker pool** | Real cores, import paid once per worker |
| Untrusted payload, or needs containment evidence | **Spawn-per-job** | Only mode with a containment declaration |
| Needs a durable run record per attempt | **Spawn-per-job** | Only mode that records |
| Few long jobs (seconds–minutes each) | **Spawn-per-job** | Startup is noise; you get records and isolation free |
| I/O-bound entry point | **In-process** | Threads already overlap waiting; processes add cost |
| Tiny trusted transforms, tests, fakes | **In-process** | Cheapest possible; real completion objects |
| Needs an enforced wall-time limit | **Worker pool** or **spawn-per-job** | In-process cancellation is cooperative only |
| Needs an environment grant | **Spawn-per-job** | The other modes accept none |

Budgets default to unbudgeted in every mode. A limit exists only where a caller
declares one.

### In-process importable JSON

`ImportableJsonExecutor` runs the importable JSON contract synchronously in the
caller interpreter. It provides throughput, not isolation, and no parallelism
for CPU-bound Python work — threads still overlap I/O waits, so an I/O-bound
entry point does benefit. There is no subprocess, no durable run record, and no
environment grant. Use it with
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

`ImportableJsonExecutor.run_blocking()` never raises for entry-point failures; it
returns typed outcomes instead so pools stay healthy. That includes
`KeyboardInterrupt`: Ctrl+C during an in-process job is mapped to
`ExitedOutcome(1)` rather than propagating through `run()`, so CLI callers
should cancel through `CancelToken` when they need explicit cancellation
semantics.

### Worker pool importable JSON

`WorkerPoolImportableJsonExecutor` runs the same
`InProcessImportableJsonTarget` jobs — the same builder, the same envelope, the
same `parse_importable_json_result` — across long-lived worker processes. The
target says what runs; the executor says where it runs.

```python
job = build_in_process_importable_json_job(job_id, entry_point, request)

with WorkerPoolImportableJsonExecutor(entry_point=entry_point) as executor:
    async with executor.open_pool() as pool:
        async for item in pool.map_stream(submissions()):
            result = parse_importable_json_result(item.completed_execution)
```

The executor owns worker processes, so it is a context manager: leaving the
`with` block stops every worker. A caller that cannot use `with` calls
`close()` instead. Workers start lazily — the first job that needs a given
slot spawns its worker — and then live until the executor closes. Worker count
defaults to the usable CPU count and is overridable with `worker_count`; it is
a parallelism width, not a resource cap.

Closing stops every live worker rather than waiting for running jobs to end.
Because an unbudgeted job runs as long as it likes, waiting for its slot could
block forever, so a job still in flight when the pool closes completes loudly
as worker death instead of hanging the caller. Close when the work is done, or
declare a budget or cancel token for jobs you may need to stop.

The entry point appears twice on purpose. The job carries it because it is part
of the target — what runs. The executor takes it because each worker imports it
once at startup, before any job arrives, which is the cost this mode exists to
amortize. That makes the pool **bound to one entry point for its lifetime**:
every job a pool serves must name the entry point the pool was constructed
with, and a caller with several functions opens a pool per function.

`map_stream` keeps the pool saturated and yields completions **as they
finish**, in completion order, pulling from its source only as slots free.
Callers do not hand-roll admission windows: a window of submissions followed by
a full drain barrier makes every worker wait on the window's slowest job, and
this helper exists so that pattern is never necessary. A `map_stream` yields
only **its own** submissions' completions, so several streams can share one
pool without consuming each other's work.

Worker-pool jobs produce ordinary `CompletedExecution` values with a
worker-pool record receipt. A worker that dies mid-job fails **that job**
loudly, with attribution distinguishing a payload-caused crash from a pool
failure, and the pool respawns a replacement worker; no job silently hangs or
disappears. Budgets remain unbudgeted by default; when a caller declares a
finite wall-time budget, the pool enforces it by terminating the worker, which
makes the budget real rather than advisory.

Workers do not outlive the pool that owns them, even if the owning process dies
abnormally and never gets to close anything. An idle worker ends when its
request pipe reaches end of file; a worker in the middle of a job is not
reading that pipe, so it instead notices that it has been reparented and exits.
This is best-effort cleanup for an abnormal death, not supervision: it puts no
ceiling on how long a job may run or how large a payload may be.

### Measuring your own workload

Run the representative resource and throughput investigation with:

```console
uv run --with ./tests/fixtures/importable-json-fixture python scripts/benchmark_importable_json.py
```

The command writes a machine-readable JSON report. Its measurements are
observations for capacity selection, not performance pass/fail thresholds. If
you are unsure which mode fits, measure one item end to end: if a single job
takes far less time than your entry point's import, you want the worker pool.

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
    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...

    def run_blocking(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...
```

The production executor exposes awaitable one-job, blocking one-job,
finite-batch, and asynchronous-pool entry points over the same execution and
scheduling contracts.

```python
@dataclass(frozen=True, slots=True)
class ProcessExecutor:
    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets = ...

    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution: ...

    def run_blocking(
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


def resolve_pool_capacity(capacity: PoolCapacity, /) -> EffectivePoolCapacity: ...
```

A pool resolves its own capacity when it opens, and reports it as
`pool.effective_capacity`. A caller that must size something *alongside* a pool
— a worker count, a second pool — calls `resolve_pool_capacity` on the declared
capacity instead of reimplementing what `AutoPoolCapacity` means.

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
