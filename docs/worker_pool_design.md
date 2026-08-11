# Worker pool importable JSON executor

Status: implementation contract for `WorkerPoolImportableJsonExecutor`.

This document is the contract the implementation must satisfy. It describes
what the executor does, what it reuses, and what it deliberately does not do.

## Purpose

Trusted, CPU-bound, milliseconds-per-item Python fan-out has no good execution
mode today.

`ProcessExecutor` with `build_trusted_importable_json_job` spawns a fresh
interpreter per job. That is real isolation and real budget enforcement, but
per-job startup plus entry-point package import costs roughly a second when the
entry-point module imports a large package. For millisecond jobs this is a
several-hundred-fold tax.

`ImportableJsonExecutor` runs the entry point inline in the caller
interpreter. It is cheap per job, but `ExecutionPool` schedules executor calls
on worker *threads*, so CPU-bound Python work serializes under the global
interpreter lock: one worker and thirty-two workers produce the same
throughput. Its wall-time budget arms a cancel token that is only observed
before dispatch and after the entry point returns, so a finite wall-time budget
cannot stop a running call.

`WorkerPoolImportableJsonExecutor` fills that gap: N long-lived worker
processes, each importing the entry-point module once, executing the same
importable-JSON jobs over pipes. Startup is amortized across all jobs a worker
handles, CPU-bound work runs on real cores, and a wall-time budget — when a
caller sets one — is enforced by killing the worker.

## What runs versus where it runs

The target says what runs; the executor says where it runs.

A worker-pool job is the existing `InProcessImportableJsonTarget`: an
`ImportableEntryPoint` plus one canonical JSON request in the fixed
`dr_exec.importable_json` envelope, built by
`build_in_process_importable_json_job`. The identical job object runs inline
through `ImportableJsonExecutor` or across worker processes through
`WorkerPoolImportableJsonExecutor`; only the executor differs. No new target
kind, no new builder, no new envelope.

The target's trust declaration is unchanged: the entry point is trusted
caller-controlled code. Worker processes are a throughput and parallelism
mechanism. They provide crash containment as a side effect — a segfaulting
entry point takes down a worker rather than the caller — but they are not an
isolation or containment claim, they accept no environment grant, and they
create no durable run record.

## Worker lifecycle

The executor owns a fixed set of worker processes.

- Workers start as explicit fresh interpreters: `subprocess` invoking
  `sys.executable` on the worker module, with the two pipe descriptors passed
  through and the parent's `sys.path` propagated so the worker resolves the
  caller's entry-point module. Never `fork`: forking a process that has already
  imported large packages, holds threads, or holds locks is the classic source
  of nondeterministic child deadlock, and the caller of this executor is
  precisely a process that has imported large packages. This is `spawn`
  semantics without `multiprocessing`'s pickle-based bootstrap, which the
  no-pickle rule rules out anyway.
- Workers start lazily. A pool holds a fixed number of slots; the first job
  that needs an empty slot spawns its worker. A pool that is opened and never
  used starts no processes, and a slot freed by a dead worker is refilled by
  the next job that needs it rather than eagerly.
- Worker count defaults to the pool capacity model already in the repo
  (`usable_cpu_count()` via `AutoPoolCapacity`), and is overridable by the
  caller. It is a parallelism width, not a resource cap.
- Each worker, at startup, imports the entry-point module named by the jobs it
  will serve and resolves the module-level attribute once. That resolution is
  the amortized cost the whole design exists to pay once instead of per job.
  Because the import is per worker rather than per job, a worker pool is bound
  to one entry point for its lifetime; a caller needing a different entry point
  opens a different pool.
- Two distinct startup failures, each with one outcome. If the OS-level spawn
  fails and no interpreter starts, the first job that needed that worker
  completes as `SpawnFailedOutcome(errno=..., error_message=...)`, attributed to
  `FailureOwner.MACHINE` by `attribute_outcome`. If a spawned worker starts but
  fails its startup import of the entry-point module, it never becomes ready,
  and the first job that needed it completes as `ProtocolFailedOutcome` with
  `MALFORMED_FRAME`, attributed through
  `executor_protocol_failure_attribution` to `FailureOwner.EXECUTOR` — the same
  shape `ImportableJsonExecutor` uses for its
  `ImportableJsonExecutorDispatchError` branch.
- Workers live until the pool closes. There is no recycling, no
  jobs-per-worker ceiling, and no idle timeout.
- Closing terminates every live worker rather than waiting for one to come
  free. Because an unbudgeted job runs as long as it likes, a slot held by a
  running job may never return, so waiting for it would make `close()`
  unbounded. A job whose worker is terminated by close completes loudly
  through the same pipe-EOF path as any other worker death. This is not a
  timeout, grace period, or join deadline: closing simply never blocks on a
  slot that may never come back.
- A worker does not outlive the pool that owns it, even when the parent dies
  abnormally and no `close()` ever runs. Two mechanisms cover the two states a
  worker can be in. An **idle** worker is blocked reading its request pipe, so
  losing the parent closes that pipe's last writer and the read reaches end of
  file; the worker returns from its serve loop and exits. A worker **inside a
  job** is not reading that pipe, so end of file cannot reach it — it would
  otherwise run to completion, possibly forever and at full CPU, with nobody
  left to receive its answer. For that case a daemon thread started before the
  entry-point import polls `os.getppid()` every
  `PARENT_LIVENESS_POLL_SECONDS`; a changed parent pid means the kernel
  reparented this orphan, and the worker leaves through `os._exit` rather than
  unwinding, because its pipes lead nowhere and the entry point may hold locks
  or be uninterruptible. The poll interval is a liveness heartbeat, not a
  limit: it bounds only how long an orphan survives its parent, never how long
  a job may run, how large a payload may be, or anything a live parent asked
  for. This is best-effort cleanup — a `SIGKILL`ed parent gets no chance to
  clean up after itself, so the worker has to end itself.

## Dispatch and wire format

The existing importable-JSON envelope and the existing canonical-bytes helper
are the wire format. There is no pickle anywhere in this design, and no second
serialization system.

- The parent sends `request_transport_bytes(target.request)` — the canonical
  JSON bytes of the same `IdentityDocument` envelope the in-process and process
  executors already exchange — to the worker over an OS pipe.
- The worker validates the envelope exactly as `_invoke_importable_entry_point`
  does today (schema and schema version must match `dr_exec.importable_json`
  version 1; the payload must be strict JSON), calls the pre-resolved entry
  point with the payload, and answers inside the same envelope.
- The worker writes those canonical result bytes back to the parent over a
  second pipe. The result envelope's payload carries one `status` field —
  `ok`, `payload_raised`, `payload_result_invalid`, or `executor_rejected` —
  alongside either the entry point's `result` or a failure `detail`. That one
  field is what lets the parent distinguish a payload failure from a
  dispatch-side rejection without a second frame kind or a digest. On success
  the parent rebuilds the plain `dr_exec.importable_json` result envelope as
  the completion's single protocol output, so `parse_importable_json_result`
  works unchanged.
- The worker answers on a dedicated pipe rather than on stdout, so anything the
  entry point prints cannot corrupt the channel.
- Framing is one canonical JSON envelope terminated by `FRAME_TERMINATOR`
  (`b"\n"`), read by newline-delimited reads over **buffered** readers on both
  sides. Buffering is a correctness concern for a mode whose purpose is
  throughput: a newline-delimited read on an unbuffered stream is a
  byte-at-a-time syscall loop, which costs roughly a second per megabyte and
  makes large results unusable. Buffering the reads is not a cap — a frame is
  still read whole, of whatever size it happens to be. The pool reuses the
  `FRAME_TERMINATOR` constant and the canonical-bytes helpers
  (`request_transport_bytes` / `canonical_json_bytes`), but not the
  prelude/output/complete `ProtocolFrame` grammar of
  `dr_exec.runtime.protocol`. That grammar — a `ProtocolPrelude` carrying a
  `request_id_sha256` the parent must match, sequenced `ProtocolOutput` frames,
  and a `ProtocolComplete` whose `output_count` must reconcile — exists to
  protect a parent from an untrusted child's shared stdout. A worker pool runs
  trusted code on a dedicated per-worker pipe, where that grammar buys nothing
  and costs three frames plus a digest per result.

**Pipe deadlock is a correctness invariant, not a hardening concern.** A parent
that writes a large request into a pipe while the worker is blocked writing a
large result into the other pipe deadlocks both. The fix of first resort is to
stream: the parent's writer and reader for a given worker must not be able to
block each other — write the request fully (with the reader drained
concurrently, on a per-worker reader thread or through a selector loop) rather
than assuming any payload fits in a pipe buffer. The fix is never a size cap on
requests or results.

Each worker serves one job at a time. Parallelism comes from having N workers,
not from concurrency inside a worker.

## Results

A worker-pool job returns a standard `CompletedExecution`, built the same way
the in-process executor builds one:

- success is `ExitedOutcome(exit_code=0)` with exactly one protocol output
  carrying the `dr_exec.importable_json` result envelope, so
  `parse_importable_json_result` works unchanged;
- an entry point that raises is `ExitedOutcome(exit_code=1)` attributed to
  `FailureOwner.PAYLOAD`;
- a non-strict-JSON return or a malformed envelope is `ProtocolFailedOutcome`
  with `MALFORMED_FRAME`, attributed to the payload for a bad return value and
  to the executor for a dispatch-side envelope or import failure;
- caller cancellation is `CancelledOutcome`;
- an expired finite wall-time budget is
  `BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)`.

`ExecutionMeasurements` are populated as they are in-process: wall-clock start
and finish, monotonic duration, and `input_bytes` from the transport bytes.

### Receipt kind

Worker-pool completions carry a new receipt distinguishable from in-process
ones. Add `RecordReceiptKind.WORKER_POOL = "worker_pool"` and a
`WorkerPoolRecordReceipt(execution_id=...)` variant of the `RecordReceipt`
union, alongside the existing complete, degraded, not-applicable, and
in-process receipts. Like the in-process receipt it references no durable
record: a worker-pool execution creates no run record. The distinct kind exists
so a completion's execution mode is visible in its evidence, and so the
conformance assertion that each executor enforces its own receipt kind extends
to this executor.

`RecordReceiptKind` members are a persisted-format contract, but no golden test
pins those literals today — `"in_process"` was added without one. This change
adds that golden test, covering the new `"worker_pool"` literal and the four
existing kinds together.

### Worker death attribution

Worker death is the one genuinely new failure mode, and the design's rule is
that jobs fail loudly rather than hang.

A worker that dies while a job is in flight fails that job immediately. The
completion distinguishes two owners:

- **Job-caused death** — the worker died while executing a job (segfault,
  `os._exit`, `SystemExit` from the entry point, `MemoryError` kill, an
  uncaught fatal signal from the entry point). Represented as `SignaledOutcome`
  when a terminating signal is observable, otherwise `ExitedOutcome` with the
  worker's exit code, attributed to `FailureOwner.PAYLOAD` with detail naming
  worker death during the job. `SystemExit` is worker death here rather than
  the in-process executor's `ExitedOutcome` translation, and lands on the same
  outcome by a different route: the worker exits with the requested code and
  the parent reports that code. The parent reaps the process before describing
  its death, because the result pipe reaches end of file as soon as the worker
  stops holding it open — before the process has finished exiting. Reporting
  from an unreaped process would make the outcome a race, attributing a kill
  by the pool to a worker that was already exiting on its own terms.
- **Pool-caused death** — the worker could not be spawned, or died between
  jobs, or the pool's own machinery failed. An OS-level spawn failure is
  `SpawnFailedOutcome`, which `attribute_outcome` attributes to
  `FailureOwner.MACHINE`; there is no owner parameter and the pool builds no
  attribution of its own. Every genuinely executor-owned case — worker startup
  import failure, death between jobs, pool machinery failure — is reported as
  `ProtocolFailedOutcome` with `MALFORMED_FRAME` and attributed through
  `executor_protocol_failure_attribution`, which is the repo's supported
  `FailureOwner.EXECUTOR` path. No in-flight job is silently retried on another
  worker.

In both cases the parent notices worker death by the read side of the pipe
reaching EOF and by reaping the child, never by waiting for a timer. A job
whose worker died must never wait on a dead pipe, and must never be silently
dropped: every submitted job gets exactly one completion.

After failing the in-flight job, the pool respawns a replacement worker so the
pool keeps its width. That is the entire recovery behavior: fail the job with
attribution, respawn, continue. No retry, no quarantine, no failure counting,
no backoff.

## Budget semantics

Budgets default to unbudgeted, exactly as they do today.
`build_in_process_importable_json_job` already defaults to
`Budgets.unbudgeted()`, and the worker pool changes nothing about that default.
An unbudgeted job runs until it finishes; the executor installs no deadline, no
timer, and no watchdog.

Enforcement machinery exists but activates only on explicit caller opt-in:

- **Wall time.** Only when `job.budgets.wall_time` is a `FiniteDurationLimit`
  does the parent arm a deadline for that job. On expiry the parent kills the
  worker executing it, reaps it, completes the job with
  `BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)`, and respawns a
  replacement. This is what makes a worker-pool wall-time budget *real* —
  unlike the in-process executor's cooperative token, killing the process stops
  arbitrary CPU-bound Python code mid-call. It is opt-in machinery, never a
  default, and no default deadline is ever synthesized from observed durations.
- **Input bytes.** A finite `input_bytes` budget is checked against the
  transport bytes before dispatch, through the same shared declaration
  validation the other executors use.
- **Payload output.** Rejected before run, as the in-process executor already
  rejects it: a worker-pool job captures no payload stdout or stderr for the
  result, so a finite payload-output budget would be an untruthful
  declaration.
- Every other budget axis remains explicitly unbudgeted and unenforced, exactly
  as the repo's finite-budget contract states.

Caller cancellation through `CancelToken` behaves the same as the wall-time
path when a job is already running: kill the worker, complete the job as
`CancelledOutcome`, respawn.

A declared stop condition covers the whole job, including the wait for its
worker to finish importing the entry point. An entry-point module that blocks
on import is otherwise indistinguishable from one that is merely slow, so a
job whose worker never becomes ready still completes as
`BudgetExceededOutcome` or `CancelledOutcome` rather than waiting forever.

A job that declares neither a cancel token nor a finite wall-time budget waits
on its worker's answer with no timeout and no periodic wakeup at all: the
parent blocks on the result queue until a frame arrives or the worker's pipe
reaches EOF. Only a job that declared a stop condition re-checks it while
waiting, because `CancelToken` exposes an `Event` that cannot be waited on
jointly with the result queue. That re-check interval is an implementation
detail of observing a caller's own declared condition, never a deadline
synthesized for a job that declared none.

## Bounded streaming map

Callers fanning work across a worker pool currently hand-roll admission: fill a
window of `worker_count` submissions, wait for *all* of them, then fill the
next window. That drain barrier is a convoy bug — the whole window waits on its
slowest member while workers sit idle. The pool must make hand-rolled windows
unnecessary.

The pool exposes a streaming map helper:

```python
async with executor.open_pool() as pool:
    async for completion in pool.map_stream(submissions, concurrency=...):
        result = parse_importable_json_result(completion.completed_execution)
```

Contract:

- it accepts an iterable or async iterable of `ExecutionSubmission`, preserving
  the caller's opaque context on each `ExecutionCompletion`, as `run_stream`
  already does;
- it keeps up to `concurrency` submissions in flight — defaulting to the worker
  count, so the pool is saturated without the caller computing anything — and
  pulls the next submission from the source only as a slot frees, so a lazy or
  infinite source is never drained ahead of capacity;
- it yields completions **in completion order**, never window order, so one
  slow job delays only itself;
- every submission yields exactly one completion, including failures;
- a map stream yields **only its own** submissions' completions. Several map
  streams may share one pool, and a plain `run_stream` sharing the pool never
  claims a map stream's work. Each map stream tags its submissions and takes
  only what it recognizes, leaving the rest buffered for the stream that owns
  them. Without that identity, a stream's in-flight width tracks whatever the
  shared queue happened to hand it rather than its own work, and streams
  starve each other;
- backpressure is intrinsic: a caller that stops consuming stops intake.

`concurrency` is a parallelism width the caller may raise or lower, not a
resource limit; it has no cap and its default saturates rather than throttles.
Existing `run_stream` semantics stay available for callers already using them.

## Defaults and non-goals

The standing policy for this work, quoted:

> No hardening. Fix correctness-blocking issues only. Prefer simplicity with
> best-effort failure handling over complexity + hardening.
>
> Do NOT add ANY restriction not required by an explicit failure observed
> during this task's testing: no size limits, no time limits, no worker
> recycling, no queue bounds, no caps of any kind. Budget/limit machinery may
> exist in APIs, but every default is unlimited (dr-exec's
> `Budgets.unbudgeted()` default is the model to follow).
>
> Correctness invariants ARE in scope: dead-worker detection, no pipe deadlock,
> jobs fail loudly instead of hanging. When one surfaces, the fix of first
> resort is the simple correct mechanism (e.g. stream the payload), never a
> cap.
>
> A review finding that proposes a limit, timeout default, or hardening is a
> deferred suggestion to record in the report, never authority to implement.

Concretely, the implementation has:

- no default wall-time deadline, and no deadline at all unless the caller
  declares a finite `wall_time` budget;
- no request or result size limit, and no frame or total-byte ceiling beyond
  the caller's own declared budgets;
- no queue bound beyond the in-flight concurrency width, which exists to
  saturate workers rather than to cap them;
- no worker recycling, no maximum jobs per worker, no idle timeout, no memory
  ceiling, no restart budget, no backoff, and no circuit breaker;
- no retry of a job whose worker died, and no automatic failover;
- no health checks, heartbeats, or liveness probes: worker death is observed
  through pipe EOF and child reaping, which are exact, rather than inferred
  from elapsed time.

Non-goals for this executor: durable run records, environment grants,
containment or sandboxing claims, untrusted payloads, remote or cross-host
execution, multi-entry-point pools, and warm-child reuse for the process
`Executor` path.

Restrictions are added only in response to a specific failure observed in this
change's own testing, and are documented with that failure when added.

## Test suites that must cover the new executor

Shared conformance first — structural conformance to `Executor` does not
establish semantic conformance.

- `tests/capabilities/test_executor_conformance.py` — the worker-pool executor
  stays out of `EXECUTOR_IMPLEMENTATIONS`, exactly as `ImportableJsonExecutor`
  does today. That fixture is parameterized over command and process-Python
  declarations: every entry in `VALID_DECLARATIONS` is one of those kinds, and
  `test_each_executor_enforces_its_own_receipt_kind` runs a command target. An
  executor that accepts only `InProcessImportableJsonTarget` cannot satisfy
  those parameterizations. Its rejection of other target kinds is also not
  expressible there: `validate_declaration` has no wrong-kind branch, so
  `test_every_executor_rejects_the_same_invalid_declarations`, which requires
  `DeclarationError`, cannot host it. The worker-pool executor follows the
  established pattern instead — an `isinstance` guard raising `ExecutorFailure`,
  matching `ImportableJsonExecutor` and pinned the way
  `test_wrong_target_raises_executor_failure` pins it.
- `tests/execution/test_importable_json_semantics.py` — the semantic
  conformance baseline for both importable-JSON executors, in place of the
  shared fixture: echo round trip, JSON `null` result, import failure,
  non-callable attribute, module-initialization failure, entry-point exception,
  non-strict-JSON return, wrong-target rejection, non-empty env rejection,
  finite input-budget enforcement, finite payload-output rejection,
  pre-cancelled token, receipt kind, unbudgeted default, concurrent calls, pool
  streaming, and pool health after a failing job. Every case is parameterized
  over both executors through one harness rather than copied, and the shared
  cases are removed from the in-process suite so each lives in exactly one
  place. `tests/execution/test_importable_json_executor.py` keeps only what is
  specific to running in the caller interpreter: cooperative wall-time
  semantics, `SystemExit` mapped to `ExitedOutcome` without worker death,
  caller-cancel precedence over a wall-time budget, and `ProcessExecutor`'s
  rejection of in-process targets.
- `tests/scheduling/test_execution_pool.py` and
  `tests/scheduling/test_pool_real_engine.py` — capacity resolution, completion
  ordering, context pairing, drain, and abort must behave identically when the
  pool is backed by worker processes.
- `tests/test_public_api.py` — the new executor, receipt model, and receipt
  kind are exported.
- Golden vectors — no golden vector pins `RecordReceiptKind` literals today, so
  this change adds a new one. It lives in
  `tests/recording/test_receipt_kind_golden_vectors.py` and pins every member
  of `RecordReceiptKind` — the existing `"complete"`, `"degraded"`,
  `"not_applicable"`, and `"in_process"` alongside the new `"worker_pool"` —
  rather than extending an existing pin.

New worker-pool-specific tests:

- real parallelism, proven by state rather than by elapsed time: N jobs each
  announce arrival in a shared directory and then block until every peer has
  arrived. No job can finish unless all N are simultaneously in flight in
  distinct processes, which one worker — or N threads under one GIL — can never
  satisfy. The terminal evidence is the completions plus N distinct worker
  PIDs, none of them the caller's; a wall-time ratio would be weaker and would
  make duration the property under test;
- amortized import: a worker imports the entry-point module once regardless of
  how many jobs it serves, observed through a state marker rather than a
  duration;
- worker death mid-job (entry point calls `os._exit` or raises a fatal signal)
  fails exactly that job with payload attribution, and the pool completes the
  remaining jobs on a respawned worker;
- worker startup import failure (the worker spawns, then fails to import the
  entry-point module) fails jobs loudly as `ProtocolFailedOutcome` with
  `FailureOwner.EXECUTOR` instead of hanging;
- a large request and a large result round-trip without deadlock, in the same
  test, sized well beyond a pipe buffer;
- a finite wall-time budget on a busy-loop entry point produces
  `BudgetExceededOutcome` and the pool stays usable afterward — proving the
  budget is enforced rather than advisory;
- an unbudgeted job whose runtime exceeds any plausible default is not stopped,
  pinning the unlimited default;
- `map_stream` yields in completion order with a slow job interleaved among
  fast ones, synchronized through explicit events rather than sleeps, and
  yields exactly one completion per submission including failures;
- `map_stream` pulls lazily from its source: an instrumented source is not
  advanced beyond the in-flight width;
- two `map_stream`s sharing one pool each yield exactly their own
  submissions, and a plain `run_stream` sharing a pool never consumes a
  `map_stream`'s completions;
- a declared wall-time budget and a caller cancel each end a job that is still
  waiting for a worker whose entry-point import blocks, so startup is inside
  the declared stop condition rather than a window it cannot reach;
- `close()` with an unbudgeted job in flight terminates the worker and
  completes that job loudly instead of blocking the caller;
- `SystemExit` from an entry point reports the exit code the entry point
  asked for, pinning that the parent reaps the worker before describing its
  death rather than racing interpreter shutdown;
- a worker does not outlive a parent killed with `SIGKILL`, covered for both
  states a worker can be in: idle at the request pipe, and inside a job that
  never returns on its own. A disposable parent process is killed hard so
  nothing it owns can clean up, and the case polls for the terminal state —
  the worker pid gone — with elapsed time appearing only as the watchdog bound
  on a hang.

Tests synchronize on state, not on the passage of time. Events, barriers, and
exact terminal outcomes control interleavings; timeouts appear only as
watchdogs.

## Definitions to update

The change updates `.defs/terms.toml` with a `worker-pool importable JSON job`
term (or an executor-level term) alongside the existing in-process one, and
`.defs/contracts.toml` with the standing rules this document establishes:
worker-pool execution provides parallelism without isolation or durable
records; workers are long-lived and amortize one entry-point import; wall-time
budgets are enforced by worker termination only when declared finite; and every
submitted job produces exactly one completion, with worker death attributed
between payload and pool.

It also amends the existing `.defs/contracts.toml` contract "Scheduling bounds
resident work and reuses no child", whose unqualified "Every production job
executes in a fresh child" and "Scheduling capacity is the only resource reused
across jobs" become false once long-lived workers land. Narrow both to
`ProcessExecutor` — "Every `ProcessExecutor` production job executes in a fresh
child; `ProcessExecutor` provides no warm-child reuse" — and add a sentence
stating that worker-pool importable JSON execution deliberately reuses
long-lived worker processes for parallelism, creating no durable record and
making no containment claim. This mirrors how "Production execution is local
and macOS-only" was amended when the in-process executor landed.
