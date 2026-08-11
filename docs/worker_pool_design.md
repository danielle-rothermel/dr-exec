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

- Workers start on `multiprocessing`'s `spawn` start method, or an equivalent
  explicit fresh-interpreter spawn. Never `fork`: forking a process that has
  already imported large packages, holds threads, or holds locks is the classic
  source of nondeterministic child deadlock, and the caller of this executor is
  precisely a process that has imported large packages.
- Worker count defaults to the pool capacity model already in the repo
  (`usable_cpu_count()` via `AutoPoolCapacity`), and is overridable by the
  caller. It is a parallelism width, not a resource cap.
- Each worker, at startup, imports the entry-point module named by the jobs it
  will serve and resolves the module-level attribute once. That resolution is
  the amortized cost the whole design exists to pay once instead of per job.
  Because the import is per worker rather than per job, a worker pool is bound
  to one entry point for its lifetime; a caller needing a different entry point
  opens a different pool.
- A worker that fails its startup import never becomes ready. That failure is
  reported to the caller as an executor-owned dispatch failure on the first job
  that needed it, in the same shape the in-process executor already uses for
  import failure (`ProtocolFailedOutcome` with `MALFORMED_FRAME` and
  `FailureOwner.EXECUTOR`).
- Workers live until the pool closes. There is no recycling, no
  jobs-per-worker ceiling, and no idle timeout.

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
  point with the payload, and returns the result inside the same envelope.
- The worker writes those canonical result bytes back to the parent over a
  second pipe.
- Framing follows the existing protected-protocol shape: one canonical JSON
  document per frame, terminated by `FRAME_TERMINATOR` (`b"\n"`), read by
  length-then-bytes or newline-delimited reads consistent with
  `dr_exec.runtime.protocol`. Reusing that framing keeps one grammar in the
  repo rather than inventing a second one.

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

`RecordReceiptKind` members are a persisted-format contract: the new literal
`"worker_pool"` is pinned by a golden test with the existing kinds.

### Worker death attribution

Worker death is the one genuinely new failure mode, and the design's rule is
that jobs fail loudly rather than hang.

A worker that dies while a job is in flight fails that job immediately. The
completion distinguishes two owners:

- **Job-caused death** — the worker died while executing a job (segfault,
  `os._exit`, `MemoryError` kill, an uncaught fatal signal from the entry
  point). Represented as `SignaledOutcome` when a terminating signal is
  observable, otherwise `ExitedOutcome` with the worker's exit code, attributed
  to `FailureOwner.PAYLOAD` with detail naming worker death during the job.
- **Pool-caused death** — the worker could not be started at all, or died
  between jobs, or the pool's own machinery failed. Represented as
  `SpawnFailedOutcome` for start failure and attributed to
  `FailureOwner.MACHINE` or `FailureOwner.EXECUTOR` respectively; no
  in-flight job is silently retried on another worker.

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

- `tests/capabilities/test_executor_conformance.py` — add the worker-pool
  executor to `EXECUTOR_IMPLEMENTATIONS` so it runs the shared
  invalid-declaration rejection, valid-declaration acceptance, and
  receipt-kind assertions. Declarations it does not accept (command targets,
  process Python targets) must be rejected the same way the in-process executor
  rejects them, through shared validation rather than a private check.
- `tests/execution/test_importable_json_executor.py` — the in-process behavior
  suite is the semantic conformance baseline: echo round trip, JSON `null`
  result, import failure, non-callable attribute, entry-point exception,
  wrong-target rejection, non-empty env rejection, finite input-budget
  enforcement, finite payload-output rejection, pre-cancelled token,
  concurrent-call safety, pool streaming, and pool health after a failing job.
  Every one of those must hold for the worker pool; parameterize the shared
  cases across both executors rather than copying them.
- `tests/scheduling/test_execution_pool.py` and
  `tests/scheduling/test_pool_real_engine.py` — capacity resolution, completion
  ordering, context pairing, drain, and abort must behave identically when the
  pool is backed by worker processes.
- `tests/test_public_api.py` — the new executor, receipt model, and receipt
  kind are exported.
- Golden vectors — `tests/core/test_scalar_golden_vectors.py` and the recording
  identity golden vectors pin the new `"worker_pool"` receipt-kind literal.

New worker-pool-specific tests:

- real parallelism: a CPU-bound entry point across N workers completes in
  measurably less wall time than the same work on one worker (the one place
  elapsed time is the property under test, asserted as a coarse ratio, never as
  a proxy for ordering or readiness);
- amortized import: a worker imports the entry-point module once regardless of
  how many jobs it serves, observed through a state marker rather than a
  duration;
- worker death mid-job (entry point calls `os._exit` or raises a fatal signal)
  fails exactly that job with payload attribution, and the pool completes the
  remaining jobs on a respawned worker;
- worker start failure (unimportable entry-point module) fails jobs loudly with
  executor attribution instead of hanging;
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
  advanced beyond the in-flight width.

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
