# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.14] - 2026-08-21

### Added

- `CancelledOutcome` now reports `started: bool`, so a batch-wall or caller
  cancel distinguishes a job that never ran from one that had already leased a
  worker, entered an in-process entry point, or spawned a child. Attribution
  is unchanged (`owner=NONE`, detail `the call was cancelled`).

### Changed

- Run-record `schema_version` is 2. New finalized cancelled records persist
  `result.outcome.started`. Existing v1 manifests no longer load.

## [0.1.13] - 2026-08-21

### Fixed

- A trusted importable-JSON payload that raises now reports its exception type,
  message, and a bounded tail of its traceback in the completion's attribution
  detail. Both the worker pool and the in-process executor previously replaced
  the exception with the fixed string `the importable JSON entry point raised`,
  so a caller reading only the completion — the pool creates no durable record —
  could not tell what failed.

### Changed

- Payload-raise detail is rendered identically by both modes and capped as a
  whole at `PAYLOAD_ERROR_DETAIL_MAX_BYTES` (8 KiB) with an explicit
  `... [truncated]` marker, so no payload-chosen message can grow a worker
  result frame without bound. The frame protocol, its terminator, and the
  persisted record shape are unchanged.
- The detail formatter is total: a payload exception whose `__str__`, type
  names, or traceback rendering itself raises still completes as the same
  payload-owned failure, with a fixed `<unprintable ...>` placeholder in place
  of what could not be rendered. Totality holds against payload-chosen string
  *types* as well as values — a `str` subclass that overrides `__format__`,
  `__str__`, `__eq__`, `encode`, or `__len__` with a raising method is
  normalized to an exact `str` before any comparison, interpolation, sizing, or
  encoding reads it. Rendering is also surrogate-safe — a lone surrogate or
  `surrogateescape` byte in a message is escaped rather than refused by strict
  UTF-8. The diagnostic path never changes the outcome kind.

## [0.1.12] - 2026-08-21

### Added

- `run_batch` and each executor's `run_many` accept an optional finite
  `wall_time`. On expiry, remaining jobs complete as `CancelledOutcome` and
  in-flight work tears down through the existing cancel path. The watcher
  stays armed through early-close drain; in-process cancellation stays
  cooperative.

### Changed

- Worker-pool workers lead their own process group. Orphan cleanup and
  parent-side stop SIGKILL that group so grandchildren that stay in it die
  with the worker. Idle EOF takes the same group path as a busy orphan, so
  leftovers from a completed job die too. Every worker exit, including
  `SystemExit` and a failed startup import, takes that group path so a
  descendant cannot hold the result pipe open.
- Updated `dr-store` to 0.2.5.

## [0.1.11] - 2026-08-18

### Added

- `WorkingDirectoryGrant` on `ExecutionJob` with scratch (default) and caller
  modes; run records persist the declared mode and caller path.
- Linux qualification alongside macOS; CI runs POSIX subprocess suites on both.
- `forward_parent_signals()` to map parent SIGTERM/SIGINT to `CancelToken`
  cancellation for cluster worker shutdown.
- `FiniteDurationLimit.from_seconds()` for declaring termination grace in
  seconds.

### Changed

- Production platform contract is qualified POSIX (macOS and Linux) rather than
  macOS-only.
- `RunDeclaration` records the working-directory grant on every new attempt.
- `ProcessExecutor` defaults to a finite 30-second `termination_time` grace
  before SIGKILL escalation; `ExecutorSelfBudgets.unbudgeted()` still opts every
  axis out explicitly, including infinite SIGTERM grace.
- Default `ExecutorSelfBudgets` executor-config identity re-pins with finite
  `termination_time` on the wire.

## [0.1.10] - 2026-08-12

### Removed

- **Breaking:** Removed the seven `*OutcomeRecord` models, the
  `ExecutionOutcomeRecord` union, and `ExecutionAttributionRecord`, which were
  field-identical twins of `ExecutionOutcome`/`ExecutionAttribution`;
  `ExecutionResultRecord` now carries those types directly. Persisted manifest
  bytes are unchanged; the root export count drops from 125 to 115.
- **Breaking:** Removed `UnbudgetedOutput`, a second name for `UnbudgetedLimit`;
  `OutputBudget` is now `UnbudgetedLimit | FiniteOutput` and
  `Budgets.payload_output` defaults to `UnbudgetedLimit`. The wire form
  `{"kind": "unbudgeted"}` is unchanged.
- Removed `InProcessImportableJsonTargetRecord` from `ExecutionTargetRecord`,
  which no execution mode can persist, so the union names exactly the four
  target kinds that reach a durable run record.
- Removed `runtime.protocol.encode_frame` and
  `runtime.protocol.request_identity_digest`, the module's two test-only
  exports; `runtime/protocol.py` is now a pure reader.
- Removed dead `DirectoryRunStore._load_prepared` and
  `runtime.identity._validate_isolated_host_runtime_identity`.
- **Breaking:** Removed `ExecutorFailureCode.IMPORTABLE_JSON_RECEIPT_MISMATCH`
  along with the in-process receipt self-check that raised it, which guarded a
  receipt the same function had just constructed; the golden receipt-kind
  vectors re-pin without it. `FakeExecutor`'s equivalent guard stays, because
  its receipt arrives from a caller-supplied responder.
- Removed `_StopState.local_token`, a per-job `threading.Event` that could
  never decide a stop on its own, and `_AdmissionGate`, a pass-through wrapper
  on `asyncio.Semaphore` with an unread width field.
- Removed the `_owned_by` private keyword parameter from the public
  `ExecutionPool.run_stream`.
- **Breaking:** Removed `ExecutorFailureCode.NO_DRAIN_STATE` along with the
  defensive engine raise that produced it for a state the attempt flow cannot
  reach; the golden receipt-kind vectors re-pin without it.
- Removed the engine's `_AttemptObservation` mutable out-parameter, which
  carried five fields across three methods; `_observe_attempt` now returns a
  frozen `_SetupFailed | _ReachedPayload` result that `_carry_attempt` matches
  on, and `_mark_running` returns its run-and-failures pair instead of
  mutating.

### Changed

- **Breaking:** `ProcessExecutor` admits jobs through the same
  `validate_declaration` gate every other executor uses, called right after the
  platform check, so one function answers what dr-exec checks before it spawns.
  The engine's own `validate_input_budget` call and the resolvability check
  buried inside `_resolve_executable` are gone; `_resolve_executable` is now a
  pure `shutil.which` lookup. The input budget is therefore checked against the
  job's declared input bytes rather than the bytes the `Runtime` prepared —
  identical for the shipped runtime, but the `Runtime` Protocol does not
  guarantee it. In-process targets are still rejected ahead of the shared gate,
  so they keep reporting `ExecutorFailureCode.TARGET_NOT_SUPPORTED`, and
  `_target_of`'s unreachable in-process arm is an `AssertionError` naming that
  ordering. The `Runtime` Protocol now documents the two conformance
  obligations that make that pre-spawn enforcement truthful for prepared
  artifacts — an absolute `argv[0]` and request bytes that are exactly the
  declared request's canonical transport bytes — and `_resolve_executable`
  reads a granted `PATH` optionally, so a nonconforming relative `argv[0]`
  under an empty environment grant reaches the spawn and completes as a
  classified `SpawnAbsentOutcome` instead of raising `KeyError`.
- The engine's protocol descriptors are one `_ProtocolTransport` group on
  `_Transports.protocol`, so the all-set-or-all-unset correlation with a
  protocol-speaking target is expressed once at construction and the pump guard
  is a single `is not None`.
- Renamed the engine's `_degraded_from` to `_finalized_receipt` — it is the
  normal finalize path, degraded or not — and made it an `_EngineCall` method
  so the redundant store parameter drops out; the pure constructor it calls is
  now `_degraded_from_failure`.
- `_await_child` waits with one flattened timeout computation; the wait was
  always cooperatively bounded, so the constant-true local that pretended
  otherwise is gone.
- `execution.engine.__all__` names the two symbols the package imports,
  `run_execution` and `SCRATCH_DIRECTORY_PREFIX`; the supported-platform
  constant is module-private.
- One shared `execution.outcomes.completed_execution` now builds every
  record-less completion from an already-constructed receipt, so the in-process
  and worker-pool executors keep only their own receipt and their own attempt
  value object; the in-process executor's four attempt facts became a frozen
  `_Execution` instead of a four-tuple threaded through eleven call sites. Both
  executors also build malformed-frame outcomes through one shared
  `execution.outcomes.malformed_frame_outcome`.
- The in-process `_run_body` observes its stop condition through one
  `_StopState.outcome(cancellation)` reader that latches nothing, mirroring the
  worker pool's `_StopWatch.outcome()`.
- `ImportableJsonExecutor.run()` does no per-job work on the event loop:
  declaration validation, request serialization, and attempt stamping all
  happen inside the one daemon-thread offload, so an unbudgeted request can no
  longer block the caller's loop and the wall-time clock starts when the
  attempt does rather than before the thread is scheduled. The offloaded
  attempt publishes its facts through a handoff slot, so an interrupted
  `run()` still reports the elapsed run; an interrupt that lands before the
  attempt starts completes at interrupt time without running the declaration
  gate. The target check exists once.
- `run_batch` is public scheduling machinery in `scheduling/scheduler.py` next
  to the `ExecutionScheduler.take_completion` it drives, and every executor's
  `run_many` is a one-line delegation over the new
  `scheduling.pool.batch_capacity`. `usable_cpu_count` moved to
  `scheduling/pool.py` beside `resolve_pool_capacity`.
- `ExecutionPool.run_stream` and `map_stream` delegate to one private
  `_drive_stream` driver that names its termination model once.
- `dr_exec.importable_json` owns the public `ENVELOPE_SCHEMA` /
  `ENVELOPE_SCHEMA_VERSION` pair and the `is_importable_json_envelope` check
  every parent-side reader uses. The spawned worker module keeps its own copy
  deliberately — it is a `-c` entry module that imports nothing from `dr_exec`
  — and a golden test pins the two definitions equal.
- Worker result-frame statuses are a `WorkerFrameStatus` `StrEnum` with their
  wire literals pinned, so the parent dispatches on members and has a
  meaningful unknown-status arm.
- `_WorkerLease.__exit__` is the single worker teardown point, and one
  `_get_watched` helper carries the stop-condition polling both worker-pool
  queues used to spell separately.
- Renamed the two independently contracted frame terminators to
  `PROTOCOL_FRAME_TERMINATOR` (execution protocol) and
  `WORKER_FRAME_TERMINATOR` (worker-pool pipes); they remain separate.
- The five pre-PEP-695 generics in `scheduling/` use PEP 695 syntax.
- Budget models own their limit: `UnbudgetedLimit.limit` is `None` and each
  finite limit returns its maximum, replacing five private per-axis unwrapping
  helpers and two `object`-typed signatures across the protocol reader, engine,
  outcomes, and declaration validation.
- Moved `ImportableEntryPoint` into `dr_exec.declarations.models` beside
  `InProcessImportableJsonTarget` and deleted the single-class top-level
  module; the root export is unchanged.
- Identity helpers crossing a module boundary lost their leading underscore,
  and `core/identity.py`, `runtime/identity.py`, and `recording/identity.py`
  now declare an `__all__` naming exactly those.
- `session_id` on the executor identity payload validates through the shared
  `CanonicalUuidSpelling` alias in `core/model.py` instead of a hand-rolled
  round-trip check, so one owner defines canonical UUID spelling.
- Git object IDs validate through one `is_git_object_id` predicate in
  `recording/provenance.py`.
- `PreparedRun` and `RunningRun` carry `durable_state`, so the store and the
  engine read the recorded state instead of each decoding it from the handle
  type.
- **Breaking:** `Executor.run()` is now awaitable and offloads to
  `Executor.run_blocking()`, the renamed blocking primitive. Standalone async
  callers use `await executor.run(job)`; sync callers and scheduler worker
  threads use `executor.run_blocking(job)`.
- **Breaking:** `WorkerPoolImportableJsonExecutor.close()` is now awaitable and
  offloads to `close_blocking()`, the renamed blocking teardown primitive. Sync
  `with` calls `close_blocking()` via `__exit__`; async callers use
  `await executor.close()` or `async with WorkerPoolImportableJsonExecutor(...)`.
- Closed worker pools reject new jobs with `ProtocolFailedOutcome` instead of
  dequeuing terminated idle workers.
- In-process async `run()` preserves Ctrl+C → `ExitedOutcome(1)` via
  daemon-thread offload and an interrupt bridge; the entry point may continue
  until it returns in the background.
- Derived `AttemptId` and `RunRecordReference` deterministically from the
  caller-supplied `JobId`, deleting internal `uuid4` mints so replay lands
  attempt-keyed instead of forking a fresh random record.
- Carried the run-record manifest header and declaration forward on
  `PreparedRun` and `RunningRun`, removing the `mark_running` reload; scoped
  `RunStore.load()` to cross-process recovery in docs and contracts.
- Removed four unenforced `ExecutorSelfBudgets` axes from declarations and
  recorded executor-config identity.
- Removed the 50 ms child-reap poll, gave unbudgeted `termination_time` infinite
  SIGTERM grace before SIGKILL escalation, and required an explicit
  `ExecutionPoolConfig.capacity` at pool assembly.
- Deleted `scripts/benchmark_importable_json.py`.
- Updated `dr-store` to 0.2.3.
- Mapped `RegularChildFailureReason` from dr-store verified child reads into
  `RecordLoadError` messages for artifact and sidecar mismatches.
- Renamed scheduler cross-module types to `AdmissionResult` and
  `ExecutionScheduler` as module-internal scheduling API.
- Added persisted `ExecutorFailureCode` on `RecordingFailure.failure_code` for
  degraded receipts when executor machinery fails.

## [0.1.9] - 2026-08-11

### Added

- Added `WorkerPoolImportableJsonExecutor`, which runs the existing in-process
  importable JSON jobs across long-lived spawned worker processes that import
  their entry point once, giving trusted CPU-bound fan-out real parallelism and
  making a declared finite wall-time budget enforceable by worker termination.
- Added `WorkerPoolRecordReceipt` and `RecordReceiptKind.WORKER_POOL` so a
  completion's execution mode is visible in its evidence, together with a golden
  test pinning every receipt-kind literal.
- Added `ExecutionPool.map_stream`, a bounded streaming map that keeps the pool
  saturated, pulls its source only as slots free, and yields completions in
  completion order so one slow job delays only itself. A map stream delivers
  only its own submissions' completions, so several streams may share one pool
  without consuming each other's work.
- Added `resolve_pool_capacity`, the capacity resolution every executor and
  pool already used internally, so a caller sizing something alongside a pool
  resolves a declared `PoolCapacity` here instead of reimplementing what
  `AutoPoolCapacity` means.

### Changed

- Scoped the scheduling contract's fresh-child and reuse guarantees to
  `ProcessExecutor`, and recorded that worker-pool execution reuses long-lived
  workers for parallelism while creating no durable record and making no
  containment claim.
- Consolidated importable JSON semantic conformance into one suite parameterized
  over the in-process and worker-pool executors.
- A declared wall-time budget or cancel token now also covers the wait for a
  worker's entry-point import, so a job whose entry-point module blocks on
  import completes as `BudgetExceededOutcome` or `CancelledOutcome` instead of
  waiting forever.
- Closing a worker pool terminates its live workers rather than waiting for a
  slot to come free, so an unbudgeted job in flight ends loudly through worker
  death instead of blocking `close()` indefinitely.
- Worker-pool frames are read over buffered readers instead of unbuffered
  streams, so a newline-delimited read is no longer a byte-at-a-time syscall
  loop. A multi-megabyte round-trip that took roughly a second per megabyte now
  takes milliseconds; no size limit was added and payloads of any size still
  round-trip.
- A worker that dies mid-job is reaped before its death is described, so
  `SystemExit` from an entry point reports the requested exit code instead of
  racing interpreter shutdown and reporting a kill by the pool.
- Workers no longer outlive a parent that died abnormally. An idle worker
  already exited at request-pipe end of file; a worker inside a job now also
  exits once it observes that it has been reparented, instead of running on at
  full CPU with nobody left to receive its answer. This bounds only an orphan's
  survival — no job runtime, payload size, or other limit was added.

## [0.1.8] - 2026-08-10

### Added

- Added `ImportableJsonExecutor` for trusted in-process importable JSON
  execution through the existing pool scheduler without subprocess overhead.
- Added `InProcessImportableJsonTarget`, `build_in_process_importable_json_job`,
  and `InProcessRecordReceipt` for explicit in-process job declarations and
  non-durable completions.

### Changed

- Scoped macOS-only production execution claims to `ProcessExecutor` and
  documented cooperative in-process cancellation semantics.

## [0.1.7] - 2026-08-08

### Fixed

- Corrected the package documentation and run-record contract to remove stale
  cached-replay claims after the caching capability was removed in 0.1.6.

## [0.1.6] - 2026-08-08

### Added

- Added trusted Python targets alongside the existing untrusted variant on the
  same runtime, protocol, budget, cancellation, and recording path.
- Added importable JSON process jobs with strict entry-point declarations,
  trusted and untrusted builders, one canonical JSON value in each direction,
  and explicit completion parsing.
- Added a repeatable resource and throughput investigation for representative
  importable JSON workloads.

### Changed

- Replaced public run-record paths with opaque serializable references and
  store-owned bounded artifact reads.
- Preserved selected virtual-environment interpreter paths so installed child
  dependencies remain importable under isolated Python startup.
- Updated to `dr-store` 0.2.0 and its `sidecar_hash` API.
- Updated the authoritative terms and contracts for trusted Python, importable
  JSON jobs, and backend-neutral run-record access.

### Removed

- Removed `CachingExecutor`, cached receipts, and their persistence contract;
  caching remains application-owned rather than bridging synchronous execution
  to the asynchronous `dr-store` Record Cache.

## [0.1.4] - 2026-08-06

### Changed

- Integrated bounded, descriptor-pinned run-record manifests through
  `dr-store` 0.1.4.
- Adopted the managed SQLite cache lifecycle in documentation and tests.
- Preserved dr-exec error and lifecycle boundaries across the storage
  integration.
- Enforced the repository's static pre-commit hooks in CI and release
  validation.

## [0.1.3] - 2026-08-05

### Added

- Added caller-scoped execution-result caching through `CachingExecutor`, with
  explicit outcome eligibility and backend-owned persistence.
- Added cached receipts that distinguish the requested job from the source
  execution whose result is replayed.

### Changed

- Updated `dr-store` to 0.1.3 for the record-cache capability.
- Made an already-cancelled cached call defer to the inner executor instead of
  replaying a warm result.

## [0.1.2] - 2026-08-05

### Changed

- Reorganized the package and tests around declarations, execution, recording,
  scheduling, core, runtime, and capabilities while preserving the ordered
  root API and pinned persisted encodings.
- Required explicit protocol-frame versions and strengthened declaration,
  record-loading, provenance, lifecycle, and scheduler validation.
- Pinned `dr-serialize` and `dr-store` at 0.1.2.
- Replaced superseded planning documents with the forward-facing README,
  repository definitions, and focused future plans.
- Switched `dr-serialize` and `dr-store` development and release resolution
  to PyPI.
- Added MIT package metadata and artifact checks that require the license in
  both distributions.
- Made asynchronous pool delivery cancellation-safe across dependent sources,
  concurrent streams, and drain or abort while preserving buffered results.
- Rejected slash-relative command executables that cannot retain their meaning
  after the child enters its scratch workspace.

### Added

- Dedicated Depot-backed macOS qualification in CI.
- Repository-wide pre-commit and CI checks for formatting, linting, typing,
  tests, definitions, and built distributions.
- Schema-backed terms validation and complete `.defs` Pages publication.

## [0.1.1] - 2026-08-05

Initial release: the complete dr-exec v1.

### Added

- The canonical v1 public API: declarations (targets, budgets, environment
  grants), outcomes-as-data (the seven-variant `ExecutionOutcome` union),
  records and receipts, and the `Executor`/`Runtime`/`RunStore` Protocol
  boundaries.
- Identity and serialization substrate: role-specific versioned
  identities, canonical secret-safe projections, the strict bounded read
  path, scalar wire spellings pinned by golden vectors, and nominal
  SHA-256 digests throughout (via the pinned `dr-serialize`).
- `DirectoryRunStore` over the pinned `dr-store` Document Directory:
  typed lifecycle handles, secret-safe durable manifests, complete and
  degraded receipts, and strict load validation.
- The protected Python protocol: stdin-through-EOF request transport, the
  library-owned bootstrap and protected fd 3 writer, the frame codec and
  prelude/output/completion state machine, and finite protocol
  self-budgets with no hidden limits on unbudgeted axes.
- The single-run macOS engine behind `ProcessExecutor.run()`: one private
  execution path for trusted-command, untrusted-command, and
  untrusted-Python targets — spawn bootstrap, concurrent transports,
  workload budget enforcement, best-effort attribution, unconditional
  teardown and reaping on every post-spawn exit path, and the mandatory
  record lifecycle with machine-readable degradation.
- `FakeExecutor` with the shared executor conformance suite, and the
  execution pool: one scheduler core behind `ExecutionPool`,
  `run_many()`, `open_pool()`, and `run_stream()` with a single shared
  resident bound, completion-order delivery, backpressure, cancellation,
  and drain-before-raise break semantics.
