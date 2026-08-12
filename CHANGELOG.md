# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

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
