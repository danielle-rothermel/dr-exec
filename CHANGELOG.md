# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
