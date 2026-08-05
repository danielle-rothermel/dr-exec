# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.2] - 2026-08-05

### Changed

- Reorganized the package and tests around declarations, execution, recording,
  scheduling, core, runtime, and capabilities while preserving the ordered
  root API and pinned persisted encodings.
- Required explicit protocol-frame versions and strengthened declaration,
  record-loading, provenance, lifecycle, and scheduler validation.
- Updated `dr-serialize` to 0.1.2 and raised the `dr-store` requirement to
  0.1.2.
- Replaced superseded planning documents with the forward-facing README,
  repository definitions, and focused future plans.
- Switched `dr-serialize` development and release resolution to PyPI.
- Added MIT package metadata and artifact checks that require the license in
  both distributions.

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
