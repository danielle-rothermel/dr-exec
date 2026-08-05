# dr-exec

Contract-driven local process execution.

## Status

The complete v1 surface is implemented:

- identity and serialization — canonical secret-safe projections, the strict
  bounded read path, and role-specific versioned identities;
- the isolated-host Python runtime with its fixed `-I` probe and the
  library-owned child wrapper;
- the protected protocol — request transport, frame codec, the
  prelude/output/completion state machine, and finite executor self-budgets;
- durable recording — `DirectoryRunStore` over the pinned Document
  Directory, with typed lifecycle handles, receipts, and strict load
  validation;
- the single-run engine behind `ProcessExecutor.run` — spawn through
  teardown for trusted-command, untrusted-command, and untrusted-Python
  targets, with budget enforcement, best-effort attribution, and the
  mandatory record lifecycle;
- `FakeExecutor`, which enforces the same declaration rules and carries an
  explicit fake receipt, plus the shared behavioral conformance suite both
  executors pass;
- the bounded execution pool — one scheduler core behind
  `ProcessExecutor.run_many`, `ProcessExecutor.open_pool`, and
  `ExecutionPool.run_stream`, with one shared resident bound over running
  and completed-but-undelivered submissions, completion-order delivery,
  backpressure on intake, cancellation, drain, and abort.

Execution runs on macOS; the engine refuses other platforms at the
declaration boundary, and the tests that need real process semantics are
marked accordingly.

- Terms and contracts sheet:
  [danielle-rothermel.github.io/dr-exec](https://danielle-rothermel.github.io/dr-exec/)
  (source: [`.defs/`](.defs/))
- Exact public API: [`src/dr_exec/__init__.py`](src/dr_exec/__init__.py)
- Stable capability boundaries:
  [`src/dr_exec/protocols.py`](src/dr_exec/protocols.py)
- Planned behavioral contracts:
  [`docs/v1-plan/contracts.toml`](docs/v1-plan/contracts.toml)
- Design and qualification mechanics:
  [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md)
- Shared serialization capabilities delivered by the dr-serialize pin:
  [`docs/v1-plan/dr-serialize-additions.md`](docs/v1-plan/dr-serialize-additions.md)
