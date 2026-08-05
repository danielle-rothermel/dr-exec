# dr-exec

Contract-driven local process execution.

## Status

The package defines the canonical v1 API and implements its substrates:

- identity and serialization — canonical secret-safe projections, the strict
  bounded read path, and role-specific versioned identities;
- the isolated-host Python runtime with its fixed `-I` probe and the
  library-owned child wrapper;
- the protected protocol — request transport, frame codec, the
  prelude/output/completion state machine, and finite executor self-budgets;
- durable recording — `DirectoryRunStore` over the pinned Document
  Directory, with typed lifecycle handles, receipts, and strict load
  validation.

The engine remains intentionally unimplemented: `ProcessExecutor.run`,
`run_many`, and `open_pool`; `ExecutionPool` scheduling; and `FakeExecutor`.

- Exact public API: [`src/dr_exec/__init__.py`](src/dr_exec/__init__.py)
- Stable capability boundaries:
  [`src/dr_exec/protocols.py`](src/dr_exec/protocols.py)
- Planned behavioral contracts:
  [`docs/v1-plan/contracts.toml`](docs/v1-plan/contracts.toml)
- Design and qualification mechanics:
  [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md)
- Shared serialization capabilities delivered by the dr-serialize pin:
  [`docs/v1-plan/dr-serialize-additions.md`](docs/v1-plan/dr-serialize-additions.md)
