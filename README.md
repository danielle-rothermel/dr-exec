# dr-exec

Contract-driven local process execution.

## Status

The package defines the canonical v1 API and implements its identity and
serialization substrate: canonical secret-safe projections, the strict
bounded read path, role-specific versioned identities, and the isolated-host
Python runtime with its fixed `-I` probe. Concrete execution, the protected
protocol, scheduling, recording, and fake behavior remain intentionally
unimplemented.

- Exact public API: [`src/dr_exec/__init__.py`](src/dr_exec/__init__.py)
- Stable capability boundaries:
  [`src/dr_exec/protocols.py`](src/dr_exec/protocols.py)
- Planned behavioral contracts:
  [`docs/v1-plan/contracts.toml`](docs/v1-plan/contracts.toml)
- Design and qualification mechanics:
  [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md)
- Shared serialization capabilities delivered by the dr-serialize pin:
  [`docs/v1-plan/dr-serialize-additions.md`](docs/v1-plan/dr-serialize-additions.md)
