# dr-exec

Contract-driven local process execution.

## Status

The package currently defines the canonical v1 API and validation scaffold.
Concrete execution, runtime preparation, scheduling, recording, and fake
behavior remain intentionally unimplemented.

- Exact public API: [`src/dr_exec/__init__.py`](src/dr_exec/__init__.py)
- Stable capability boundaries:
  [`src/dr_exec/protocols.py`](src/dr_exec/protocols.py)
- Planned behavioral contracts:
  [`docs/v1-plan/contracts.toml`](docs/v1-plan/contracts.toml)
- Design and qualification mechanics:
  [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md)
- Required shared serialization work:
  [`docs/v1-plan/dr-serialize-additions.md`](docs/v1-plan/dr-serialize-additions.md)
