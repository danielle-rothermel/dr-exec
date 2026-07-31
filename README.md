# dr-exec

Canonical subprocess execution primitives for the dr-* fleet.

The target vocabulary and binding contracts live in
[`.defs/terms.toml`](.defs/terms.toml) and
[`.defs/contracts.toml`](.defs/contracts.toml). The v1 implementation design
is in [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md).

- `dr_exec.declare` — budgets, environment grants, containment profiles,
  exit policies, records declarations, and Python runtimes.
- `dr_exec.record` — run results, the durable run record and its pinned wire
  format, and executor identity.
