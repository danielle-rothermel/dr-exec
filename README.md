# dr-exec

Canonical subprocess execution primitives for the dr-* fleet.

One call-scoped executor with one implementation of the shared invariants:
argv-only spawning, a fresh session per run, concurrent input feeding and
output draining, group-targeted teardown with no survivors, a per-run scratch
workspace, and byte-denominated budgets. Outcomes are data — a budget
violation, a signal death, and an absent program are all values on a
`RunResult`, each carrying exactly one attribution. Exceptions are reserved
for declarations that were never runnable and for the executor's own
machinery breaking.

The target vocabulary and binding contracts live in
[`.defs/terms.toml`](.defs/terms.toml) and
[`.defs/contracts.toml`](.defs/contracts.toml). The v1 implementation design
is in [`docs/v1-plan/v1-design.md`](docs/v1-plan/v1-design.md).

## Modules

- `dr_exec.run` — the three call-scoped entry points. Trust categorization is
  the function you call: `run_tool`, `run_untrusted_python`,
  `run_untrusted_command`.
- `dr_exec.declare` — budgets, environment grants, containment profiles, exit
  policies, records declarations, and Python runtimes.
- `dr_exec.record` — run results, the durable run record and its pinned wire
  format, and executor identity.
- `dr_exec.batch` — the batch protocol: one warm child per request, results
  delivered incrementally as newline-delimited JSON, parent-side accounting.
- `dr_exec.fake` — the contract-enforcing fake: the same declarations, the
  same validation, no spawn.

## Usage

```python
from dr_exec import (
    PROCESS_BOUNDARY_ONLY,
    Attribution,
    Budgets,
    EnvironmentGrant,
    OutputBudget,
    OverflowPolicy,
    Records,
    run_untrusted_python,
)

result = run_untrusted_python(
    "import sys; print(sum(int(line) for line in sys.stdin))",
    profile=PROCESS_BOUNDARY_ONLY,
    budgets=Budgets(
        wall_clock=5.0,
        output=OutputBudget(
            limit_bytes=64 * 1024,
            overflow_policy=OverflowPolicy.MARKED_TRUNCATION,
        ),
        input=4096,
    ),
    records=Records.directory("runs/"),
    input_text="1\n2\n3\n",
    environment=EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"}),
)

if result.outcome.attribution is Attribution.PAYLOAD:
    print(result.returncode, result.stdout)
```

Consumers test their own logic against `dr_exec.fake.FakeExecutor`, which
runs the same declaration validation and refuses to return an outcome the
engine could not have produced. Spawn-path correctness is this repo's tested
responsibility.
