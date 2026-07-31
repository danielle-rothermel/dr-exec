# dr-exec v1 design

This is the **v1** design. Its purpose is narrow and explicit: fill the
execution needs of dr-code's open PR stack — HumanEval batch evaluation,
self-invocation test probes, execution faking in tests — on a foundation
shaped so the full vision in `target-usecases.md` can be built on top of
it, or reshape it, without a rewrite. Every surface here is designed
against that contract's behaviors and vocabulary; where v1 defers a
behavior, it defers visibly, never silently.

## Scope

**Serves:** use case 1 (untrusted Python source, call-scoped), use case 3
(untrusted batch), use case 4 (trusted tool invocation, minus multi-call
aggregation), and the Testing section (the fake, and ownership of the
spawn-path test suite).

**Defers, cleanly severable:** containment mechanisms beyond the process
boundary (use case 5 — v1 ships exactly one containment profile),
supervised children and fleets (use cases 6–7), the streamed / spooled /
stdio-passthrough delivery modes (captured with marked truncation covers
the v1 consumers; spooled is the designed-for first addition), bytes-mode
I/O, and cwd beyond the per-run scratch workspace.

**Visibly unbudgeted in v1:** memory, CPU time, processes, file size,
open files. Per the no-unstated-third-case rule, these axes are declared
unbudgeted in the run record — never silently unenforced.

## Architecture

One package, `dr_exec`, consumed only as a pinned release. Modules:

- `dr_exec.engine` — internal: the call-scoped spawn/lifecycle/IPC core.
  Not imported by consumers.
- `dr_exec.run` — public call-scoped entry points.
- `dr_exec.batch` — the batch protocol: parent-side orchestration and the
  driver kit the child runs.
- `dr_exec.declare` — the declaration types: budgets, grants, containment
  profiles, exit policies.
- `dr_exec.record` — run result, run record, narration.
- `dr_exec.fake` — the contract-enforcing fake.

### The engine

The single implementation of the shared invariants; every public entry
point routes through it.

- Argv-only: commands are validated argument vectors (nonempty strings,
  no NULs); nothing is ever interpreted by a shell.
- Lifecycle: fresh session per spawn; group-targeted teardown on every
  exit path with a completion-race-safe kill, escalation, and reap inside
  the termination self-budget — no survivors before the call returns.
- I/O: input feeding and output draining are concurrent whenever both are
  live; a caller can never deadlock a run through the executor's own
  plumbing.
- Inherited state: the child receives nothing by default — environment,
  working directory, file descriptors. All grants are explicit
  declarations.
- Scratch workspace: each run executes in its own temporary working
  directory by default; concurrent runs cannot collide.

### Declarations (`dr_exec.declare`)

Frozen internal value objects (dataclasses); anything that crosses a
process or persistence boundary is a serialization model.

- `Budgets` — wall-clock, output, and input axes. Each axis holds either
  a declared budget or the explicit `UNBUDGETED` sentinel; there is no
  unset state. Output budgets carry a caller-declared overflow policy:
  `FAIL` or `MARKED_TRUNCATION`. Input budgets are enforced before spawn
  (a caller error, never a wasted spawn). Termination and startup
  self-budgets have built-in defaults and are not caller-facing in v1.
- `EnvironmentGrant` — `none()` (default), `named(vars)`,
  `overlay(extra, exclusions=())`. Exclusions are verified absent before
  spawn.
- `ContainmentProfile` — v1 ships one: `PROCESS_BOUNDARY_ONLY`, whose
  declared limits state plainly that it restricts nothing beyond the
  process boundary (full filesystem, network, and credential reach).
  Running any untrusted payload requires naming a profile at the call
  site — the trust gate is the parameter's existence, and it cannot be
  defaulted.
- `ExitPolicy` — caller-declared mapping from exit status to meaning;
  default report-only.

### Entry points (`dr_exec.run`)

Trust categorization is declared by which function is called — the
call-site acknowledgment is the function name, ungreppable-around:

- `run_tool(command, *, budgets, environment=EnvironmentGrant.none(),
  exit_policy=REPORT_ONLY) -> RunResult` — trusted payloads: known
  programs with first-party arguments. Absence (unresolvable program) is
  a distinct outcome in the result, not a start failure.
- `run_untrusted_python(source, *, profile, budgets, runtime=HERMETIC,
  input_text="", environment=EnvironmentGrant.none()) -> RunResult` —
  untrusted source in a declared runtime. `HERMETIC` is the default
  runtime (isolated interpreter, minimal declared environment); a
  declared alternative names an interpreter and importable package set.
  `profile` has no default.

Outcomes are data: both entry points return a `RunResult` for every run
that spawned, including budget violations and signal deaths. Exceptions
are reserved for executor failure — the case where no run result exists —
and for pre-spawn caller errors (invalid declarations, oversized input).
This extends the contract's use-case-4 rule to all of v1: budget
violations arrive as attributed outcomes, not exceptions, so batch
adapters never translate exception types into per-item data (the pattern
dr-code's batch_runner currently hand-rolls).

### Results, records, narration (`dr_exec.record`)

- `RunResult` (frozen, in-memory) — raw returncode including negative
  signal values, captured stdout/stderr with any truncation marked as
  metadata (never in-band), measurements (duration, budget consumption),
  and an `Attribution` field: payload, executor, channel, budget,
  machine, or absence. Exactly one.
- `RunRecord` (serialized) — the durable twin: invocation (argv or source
  digest, grants, profile, budgets in force, runtime), timestamps,
  outcome and attribution, and where outputs landed. Written at spawn,
  finalized at exit, kept regardless of outcome. A record-write failure
  is narrated and attributed executor-side; it never fails the run.
  Records land in a caller-configured run directory (one JSON file per
  run).
- Narration — verbose by default on the standard `logging` channel
  (`dr_exec.*` loggers), quiet by configuration: spawn (with what and
  where), waiting, killing, reaping, record location. Narration is
  faithful, budget-accounted separately from payload output, and never
  fails the run.

### Batch protocol (`dr_exec.batch`)

The generic half of dr-code's HumanEval machinery, with the contract
obligations its ancestor lacks.

- `BatchRequest` — items indexed by declared dimensions; an opaque
  per-item payload; a caller-supplied driver program plus the item
  schema. Budgets cross the boundary as data: the child rehydrates the
  same declared contract object the caller wrote.
- Wire protocol (child → parent): newline-delimited JSON. The driver
  emits one result line *per item as it completes* — a result once
  produced is never lost — followed by a terminal summary line that
  echoes the request identity (item ids, config digest). The parent
  verifies the echo before trusting any result.
- Driver kit — the executor's agent inside the child: protocol-stdout
  protection (private handle captured before `sys.stdout` reassignment;
  the known fd-level hole is documented in the profile's limits),
  per-item execution hooks, item-failures-as-data, load-phase failure
  fanned out to one error result per item.
- Parent-side accounting: exactly one result per item; missing,
  duplicate, unknown, or shape-invalid results are executor-side protocol
  failures. Partial results survive every failure mode, scoped per
  dimension.
- The attribution seam: dr-exec reports raw distinguishable outcomes
  (which items completed, how the child exited, driver-vs-payload fault).
  Domain meaning — e.g. mapping a SIGSEGV death to "candidate crashed" —
  stays with the consumer.

### The fake (`dr_exec.fake`)

- `FakeExecutor` — implements the same entry-point signatures, runs the
  same declaration validation as the engine (a call the real executor
  would reject, the fake rejects identically), executes nothing, and
  returns scripted results.
- Every call's full declaration set (command/source, budgets, grants,
  profile, exit policy) is recorded and assertable — adding a budget to
  production code is test-visible, never fake-breaking.
- Consumers never test the spawn path: the engine's own test suite owns
  lifecycle correctness, seeded by porting dr-code's `os.killpg`
  fault-injection tests (the reap-race coverage) into this repo.

## Packaging and the dr-code cutover

dr-exec is released as a pinned package; consumers upgrade by explicit
pin bumps, never by tracking a branch. The dr-code cutover is one PR per
the hard-cutover convention: delete `dr_code/execution/subprocess.py`,
the generic protocol half of `humaneval/batch_runner.py` and
`batch_runner_script.py`, and all three test doubles; keep HumanEval
schemas, case semantics, and the candidate/harness domain mapping;
migrate the lifecycle fault-injection tests here.

## Deliberate v1 decisions to revisit

- Budget violations as data (never exceptions) is stricter than the
  contract requires for use cases 1–3; revisit only if a consumer needs
  raise-on-violation ergonomics.
- One containment profile. The profile parameter's shape is the UC5
  foundation; new profiles must not change call sites.
- Captured-only delivery. `RunResult`/`RunRecord` already model "where
  outputs landed" so spooled delivery adds a declaration, not a schema
  change.
- The record directory is caller-configured, not discovered; a registry
  (for supervised use cases) would layer above it.
