# dr-exec v1 design

This is the **v1** design. Its purpose is narrow and explicit: fill the
execution needs of dr-code's open PR stack — HumanEval batch evaluation,
self-invocation test probes, execution faking in tests — on a foundation
shaped so the full vision in `target-usecases.md` can be built on top of
it, or reshape it, without a rewrite. Every surface here is designed
against that contract's behaviors and vocabulary; where v1 defers a
behavior, it defers visibly, never silently.

Repository-wide execution vocabulary lives in `.defs/terms.toml`.
Vocabulary introduced by this plan lives in `terms.toml`.

## Scope

**Serves:** use case 1 (untrusted Python source, call-scoped), use case 2
(untrusted command, call-scoped — the argv-general form; model-authored
prompts driving agent CLIs are untrusted payload by the contract's
categorization), use case 3 (untrusted batch), use case 4 (trusted tool
invocation, minus multi-call aggregation and minus stdio passthrough —
the passthrough deferral is what keeps the build-tooling consumers below
on stdlib), and the Testing section (the fake, and ownership of the
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

**Deferred consumers:** fleet call sites whose needs sit on deferred
surfaces stay on stdlib `subprocess` until the surface exists, and are
named here so staying is a visible decision, not drift: build tooling
(hatch build hooks, packaging tests driving `uv`/`pnpm` — need arbitrary
cwd, passthrough delivery, and generous or absent deadlines), repo-scoped
`git` provenance capture (needs arbitrary cwd), long-lived dev servers
with readiness probes (use case 6), and interactive multi-process test
harnesses (mid-run polling and rendezvous — no call-scoped shape fits).

## Architecture

One package, `dr_exec`, consumed only as a pinned release. Modules:

- `dr_exec.engine` — internal: the call-scoped spawn/lifecycle/IPC core.
  Not imported by consumers.
- `dr_exec.run` — public call-scoped entry points.
- `dr_exec.batch` — the batch protocol: parent-side orchestration and the
  driver kit the child runs.
- `dr_exec.declare` — the declaration types: budgets, environment
  passthrough, containment profiles, exit policies.
- `dr_exec.record` — run result, run record, narration.
- `dr_exec.fake` — the contract-enforcing fake.

### The engine

The single implementation of the shared invariants; every public entry
point routes through it.

- Argv-only: commands are validated argument vectors (nonempty strings,
  no NULs); nothing is ever interpreted by a shell.
- Lifecycle: fresh session per spawn; group-targeted teardown on every
  exit path with a completion-race-safe kill, escalation, and reap inside
  the termination self-budget — the run's process group is gone before
  the call returns. The one residual, declared as a limit of
  `PROCESS_BOUNDARY_ONLY` (below) rather than left silent: a descendant
  that itself calls `setsid`/`start_new_session` after the leader exits
  reparents to init and leaves the group, so group teardown cannot reach
  it. Closing that gap needs a PID namespace, a subreaper, or a job
  object — real containment, which is use case 5's job; the profile
  states the exposure so no caller mistakes v1's guarantee for the full
  process-tree teardown the contract's ultimate "no survivors" behavior
  describes. A descendant that merely runs in the leader's group (an
  ordinary background helper) is reaped normally.
- I/O: input feeding and output draining are concurrent whenever both are
  live; a caller can never deadlock a run through the executor's own
  plumbing.
- Inherited state: the child receives nothing by default — environment,
  working directory, file descriptors. All grants are explicit
  declarations. Stdin is a pipe carrying exactly the declared input and
  then closed (closed immediately when no input is declared); the
  parent's stdin is never inherited, so a child that reads stdin sees
  EOF, never a hang.
- Scratch workspace: each run executes in its own temporary working
  directory by default; concurrent runs cannot collide. The workspace is
  removed on every exit path before the call returns; its path is in the
  run record, and payload artifacts that must outlive the run are
  written to caller-supplied absolute paths (artifact paths are payload
  output, per the contract). Cleanup failure is narrated and
  executor-attributed but never converts a completed run into an
  exception — the record-write rule's mirror.

### Pinned semantics

Contract-level decisions consumers build persisted identity and scoring
on. Each is deliberate, golden-tested at the exact-literal level where a
literal exists, and changed only by contract revision — never by a local
edit that happens to pass tests.

- Byte-denominated budgets — output and input budgets count bytes on the
  raw streams; decoding happens after accounting. A budget boundary never
  moves because an encoding changed.
- Never-raising decode — captured output is UTF-8 decoded with
  `errors="replace"`. Hostile payload bytes yield a scoreable string,
  never an executor failure.
- Byte-exact capture — captured payload output is exactly what the child
  wrote: no banners, prefixes, framing, or newline normalization.
  Narration lives on the logging channel only. Consumers may parse
  captured streams with `startswith`/whole-stream equality.
- Real pipes — the child's stdio are ordinary OS pipes; the payload may
  `dup`, redirect, or close its own descriptors. Anti-spoofing protocols
  built on descriptor duplication are supported, not fought.
- Untrusted-Python invocation shape — `HERMETIC` runs
  `interpreter -I -c <source>`: `-I` isolation (no `PYTHON*`
  environment, no user site, no cwd on `sys.path`), source delivered as
  argv so child-observable state is run-invariant (`<string>` tracebacks,
  no `__file__`, `__name__ == "__main__"`). Source size is validated
  before spawn against an explicit bound with declared ARG_MAX headroom —
  a caller error, never a mid-spawn surprise.
- Run-invariant child-observable state — the executor injects nothing
  run-varying into what the child can see: no environment variables, no
  argv additions, no stdio framing. Determinism gates (run twice, compare
  exactly) are a supported consumer pattern. The one run-varying
  observable is the scratch working directory's path; payloads that read
  and emit their cwd are the documented caveat.
- Executor kills are never payload-attributed — a signal death inflicted
  by the executor's own enforcement (deadline, overflow) is reported
  through the outcome's budget attribution; the raw returncode is still
  present but consumers branch on attribution first, so an
  executor-inflicted `-SIGKILL` can never masquerade as a payload crash.
- Thread-safe, duration-bounded calls — the engine is safe under
  concurrent calls from one process, and every call's wall time is
  bounded by the declared deadline plus the termination and join
  self-budgets. Callers may hold leases and heartbeats around calls.
- Descriptor table — the child starts with exactly file descriptors 0,
  1, 2 (the run's pipes); the executor's own descriptors (records,
  scratch, narration) are never inheritable, so `os.dup(1)` in a
  payload deterministically returns 3. Anti-spoofing protocols count on
  this; it is golden-tested with a descriptor-probe child.
- Absence and spawn-errno rules — argv[0] is resolved execvp-style
  against the *granted* environment's `PATH` (with no `PATH` granted,
  only absolute argv[0] resolves; a relative argv[0] under `none()` is
  a pre-spawn caller error). Absence attribution is assigned exactly on
  ENOENT from the spawn attempt; any advisory pre-check never changes
  the outcome. Every other spawn errno (EACCES, ENOEXEC, …) lands as
  machine attribution with the errno preserved — distinguishable, per
  the collapsed-attribution prohibition.
- Attribution precedence — exactly one attribution, decided once after
  teardown from recorded enforcement flags, in pinned order: absence,
  then output budget, then wall-clock budget, then exit-status
  interpretation. A recorded violation wins over a clean exit that
  raced it (a child that flooded past a `FAIL` bound and exited 0
  before the kill landed is still a budget outcome), and an overflow
  that expired the deadline while draining is an output outcome, not a
  timeout. Golden-tested.
- Measurements — duration is spawn-to-reap on the monotonic clock,
  excluding parent-side setup; teardown time is reported as its own
  field. Output consumption counts bytes *produced* per stream (the
  executor keeps counting past a truncation bound), so a consumer can
  size a bound from an overflowing run. Record timestamps are ISO-8601
  UTC wall-clock.
- Source-size bound — machine protection derived from the platform exec
  limits, not an interior default: the binding constraint is the
  per-argument ceiling (Linux `MAX_ARG_STRLEN` = 128 KiB), so `source`
  is validated pre-spawn against a pinned 96 KiB bound, and the full
  argv plus granted environment is validated against a conservative
  1 MiB aggregate (`ARG_MAX` floor) — an oversized environment
  passthrough with a valid-size source is rejected pre-spawn too, never
  a mid-spawn E2BIG.
- Narration is parent-side — `dr_exec.*` loggers live in the calling
  process; narration is never written into the child's streams, so a
  consumer asserting exact captured stderr is unaffected by verbosity.

### Declarations (`dr_exec.declare`)

Frozen internal value objects (dataclasses); anything that crosses a
process or persistence boundary is a serialization model.

- `Budgets` — wall-clock, output, and input axes. Each axis holds either
  a declared budget or the explicit `UNBUDGETED` sentinel; there is no
  unset state. The output budget is a single bound shared across stdout
  and stderr (a deliberate shape: it bounds the executor's total capture
  memory, and a noisy stderr consuming the protocol channel's budget is a
  visible, attributed outcome rather than a hidden coupling), denominated
  in bytes on the raw streams before decoding. Output budgets carry a
  caller-declared output overflow policy: `FAIL` (the run is killed and
  the outcome attributed to the budget; output captured so far is
  retained and marked truncated — diagnostics are never discarded) or
  `MARKED_TRUNCATION` (the run continues to completion; capture stops at
  the bound and the truncation is marked). Under `MARKED_TRUNCATION` the
  executor keeps draining both streams to EOF and discards bytes past
  the bound — the pipe is never closed early and the payload is never
  blocked or killed by the executor's own accounting, so an
  executor-side capture decision can never change how the payload dies.
  Truncation metadata records the bytes dropped per stream. Under either
  policy a consumer branches on the outcome before parsing captured
  output, so truncation can never masquerade as a protocol violation.
  Input budgets are enforced before spawn (a caller error, never a
  wasted spawn). Termination and startup self-budgets have built-in
  defaults and are not caller-facing in v1.
- `EnvironmentGrant` — declares environment passthrough as `none()`
  (default), `named(vars)` (listed parent variables), `fixed(mapping)`
  (a literal replacement environment; nothing is read from the parent
  at all — the shape for hermetic determinism controls like
  `OPENBLAS_NUM_THREADS=1`), or `overlay(extra, exclusions=())` (the
  whole parent environment plus extras minus exclusions; exclusions
  verified absent before spawn). Passthrough declarations are frozen
  snapshots: `named` resolves values from the parent environment at
  declaration construction, never at spawn, so an identity derived from
  the declaration is a claim every later run honors. The declarations
  are introspectable data — declared names, and for `fixed` the mapping
  — so consumers can derive identity hashes from exactly what the child
  will receive.
- `ContainmentProfile` — v1 ships one: `PROCESS_BOUNDARY_ONLY`, whose
  declared limits state plainly that it restricts nothing beyond the
  process boundary (full filesystem, network, and credential reach) and
  that teardown reaches the run's process group but not a descendant
  that re-sessions itself out of it (the setsid-escape residual above).
  Running any untrusted payload requires naming a profile at the call
  site — the trust gate is the parameter's existence, and it cannot be
  defaulted.
- `ExitPolicy` — caller-declared mapping from exit status to meaning;
  default report-only.

### Entry points (`dr_exec.run`)

Trust categorization is declared by which function is called — the
call-site acknowledgment is the function name, ungreppable-around — and
the category is recorded in the run record, so it is auditable after
the fact, not only visible at the call site. All three entry points
share one declaration surface (`input_text`, `environment`,
`exit_policy`, `budgets`, `records`); asymmetries between them are
limited to the trust parameters themselves. `records` is a required
keyword on every entry point — `Records.directory(path)` or the
explicit `Records.none()`; there is no unset state and no ambient
record configuration (a process-global record directory would be
exactly the inherited-state-by-default this contract forbids, and would
break under concurrent callers in one process).

- `run_tool(command, *, budgets, records, input_text="",
  environment=EnvironmentGrant.none(), exit_policy=REPORT_ONLY)
  -> RunResult` — trusted payloads: known programs with first-party
  arguments, including stdin-fed tools. Absence (unresolvable program)
  is a distinct outcome in the result, not a start failure.
- `run_untrusted_python(source, *, profile, budgets, records,
  runtime=HERMETIC, input_text="",
  environment=EnvironmentGrant.none(), exit_policy=REPORT_ONLY)
  -> RunResult` —
  untrusted source in a declared runtime. `HERMETIC` is the default
  runtime (isolated interpreter; the child environment is solely the
  declared environment passthrough — the runtime injects nothing); a
  declared alternative names an interpreter and importable package set.
  `profile` has no default.
- `run_untrusted_command(command, *, profile, budgets, records,
  input_text="", environment=EnvironmentGrant.none(),
  exit_policy=REPORT_ONLY) -> RunResult` — the argv-general untrusted
  form (use case 2): compiled artifacts of generated code, agent CLIs
  driven by model-authored prompts. Same engine, same invariants as
  `run_tool`, plus the undefaultable `profile` parameter and absence as
  a distinct outcome.

Outcomes are data: every entry point returns a `RunResult` for every run
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
  machine, or absence. Exactly one. Attribution values are a pinned
  `StrEnum` whose literals are persisted-format strings (consumers write
  them into durable artifacts and cache keys), golden-tested per the
  wire-format rule. A budget attribution names the violated axis
  (wall-clock, output, input) — three-way discrimination is data, never
  exception type.
- `RunRecord` (serialized) — the durable twin: invocation (argv or
  source digest *and* input digest — a trusted driver over untrusted
  stdin is the supported use-case-3 shape, and the record identifies
  both halves), trust category, environment passthrough, profile,
  budgets in force, runtime, timestamps, outcome and attribution,
  measurements, and where outputs landed (scratch path included).
  Written at spawn, finalized at exit, kept regardless of outcome. A
  record-write failure is narrated and attributed executor-side; it
  never fails the run.
  Records land in the caller-declared directory (one JSON file per run,
  filename `run-<utc-timestamp>-<uuid>.json` — collision-free under
  concurrency; volume control is the caller's directory choice plus
  `Records.none()` for hot loops).
- The record wire format is pinned — consumers derive persisted cache
  keys from it, so it is a persisted format under the no-magic-strings
  rule: JSON keys are an explicit key enum golden-tested at
  exact-literal level, never derived from field names; digests are
  SHA-256 over UTF-8 with stated canonicalization; `UNBUDGETED`
  serializes as the literal string `"unbudgeted"`; environment
  passthrough serializes as sorted declared *names* plus a SHA-256 digest
  of the canonicalized name=value payload — value-sensitive identity
  without persisting values, because redaction is the caller's and
  secrets never land in records.
- Narration — verbose by default on the standard `logging` channel
  (`dr_exec.*` loggers), quiet by configuration: spawn (with what and
  where), waiting, killing, reaping, record location. Narration is
  faithful, budget-accounted separately from payload output, and never
  fails the run.
- Executor identity — `EXECUTOR_IDENTITY`, a pinned string of the form
  `dr-exec@<version>`, carried in every `RunRecord` and exposed for
  consumers to fold into content-addressed cache keys and dataset
  provenance. The fake declares its own distinct identity
  (`dr-exec-fake@<version>`) and refuses construction claiming the
  production identity, so a fake-produced outcome can never
  cache-collide with or impersonate a real one — the guard dr-code's
  corpus evaluator enforces caller-side today moves into the library.
  Identity is a declared value, never inferred from callable object
  identity — wrapping or partial-applying an entry point must not
  change what a run claims to be. Executor identity answers "which
  machinery produced this run"; it is not a *runtime* identity
  (platform/interpreter provenance stays the consumer's to declare and
  persist).

### Batch protocol (`dr_exec.batch`)

The generic half of dr-code's HumanEval machinery, with the contract
obligations its ancestor lacks.

- `BatchRequest` — a flat item list whose ids are caller-declared
  dimension coordinates (opaque strings; the cross-product fan-out
  across outer dimensions stays parent-side, so the sharing boundary is
  declared by constructing the request: one warm child per
  `BatchRequest`); an opaque per-item payload; a caller-supplied driver
  body plus the item schema. Budgets cross the boundary as data: the
  child rehydrates the same declared contract object the caller wrote.
- Wire protocol (child → parent): newline-delimited JSON, pinned at the
  same fidelity as the record schema (exact line-shape key literals,
  golden-tested). The driver's *first* protocol line is a prelude that
  echoes the request identity (item ids, config digest — SHA-256 over
  canonical sorted-key UTF-8 JSON, the canonicalization pinned), so
  results are trustable incrementally and a later truncation or death
  can never retroactively invalidate results already delivered. Then
  one result line *per item as it completes* — a result once produced
  is never lost — and a terminal completion line signaling the child
  finished on its own terms.
- Protocol channel budget — the driver's protocol stdout carries its
  own declared contract budget (per-item result size, prelude/terminal
  size), separate from the payload-stream output budget that bounds
  payload stderr. A payload that floods its own streams can therefore
  never consume the protocol channel's budget and void completed
  results — the noisy-payload case is the common case for generated
  code, and it costs only the noisy items, never the batch.
- Driver kit — the executor's agent inside the child: protocol-stdout
  protection (private handle captured before `sys.stdout` reassignment;
  the known fd-level hole is documented in the profile's limits),
  per-item execution hooks, item-failures-as-data, load-phase failure
  fanned out to one error result per item. The per-item *body* the
  consumer supplies is consumer domain code: consumers keep
  real-execution tests of their driver bodies (see the fake section's
  testing rule).
- Parent-side accounting: exactly one result per item; missing,
  duplicate, unknown, or shape-invalid results are executor-side protocol
  failures. Partial results survive every failure mode, scoped per
  dimension; items missing at child death are synthesized by the caller
  from the run's outcome and attribution, never invented by the
  executor.
- The attribution seam: dr-exec reports raw distinguishable outcomes
  (which items completed, how the child exited, driver-vs-payload fault).
  Domain meaning — e.g. mapping a SIGSEGV death to "candidate crashed" —
  stays with the consumer.

### The fake (`dr_exec.fake`)

- `FakeExecutor` — implements the same entry-point signatures, runs the
  same declaration validation as the engine (a call the real executor
  would reject, the fake rejects identically), executes nothing, and
  returns scripted results. Scripting is behavioral: results are keyed
  by a caller-supplied callable over the full declaration (dr-code's
  existing doubles inspect the payload and synthesize matching
  protocol output — a flat FIFO cannot express them), with a simple
  in-order queue as convenience.
- The fake validates scripted results against the same invariants the
  engine guarantees — attribution/returncode consistency, truncation
  marking, exactly one attribution — rejecting unconstructable
  outcomes at scripting time: a test that passes against the fake
  cannot be wrong about the contract.
- Every call's full declaration set — command/source, `input_text`,
  runtime, budgets, environment passthrough, profile, exit policy,
  records — is recorded and assertable: adding a budget to production
  code is test-visible, never fake-breaking.
- Consumers never test spawn-path *correctness*: lifecycle, teardown,
  and budget enforcement are the engine suite's job, seeded by porting
  dr-code's `os.killpg` fault-injection tests (the reap-race coverage)
  and the real-descendant liveness tests (grandchild observably dead
  within the termination self-budget on the deadline, overflow, and
  normal-exit paths) into this repo. Consumer *oracle* tests — parity
  suites and driver-body tests whose meaning depends on genuinely
  executing a payload — are a sanctioned real-engine use, run with
  `Records.none()` and quiet narration; the fake is for logic tests,
  never a mandate to make oracle tests tautological.

## Packaging and the dr-code cutover

dr-exec is released as a pinned package; consumers upgrade by explicit
pin bumps, never by tracking a branch. During the cutover's development
phase, dr-code consumes dr-exec as a local path dependency
(`[tool.uv.sources]`), switched to a pinned PyPI release once the design
settles and publishes.

The cutover is governed by an authority principle: this contract is the
deliberate design; dr-code's pinned behaviors are prior art under
review, not requirements. Each divergence gets a requirement-vs-artifact
verdict — real obligations (attribution fidelity, protocol protection,
no-deadlock I/O) are honored in this contract's vocabulary; incidental
shapes (exception-class dispatch, whole-batch wire formats, sentinel
returncodes) are replaced, and dr-code's tests, schemas, and persisted
formats are re-pinned to the new contract. The full adjudication, the
accepted behavior changes, and the parallel-stack migration plan live in
`dr-code-cutover.md`.

The lifecycle fault-injection tests (the reap-race coverage) migrate
into this repo's engine suite; dr-code consumers test against the fake
only.

## Deliberate v1 decisions to revisit

- One containment profile. The profile parameter's shape is the UC5
  foundation; new profiles must not change call sites.
- Captured-only delivery. `RunResult`/`RunRecord` already model "where
  outputs landed" so spooled delivery adds a declaration, not a schema
  change. Spooled and passthrough delivery unblock the deferred
  build-tooling consumers.
- Per-run scratch cwd only. A declared-cwd grant (the shape that
  unblocks the git-provenance and build-tooling consumers) would be a
  new declaration on the existing grant vocabulary, not a schema change.
- The record directory is caller-declared per call, not discovered; a
  registry (for supervised use cases) would layer above it. Layout is
  one flat directory per declaration — sharding under high run counts
  is the caller's directory choice; revisit if a consumer outgrows it.
- Batch budgets are per-run (the whole child), not per-item; the driver
  kit reports per-item timing, and incremental NDJSON delivery means a
  deadline costs only the unfinished tail. Per-item enforcement would be
  an in-child protocol addition if a consumer ever needs it.
- Trust categorization is enforced by entry-point naming plus the
  recorded category, not by a redundant required parameter — the
  function name already is the declaration, and the record makes it
  auditable. Real enforcement (verified containment) is use case 5's
  job; revisit if audit shows `run_tool` absorbing untrusted payloads.
- Per-stream output budgets exist only where a protocol demands them
  (the batch protocol channel); plain runs keep the single shared
  bound. Generalizing per-stream declaration is a compatible extension
  if a consumer needs it.
