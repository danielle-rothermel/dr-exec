# dr-exec v1 design

V1 targets the execution needs of dr-code's open PR stack: HumanEval batch
evaluation, self-invocation probes, and execution fakes. It implements a narrow
call-scoped slice of the [target use cases](../high-level-plan/target-usecases.md)
without making deferred containment, supervision, or fleet behavior appear
partially supported. Its shared call-scoped foundation is intended to support
later use cases without introducing a second execution engine.

## Structured design index

Repository-wide accepted vocabulary and behavior live in:

- [core terminology](../../.defs/terms.toml)
- [standing contracts](../../.defs/contracts.toml)

The v1 plan is classified against a frozen revision of those contracts. These
files are proposals for review, not standing repository authority:

- [v1 terminology](terms.toml)
- [compatible contract extensions](contract-extensions.toml)
- [contract contradictions](contract-contradictions.toml)
- [new contracts](new-contracts.toml)
- [intentional scope exclusions](intentionally-out-of-scope.toml)
- [unaddressed contracts](unaddressed-contracts.toml)

Every unchecked item in the
[review discussion](review-discussion-topics.md) remains unresolved. In
particular, a clause appearing in an extension or contradiction file does not
settle the corresponding discussion topic. Review must resolve that checklist
before the disputed behavior is treated as accepted.

## Scope and consumer map

V1 serves target use cases 1 and 2, the v1 batch slice of use case 3, and the
captured-output subset of trusted-tool use case 4. Multi-call trusted-tool
aggregation is not part of the v1 API. V1 also supplies the library-owned fake
and owns the production engine's spawn-path tests.

The structured [scope exclusions](intentionally-out-of-scope.toml) cover
delivery beyond captured text, containment beyond the process boundary,
supervised ownership and interaction, and fleets. V1 also does not expose
arbitrary cwd or interactive multi-process orchestration.

The plan currently proposes that memory, CPU time, process count, file size,
and open-file count remain visibly unbudgeted. The memory choice conflicts with
the standing contract and is recorded in
[contract contradictions](contract-contradictions.toml); the broader budget
shape remains subject to the linked [review discussion](review-discussion-topics.md).

Consumers that require a deferred surface stay on their current execution path
until that complete surface exists:

- build hooks and packaging tests need arbitrary cwd and passthrough output;
- repository provenance capture needs arbitrary cwd;
- long-lived development servers need supervision and readiness;
- interactive multi-process harnesses need mid-run polling and rendezvous.

## Package navigation

The proposed package is `dr_exec`, consumed through pinned releases:

- `dr_exec.engine` — private call-scoped spawn, I/O, lifecycle, budget,
  attribution, and recording implementation;
- `dr_exec.run` — public call-scoped entry points;
- `dr_exec.batch` — parent-side batch orchestration and the child driver kit;
- `dr_exec.declare` — budget, environment, containment, and exit declarations;
- `dr_exec.record` — results, records, and narration;
- `dr_exec.fake` — the contract-enforcing consumer test fake.

The single-engine boundary and pinned-release rule are proposed in
[new contracts](new-contracts.toml). The module names and ownership map above
remain navigational design rather than a structured contract.

## Residual API design

The structured files specify the behavioral obligations but not the complete
Python signatures. The proposed call surface is:

```python
run_tool(
    command,
    *,
    budgets,
    records,
    input_text="",
    environment=EnvironmentGrant.none(),
    exit_policy=REPORT_ONLY,
) -> RunResult

run_untrusted_python(
    source,
    *,
    profile,
    budgets,
    records,
    runtime=HERMETIC,
    input_text="",
    environment=EnvironmentGrant.none(),
    exit_policy=REPORT_ONLY,
) -> RunResult

run_untrusted_command(
    command,
    *,
    profile,
    budgets,
    records,
    input_text="",
    environment=EnvironmentGrant.none(),
    exit_policy=REPORT_ONLY,
) -> RunResult
```

`EnvironmentGrant` exposes `none()`, `named(vars)`, `fixed(mapping)`, and
`overlay(extra, exclusions=())`. Record selection is explicit per call through
`Records.directory(path)` or `Records.none()`. There is no ambient record
configuration. These names and exact signatures still need to be captured by
the implementation's public API and tests.

## Residual engine and record details

The following concrete details are not fully represented by the structured
proposal and therefore remain visible design obligations:

- Command resolution uses the granted environment's `PATH`. With no granted
  `PATH`, only an absolute executable resolves; a relative executable is a
  pre-spawn declaration error. Spawn-time `ENOENT` is spawn absence, while
  other spawn errors preserve their errno and receive machine attribution.
- Attribution is selected after teardown in this order: spawn absence, output
  budget, wall-clock budget, then exit-status interpretation. A recorded output
  violation wins a race with the deadline or a clean exit.
- Duration covers spawn through reap on a monotonic clock, parent setup is
  excluded, teardown duration is separate, and record timestamps are UTC.
  Output measurements count bytes produced after the retention limit so an
  overflowing run still provides sizing evidence.
- Scratch cleanup runs on every exit path. A cleanup failure is narrated and
  executor-attributed without replacing an otherwise trustworthy run result.
- Records use one collision-free `run-<utc-timestamp>-<uuid>.json` file per run
  in the caller-selected directory. The initial layout is flat; directory
  sharding remains a future consumer-driven choice.
- Persisted digests use SHA-256 over explicitly canonicalized UTF-8 input.
  Environment identity includes sorted declared names and a digest of the
  canonical name/value payload without persisting the values themselves.
- Executor identity has the proposed forms `dr-exec@<version>` and
  `dr-exec-fake@<version>`. Exact persisted keys, literals, canonicalization,
  and identity strings belong in serialization models and golden tests rather
  than additional narrative prose.

The structured proposal selects captured text and a pinned incremental NDJSON
batch protocol, but output and batch serialization remain under explicit
review in [review discussion topics](review-discussion-topics.md). The exact
wire schemas must be captured in boundary models and golden tests once that
discussion settles.

## Residual batch and fake details

Batch items cross to the child as a whole-read JSON array on stdin, bounded by
the declared input budget rather than embedded in the driver source. This keeps
the composed source roughly constant in size and leaves batch size independent
of the source/ARG_MAX bound; over-budget input is rejected with a pre-spawn
`DeclarationError` before any child exists.

The batch driver retains a private protocol-output handle before consumer code
can replace `sys.stdout`; direct file-descriptor writes remain a declared hole
of the process-boundary-only profile. The driver provides per-item hooks and
turns a load-phase failure into one error result per item. These implementation
choices support the proposed [incremental batch contract](contract-extensions.toml)
but are not themselves structured clauses.

`FakeExecutor` supports behavior selected from the complete declaration, with
an in-order queue as a convenience. It does not execute payloads. The
contract-enforcing behavior and testing ownership are proposed in
[contract extensions](contract-extensions.toml); the scripting interface above
remains residual API design.

The engine test suite receives the lifecycle fault-injection and descendant
liveness cases currently owned by dr-code. Consumer parity suites and
driver-body tests remain real-engine oracle tests; consumer logic tests use the
fake. The exact cutover sequence and behavior adjudication are in the
[dr-code cutover plan](dr-code-cutover.md).

## Future design hooks

The current proposal leaves several extension points deliberately unresolved:

- additional containment profiles should preserve the profile-shaped call
  surface;
- a declared-cwd grant should extend the existing grant vocabulary;
- spooled and passthrough output need complete delivery contracts;
- per-item batch enforcement would require an in-child protocol addition;
- plain-run per-stream output budgets can be added if a consumer establishes
  the need;
- a record registry and high-volume directory layout belong above the current
  per-call record selection.

These are design directions, not accepted compatibility guarantees. Any choice
that settles them must first be represented in the plan's structured contract
artifacts and reviewed through the repository's
[contract-led planning process](../processes/planning.md).
