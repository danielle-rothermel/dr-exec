# Changelog

## 2026-07-31 (batch item delivery)

- Batch items now cross to the child as its stdin payload, not inlined
  in the driver source. The composed driver program is delivered as one
  argv-carried `-c` argument and validated against the 96 KiB source
  bound (Linux `MAX_ARG_STRLEN`); binding every item's payload into that
  source made a real batch — e.g. a HumanEval task with ~1000 candidate
  cases — compose to hundreds of KiB and fail with a `DeclarationError`
  before any child spawned, so the batch protocol could not carry its
  actual load. Item data now rides through the input channel the
  contract already provides: `run_batch` feeds the item array as the
  run's `input_text`, the driver kit reads it whole from `sys.stdin`
  (fd 0, separate from the fd-1 anti-spoofing capture, after the prelude
  is emitted so identity always reaches the parent first), and batch
  size is bounded by the declared input budget — machine-scale by
  default, or the caller's declared input budget — never by the source
  bound. An over-input-budget batch is a clean pre-spawn
  `DeclarationError`. The wire shape of the protocol output lines is
  unchanged; only the delivery channel for item input moved.

## 2026-07-31 (contract adjudication after review)

- Narrowed the v1 "no survivors" pin to what `PROCESS_BOUNDARY_ONLY`
  can enforce: teardown reaches the run's process group, and a
  descendant that `setsid`s itself out of the group after the leader
  exits is a declared profile limit (reparents to init; closing it needs
  real containment — use case 5), not a silent gap. The full
  process-tree "no survivors" behavior stays the contract's ultimate
  target, delivered by containment.
- Recorded the finalized record shape (source-digest-not-verbatim,
  `EXECUTOR_FAILED` terminal status) in `dr-code-cutover.md` so the
  cutover binds to it. Both land with the implementation stack that
  amended them; this note keeps the design branch's rationale complete.

## 2026-07-31 (adversarial review fixes)

- Attribution: `CHANNEL` and `EXECUTOR` are now reachable outcomes. A
  drain fault or a drain that never reaches EOF within the join budget is
  a channel outcome on the result; a fault feeding the child its declared
  input is an executor outcome. Both sit below the budget axes and above
  exit-status interpretation, so a payload is never blamed by elimination
  for bytes the executor's own plumbing lost. IPC faults are narrated.
- A payload that leaves a descendant holding an inherited pipe no longer
  turns a completed, budget-compliant run into `ExecutorFailure`: the join
  budget is one shared deadline, a stranded daemon drain is abandoned, and
  the result the reaped child produced is returned.
- Overflow is flagged per stream and drains always read to EOF, so a flood
  on the payload stream can never abandon protocol result lines already
  produced on the protocol stream. `FAIL` still kills on either crossing.
- The record: a source-carrying invocation records its `source_digest` and
  no `argv`, so untrusted source and batch item payloads never persist
  verbatim. `RecordStatus.EXECUTOR_FAILED` is written when the executor
  aborts a run, and a finalize-write failure is re-attempted as
  `write_failed`, so `spawned` means only "still in flight".
- `Records.directory(path)` replaces `Records.directory_at`; the field is
  `Records.path`. `Outcome.exit_verdict` and `RunRecord.exit_verdict` are
  `ExitVerdict`, and an exit verdict on a non-payload outcome is
  unconstructable. Declaration types raise `DeclarationError`, not bare
  `ValueError`.
- Removed `STARTUP_SELF_BUDGET_SECONDS` (no enforcement site) and
  `PythonRuntime.packages` (no read site). `stream_bounds` left the public
  entry point and the fake: per-stream bounds stay scoped to the batch
  protocol channel, declared through `untrusted_python_declaration`.
- The fake rejects budget outcomes on axes the declaration never budgeted,
  including `INPUT` (a pre-spawn error) and `OUTPUT` outside `FAIL`.
- Tests: descriptor-table probes assert the child's whole table by
  `fstat` rather than a numeric filter, attribution precedence is pinned
  over both-flags-set states, record identity fields are asserted against
  independently computed hashes, and the exec-derived bounds, self-budgets
  and batch channel defaults are pinned at exact literal value.

## 2026-07-31 (declaration and record layers)

- Added `dr_exec.declare`: `Budgets` over the three v1 axes with the
  `UNBUDGETED` sentinel, `EnvironmentGrant` construction-time-frozen
  snapshots with value-private repr and a SHA-256 contents digest,
  `PROCESS_BOUNDARY_ONLY`, `ExitPolicy`, `Records`, and `HERMETIC`.
- Added `dr_exec.record`: the attribution, budget-axis, trust-category,
  record-status, and record-key `StrEnum`s, `Outcome`/`TruncationMark`/
  `Measurements`/`RunResult`, the `RunRecord` serialization model with
  its aliased wire keys, and `EXECUTOR_IDENTITY`.
- Added project scaffolding: pydantic dependency, dev group, ruff and
  pytest configuration, and a CI workflow.

## 2026-07-31 (cutover adjudication)

- Amended `v1-design.md` after a ten-reader analysis of dr-code's open
  PR stack: added use case 2 (`run_untrusted_command`), executor
  identity, a pinned-semantics section (byte budgets, never-raising
  decode, byte-exact capture, `HERMETIC` invocation shape,
  run-invariant child-observable state, thread safety), shared-budget
  and overflow-policy detail, and a named deferred-consumers list.
- Added `dr-code-cutover.md`: requirement-vs-artifact adjudication of
  every divergence between dr-code's pinned behaviors and this
  contract, the accepted behavior changes, and the `cutover/*`
  parallel-stack migration plan on a local path dependency.
- Five-lens adversarial review round (coverage, consistency,
  ambiguity, migration, adjudication) drove a second revision: added
  the `fixed()` grant shape with construction-time-frozen introspectable
  grants, the required `records` declaration, the batch prelude echo
  and protocol-channel budget, entry-point declaration symmetry, pinned
  fd-table/stdin/precedence/truncation-drain/absence/measurement/
  source-bound semantics, the pinned record wire schema, the
  OPENBLAS_NUM_THREADS requirement verdict, the sharpened
  spawn-path-correctness testing rule, rescoped cutover waves, and the
  expanded accepted-behavior-changes list.

## 2026-07-31 (consistency pass)

- Three-reviewer consistency pass over the doc system. Resolved
  contradictions in target-usecases.md (record guarantee vs best-effort,
  faithful-record vs truncation, fresh-state vs sharing, no-survivors
  scoping, machine-protection composition); retitled use case 5 to
  "Containment profiles"; added six terms and an absence attribution
  party; corrected catalog claims that earlier verification rounds had
  fixed only partially (dr-docker argv validation, code-eval find_spec,
  llmflow timeout constant, dr-queues stop protocol, symphony-lite
  narration).

## 2026-07-31

- Removed `docs/subprocess-usage-audit.md`. The fleet audit served as
  decision-support input for the initial design pass; its facts were
  re-verified against source and superseded by the curated catalogs
  (`docs/current-uses.md`, `docs/current-implementations.md`), which
  corrected several audit errors in the process.
