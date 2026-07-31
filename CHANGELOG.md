# Changelog

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
