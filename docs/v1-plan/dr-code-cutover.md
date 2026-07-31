# dr-code cutover plan

The migration of dr-code's open `rebuild/*` PR stack onto dr-exec.
Companion to `v1-design.md`, which owns the executor contract this plan
applies.

Record shape the cutover reads: untrusted-source runs record the source
digest, not verbatim source, in the identity (`argv` is null for source
forms — the redaction and content-addressing rule), and
`RecordStatus.EXECUTOR_FAILED` is a terminal status. dr-code's cache-key
and provenance derivation binds to these, and there is no old-shape
recorded data to migrate (the cutover stack is built fresh).

## Authority principle

The dr-exec contract is the deliberate design; dr-code's pinned
behaviors are prior art under review. dr-code's execution decisions were
made incidentally, in service of features, without the design pass this
repo's contract represents — so a pinned behavior earns preservation
only by embodying a real requirement, never by being pinned. Each
divergence below carries a verdict:

- **Requirement** — the pin encodes a genuine obligation (attribution
  fidelity, protocol protection, no-deadlock I/O). Honored, expressed in
  the dr-exec contract's vocabulary.
- **Artifact** — the pin is an accident of how dr-code happened to be
  built. Replaced; dr-code's tests, schemas, and persisted formats are
  re-pinned to the new contract in the cutover stack.

Per the hard-cutover convention: no compatibility shims, no
behavior-preserving workarounds, no coexistence of old and new. Recorded
dev data is archived, never destroyed — and never justifies preserving a
schema.

Attribution vocabulary note: in this document, "attributed" used alone
means dr-exec's executor attribution. dr-code's scoring layer *maps*
attributions onto domain verdicts (an output-budget attribution is
scored against the candidate); that mapping is domain judgment, written
here as "scored against", never as "attributed to".

## Divergence adjudication

### Requirements — honored in dr-exec vocabulary

1. **Three-way budget discrimination.** Wall-clock, output-overflow, and
   infrastructure failures have three different downstream meanings
   (scored against the candidate vs invalidating the submission).
   Honored as data: budget attribution names the violated axis;
   executor/channel/machine attributions are distinct values, decided
   under the pinned precedence rule.
2. **Signal fidelity.** Raw negative returncodes (−SIGKILL, −SIGSEGV)
   are semantically load-bearing for candidate-crash classification.
   Honored: `RunResult.returncode` is raw; executor-inflicted kills are
   distinguished by budget attribution, so consumers branch on
   attribution before interpreting returncodes.
3. **Nonzero exit delivers evidence.** A nonzero exit must arrive with
   returncode and captured stderr (the classifier lane's bounded
   diagnostics). Honored: outcomes are data; every spawned run returns a
   full `RunResult`.
4. **Spawn absence is distinct, and finer than "spawn failed".** Missing
   executable (ENOENT) is distinguished from other start failures
   (EACCES et al.). Honored: spawn absence attribution exactly on
   ENOENT; other spawn errnos land as machine attribution with the errno
   preserved.
5. **Never-raising decode.** Hostile bytes on either stream must still
   yield a scoreable string. Honored as pinned semantics
   (`errors="replace"`).
6. **No-deadlock I/O at the input bound.** Concurrent feed/drain,
   including a child that reads stdin only after writing. Honored as an
   engine invariant.
7. **Group teardown including descendants, race-safe, budgeted.**
   Grandchildren observably dead within the executor self-budget for
   termination on every exit path. Honored; both the `os.killpg`
   reap-race fault-injection suite *and* the real-descendant liveness
   tests (deadline, overflow, and normal-exit paths) migrate into
   dr-exec's engine suite.
8. **Protocol-stdout protection.** Payload prints must not corrupt
   protocol output. Honored in the driver kit; the fd-level hole stays a
   declared profile limit.
9. **Byte-exact capture, real pipes, deterministic descriptor table.**
   The mutants oracle's sentinel-envelope protocol and fd-dup
   anti-spoofing require capture with no executor framing, dup-able
   descriptors, and a child fd table of exactly 0/1/2. Honored as
   pinned semantics.
10. **Partial results survive failure.** Honored and strengthened:
    NDJSON incremental delivery with a prelude echo makes delivered
    results trustable the moment they arrive; a protocol-channel budget
    separate from the payload streams means a noisy payload costs only
    its own items, never the batch.
11. **Deterministic child-observable state.** The mutants double-run
    determinism gate requires that the executor inject nothing
    run-varying. Honored as pinned semantics, with the scratch-cwd path
    documented as the single caveat.
12. **Thread-safe, duration-bounded calls.** The corpus evaluator's
    thread pool and lease heartbeats require both. Honored as pinned
    semantics.
13. **Hermetic interpreter invocation.** `-I -c <source>` semantics
    (no `PYTHON*` env, no user site, `<string>` tracebacks, no
    `__file__`) are load-bearing for isolation and determinism. Honored
    as the pinned `HERMETIC` invocation shape, with the source-size
    bound derived from platform exec limits.
14. **`OPENBLAS_NUM_THREADS=1` in the Python child environment.** A
    determinism and thread-oversubscription control, not a nicety: BLAS
    thread count changes float reduction order (the mutants determinism
    gate would see spurious nondeterminism) and unpinned BLAS threads
    oversubscribe the corpus evaluator's 4-way process pool. Honored:
    dr-code declares `EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS":
    "1"})` at its Python-execution call sites — the fixed environment
    passthrough shape added to the contract for exactly this (`HERMETIC`
    itself injects nothing; the child environment is solely the declared
    environment passthrough).
15. **Fake cannot claim production identity.** dr-code's corpus
    evaluator refuses an injected runner claiming the production
    identity. Honored library-side: `FakeExecutor` refuses construction
    with `EXECUTOR_IDENTITY`.

### Artifacts — replaced, with dr-code re-pinned

1. **Exception-class dispatch and persisted *executor* exception
   names.** `except SubprocessTimeoutError` control flow;
   `failure_type` / `exception_type` strings in Parquet artifacts and
   resume state *where they name executor failure classes*. → Outcomes
   are data; consumers branch on attribution and persist dr-exec's
   pinned attribution literals. Carve-out, explicitly: payload-observed
   exception identity — `dr_code.mutants.outcomes.ErrorOutcome
   .exception_type` and the HumanEval driver's per-case
   `exception_type`, which record what the *candidate* raised — is
   payload data observed by the driver, preserved byte-for-byte. Only
   executor-failure vocabulary changes. Likewise dr-code-synthesized
   literals (`'AbandonedInfrastructureAttempt'`) are dr-code's own and
   stay dr-code's to define.
2. **Whole-array JSON batch protocol.** One JSON list emitted at child
   exit; a late death loses the batch; a batch timeout fans out
   all-cases-TIMEOUT with `elapsed_seconds = budget`. → NDJSON
   incremental delivery with prelude echo and terminal completion line.
   Completed items survive any death; the parent synthesizes outcomes
   for missing items from the run's outcome. Recorded fact semantics
   (timeout/error counts, elapsed values) change accordingly.
3. **Abort-and-discard on output overflow.** Overflow kills the run and
   discards all captured output. The distinct-outcome half is a
   requirement (above); the discard half is an artifact. → `FAIL` output
   overflow policy: killed, budget-attributed, captured-so-far output
   retained with marked truncation. Every protocol consumer branches on
   attribution *before* parsing captured output — the mutants oracle's
   `_parse_outcomes` gains a mandatory pre-branch (budget/output →
   execution failure, never a protocol error), and the HumanEval
   adapter likewise.
4. **Executor-owned budget constants.** 1 MiB output and 4 MiB input as
   module constants — interior defaults by the contract's definition. →
   All axes caller-declared (or explicitly unbudgeted); dr-code
   declares its bounds as contract budgets at the call sites, where the
   protocol knowledge lives. (The distinguishing test is derivation:
   dr-exec's own source-size bound survives because it derives from a
   real machine constraint, `MAX_ARG_STRLEN`; dr-code's constants
   derive from nothing.) Consequence: budget declarations enter
   `CodeTestSettings` and therefore every `question_identity_hash` —
   see accepted changes.
5. **Sentinel returncodes in the execution cache.** Out-of-band magic
   returncodes standing in for budget violations in
   `dr_code.metrics.engine.execution.ExecutionOutcome` (not the
   unrelated `dr_code.mutants.outcomes.ExecutionOutcome`, which is a
   payload wire schema and untouched by this item). → Structured
   outcome fields; schema change, goldens re-pinned, prior recorded dev
   data archived.
6. **Cache key as an incidental shape.** The current key is the
   four-tuple (`computation_id`, source, input_text, timeout_seconds).
   `computation_id` (`humaneval-runner@v1`) is deliberate domain
   namespacing and is *retained*; the rest of the shape is incidental.
   → Key derived from `computation_id` plus dr-exec's declared
   invocation identity (executor identity + source digest + input
   digest + budgets in force + environment-passthrough names/value
   digest + profile + runtime), pinned by a golden test in dr-code so
   drift is loud.
7. **Runner identity by callable `is`-check.** `generate.py`'s
   `runner is run_python_subprocess` check (the corpus evaluator's
   None-check-plus-declared-string is already the right shape). →
   `EXECUTOR_IDENTITY` for the executor half. The *runtime* half
   (platform/interpreter provenance, `current_runtime_identity()`)
   stays dr-code's — executor identity and runtime identity answer
   different questions, and dr-exec's version string is
   machine-invariant, so it can never substitute for the cross-machine
   validation gate.
8. **Environment handling as ad-hoc per-site idioms.** Inherit-all on
   the generic path; copy-then-overlay in the test module runner; a
   construction-time allowlist snapshot in the classifier lane. → The
   environment passthrough vocabulary, matched to what each site
   actually means:
   - Classifier lane: `EnvironmentGrant.named(<behavior allowlist ∪
     matched secret names>)` — a deny-by-default snapshot frozen at
     lane construction (`named()` resolves at construction by contract),
     so the persisted `environment_identity` stays derivable from the
     passthrough declaration's introspectable contents and reproducible
     across runs. The suffix predicate stays dr-code's; the executable
     `shutil.which` + SHA-256 pin stays caller-side. Never `overlay()` —
     that would both widen an untrusted agent CLI's reach and make the
     identity hash a function of the operator's shell.
   - Python execution sites: `fixed({"OPENBLAS_NUM_THREADS": "1"})`
     (requirement 14).
   - Self-invocation module-runner fixture: `run_tool` with
     `overlay(extra={COLUMNS, NO_COLOR, PYTHONHASHSEED=0})` — genuinely
     ambient-plus-extras, and deliberately *not* the hermetic Python
     runner, whose `-I` would strip `PYTHONHASHSEED` and defeat the
     fixture's determinism intent.
9. **Inherited cwd.** Children run in the parent's cwd. → Per-run
   scratch cwd. Accepted behavior change; the self-invocation probes
   resolve modules via the installed package, not cwd (verified: the
   editable `.pth` install makes `python -m dr_code.*` work
   cwd-independently). Payloads reading their cwd see a run-varying
   path (documented caveat, with the cache consequence in accepted
   changes).
10. **Raise-on-timeout ergonomics in test helpers and consumers.**
    `TimeoutExpired`/`SubprocessTimeoutError` escapes as the timeout
    path; test assertions match exception message text (`"exceeded"`). →
    Outcome branching; tests re-pin on attribution fields, not message
    strings.
11. **Whole-batch timeout as uniform per-case TIMEOUT.** → Incremental
    results plus run-outcome-derived synthesis for the unfinished tail.
    Eval-fact outcomes may shift where a slow early item previously
    destroyed completed results; this is the intended semantics, not
    regression.
12. **Infrastructure retry keyed on one exception class.** The corpus
    evaluator retries every `SubprocessError` up to
    `max_infrastructure_retries`. → The retriable set is rebuilt from
    two sources and its membership deliberately changes: retry on
    channel and machine attributions and on raised executor failures;
    never retry spawn absence (a missing interpreter is not transient)
    or budget attributions. See accepted changes for the observable
    consequence.

### Accepted behavior changes (the validation list)

The cutover deliberately changes observable dr-code behavior where the
dr-exec contract is the better design. For post-hoc validation, the
complete list:

- Failure-taxonomy vocabulary in persisted artifacts (Parquet columns,
  resume state, JSONL records) switches from *executor* exception class
  names to dr-exec attribution literals. Payload-observed exception
  identity is unchanged (adjudication artifact 1's carve-out).
- Batch wire format switches to NDJSON incremental with prelude echo;
  partial results survive child death and batch timeout; per-case
  `elapsed_seconds` under timeout reflects real elapsed work, not the
  whole budget; timeout/error fact counts can differ on the same
  inputs.
- Output overflow retains marked-truncated partial output instead of
  discarding; dr-exec attributes it to the output budget, and dr-code's
  domain mapping continues to score it against the candidate
  (unchanged).
- Execution cache keys and
  `dr_code.metrics.engine.execution.ExecutionOutcome` change; dedupe
  identity now includes executor identity and input digest; prior cache
  entries are archived, not migrated. A dr-exec version bump changes
  `EXECUTOR_IDENTITY` and therefore invalidates execution caches — a
  deliberate property of pinned-dependency identity.
- Declaring output/input budgets at the `code_test` call site changes
  `CodeTestSettings` and therefore every `question_identity_hash`,
  `evaluation_identity`, and `candidate_evaluation_key`; the HumanEval
  scoring profile version bumps carrying the full budget set. Prior
  evaluation generations are archived, not migrated.
- Corpus/evaluation runtime identity (`checkout_source_tree_sha256`,
  `installed_environment.identity`) changes when dr-exec enters
  `pyproject.toml`/`uv.lock`, and changes again at the
  path-source-to-pinned-release swap; generations produced during the
  development phase are not reproducible after the swap and are
  archived.
- Spawn absence failures are no longer retried (previously retried as
  `SubprocessStartError` infrastructure attempts): `attempt_count` and
  terminal `record_status` change for missing-interpreter scenarios.
- Children run in per-run scratch directories with explicitly granted
  environments; candidates that read cwd or ambient env behave
  differently. Cache consequence, accepted: a candidate that reads its
  cwd is nondeterministic across runs, and the execution cache serves
  the first observed outcome — folding the scratch path into the key
  would make the cache a no-op.
- Mutant dataset identity can change: the canonical determinism gate
  now observes a run-varying scratch cwd, and timeout-as-data changes
  which tasks reach the "canonical execution did not complete" skip
  branch. Persisted skip-reason literals in generated datasets are
  re-pinned. Regenerated datasets are new datasets, not corrupted old
  ones.
- The parity and real-engine oracle test suites run on the real engine
  with `Records.none()` (sanctioned by the contract's testing rule);
  logic tests move to `FakeExecutor`. In-child driver-body code
  (`failure_metadata`'s re-execution, clipping, per-case timing) keeps
  real-execution coverage in dr-code. Driver tracebacks persisted into
  eval records (`File "<string>", line N` frames) shift line numbers
  with the driver rewrite — re-pinned, archived.
- CI for the cutover stack checks out dr-exec as a sibling workspace
  path during the development phase, and the packaging test's
  clean-install and wheel-reproducibility assertions are marked
  xfail while the path source is in force (a path source does not
  propagate into wheel metadata and dr-exec is not yet on an index);
  both re-enable at the pin-swap commit. Red-CI-free review signal
  during the phase depends on the checkout step working against the
  dr-exec repo — a validation item.

## Migration mechanics

### Dependency

`cutover/01` adds dr-exec to dr-code's `pyproject.toml` as a local path
dependency via `[tool.uv.sources]` (editable, relative sibling path),
so the two repos iterate in lockstep during development. Consequences
handled up front, not discovered:

- CI: the dr-code workflows gain a step checking out dr-exec into the
  runner workspace (`path: dr-exec`, with the uv source pointing at
  that layout) so `uv sync` resolves on runners; cross-repo checkout
  credentials are a setup item surfaced for validation.
- Packaging: `[tool.uv.sources]` does not survive into wheel metadata,
  so `tests/packaging/test_installed_viewer_wheel.py`'s clean-install
  smoke and byte-reproducibility assertions cannot pass while the path
  source is in force; they are marked xfail with the pin-swap commit
  named as the re-enable gate.
- After the design merges and dr-exec publishes to PyPI, one commit
  swaps the source entry for a pinned release, drops the CI checkout
  step, and re-enables the packaging assertions — the
  boundary-crossing discipline the defensive-design convention
  requires.

### The parallel stack

A new stack `cutover/01..09` parallels `rebuild/01..09` branch-for-
branch — same feature content, execution routed through dr-exec, pins
updated to the new contract. Parallel means one PR per original PR;
it does *not* mean scopes match the original commit boundaries — work
lands where the code it replaces actually lives. The `rebuild/*` stack
stays open until Danielle reviews both; `cutover/*` supersedes it on
approval, at which point `rebuild/*` closes per the one-open-PR-set
rule.

Before wave work starts, generate the complete consumer inventory
(`git grep -l "dr_code\.execution\|dr_code\.execution\.subprocess"` on
`rebuild/09`) and assign every hit to a wave; the wave notes below name
the known non-obvious ones.

- **cutover/01** ← rebuild/01: adds the dr-exec dependency (uv source +
  CI checkout step); deletes `dr_code/execution/subprocess.py` outright
  (no port — dr-exec is the implementation). Because the deleted
  module's consumers all land in rebuild/01, this wave also rebuilds:
  the HumanEval batch adapter on `dr_exec.batch`'s driver kit and
  NDJSON protocol (keeping case semantics and the candidate/harness
  domain mapping, shedding the generic protocol half);
  `metrics/engine/execution.py` (sentinels deleted,
  `ExecutionOutcome` restructured with attribution fields, cache key
  per adjudication artifact 6 with a golden test);
  `metrics/operators/code_test.py`'s outcome interpretation; and
  `metrics/engine/engine.py`'s executor seam. Test doubles replaced by
  `FakeExecutor`; parity and real-engine oracle suites move onto the
  real engine with `Records.none()`; lifecycle fault-injection and
  descendant-liveness tests are deleted here (they live in dr-exec).
  Call sites declare budgets, environment passthrough, records.
- **cutover/02** ← rebuild/02: test-suite baseline; the `python -m`
  module-runner fixture rides `run_tool` with the
  `overlay(COLUMNS, NO_COLOR, PYTHONHASHSEED)` environment passthrough
  and scratch cwd; contract tests re-pin on outcome fields, never
  message text.
- **cutover/03** ← rebuild/03: eval kernel rebuild on the seam
  cutover/01 established; scoring-profile version bump carrying the
  declared budget set; `HumanEvalScoringProfile` and eval-record
  goldens re-pinned.
- **cutover/04** ← rebuild/04: preprocessing; `CandidateHarnessFailure`
  persists attribution literals (executor vocabulary only — payload
  exception identity preserved); per-candidate fan-out unchanged (an
  aggregate budget across candidates remains the caller's declaration,
  out of executor scope).
- **cutover/05** ← rebuild/05: corpus evaluation; runner identity =
  `EXECUTOR_IDENTITY`, runtime identity stays
  `current_runtime_identity()`; retry classification per adjudication
  artifact 12; record directories declared at the evaluator's call
  sites; git provenance capture stays on stdlib `subprocess` (named
  deferred consumer).
- **cutover/06** ← rebuild/06: analysis viewer; upstream schema changes
  flow through; build hooks and packaging tests stay stdlib (named
  deferred consumers) with the packaging xfail from the dependency
  section; `tests/viewer/test_domain.py`'s
  real-runner-with-injected-identity fixture is re-decided — it runs
  the real engine under the real `EXECUTOR_IDENTITY` and the fixture's
  recorded evaluation identity updates (the spoofed-identity shape is
  unconstructable by design).
- **cutover/07** ← rebuild/07: behavioral mutants; oracle keeps its
  sentinel-envelope protocol over `run_untrusted_python` with a `FAIL`
  output overflow policy and a mandatory attribution pre-branch before
  envelope parsing; runner identity from dr-exec, runtime identity stays
  dr-code's; `is`-check deleted; determinism gate re-pinned;
  `tests/mutants/test_boundaries.py`'s AST module-path pin re-pinned to
  `dr_exec.run`.
- **cutover/08** ← rebuild/08: task annotations; execution-free,
  near-verbatim port.
- **cutover/09** ← rebuild/09: failure classifier; `SubscriptionLane`
  rides `run_untrusted_command` with `PROCESS_BOUNDARY_ONLY` and the
  `named()` snapshot environment passthrough (adjudication artifact 8);
  `LaneTransportError` mapping keyed on attribution;
  `MAX_SUBPROCESS_OUTPUT_BYTES` imports re-pinned to the lane's own
  declared budget constant; the interactive flock/rendezvous tests in
  `test_lane.py` *and* `test_cli.py`'s two-`Popen` lease-contention
  test stay raw `Popen` (named deferred consumers); grandchild-kill
  tests deleted here with their coverage living in dr-exec's engine
  suite.

PR #73 (`static-viewer`) is execution-free static assets on top of
rebuild/01; it re-parents onto cutover/01 (`gt track --parent`) *before*
rebuild/01 closes so it is never orphaned. Its content is unchanged,
but the packaging test's wheel-reproducibility assertion is re-verified
with #73 in the stack once the pin swap re-enables it, since cutover/01
changes `pyproject.toml`.

### What each side owns afterward

dr-exec: spawn, lifecycle, budgets, environment passthrough, capture,
batch transport, attribution, records, executor identity, the fake, and
the spawn-path test suite. dr-code: HumanEval schemas and case semantics,
candidate/harness domain mapping (e.g. SIGSEGV → "candidate crashed"),
scoring, caching policy and key derivation (including
`computation_id`), retry policy, runtime identity, the classifier's
environment allowlist and executable pinning, and everything it
persists.
