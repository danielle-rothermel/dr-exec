# Importable JSON Process Jobs

Status: selected design for local implementation planning; implementation has
not started.

## Planning sources

Repository terminology and guarantees remain authoritative in
[`../../../.defs/terms.toml`](../../../.defs/terms.toml) and
[`../../../.defs/contracts.toml`](../../../.defs/contracts.toml). During this
planning stage, use the proposed additions in [`plan-terms.toml`](plan-terms.toml)
and [`plan-contracts.toml`](plan-contracts.toml), with the required revisions
below. Backend-neutral recording is a prerequisite specified in
[`run-record-reference-plan.md`](run-record-reference-plan.md).

Historical review record:
[`perf-and-plan-audit-review-doc.md`](perf-and-plan-audit-review-doc.md) records
the performance and cross-plan feedback incorporated here. The terms,
contracts, and plans remain the current sources of truth.

The terms documents own conceptual vocabulary, the contracts documents own
standing guarantees, and this plan owns scope, selected behavior, implementation
sequence, and validation. Where this plan describes an implementation consequence
of a standing guarantee, the contract remains the source of truth.

## Goal

Add the minimum adapter that lets callers run a synchronous importable Python
callable in a fresh local child with one canonical JSON value in each direction.
The adapter must reuse the existing execution, protocol, recording, cancellation,
and scheduling paths rather than introduce another executor or pool. Release the
adapter only after real record access no longer exposes store paths.

## Selected design

### Public surface

- Represent an importable entry point with a closed serializable declaration
  containing `module_name` and `attribute_name`. Treat `module:function` only as
  display notation.
- Expose separate trusted and untrusted job-building operations. Trust must be
  explicit in the resulting target type, never an untyped boolean.
- Keep execution explicit: the builder returns an ordinary `ExecutionJob`, and
  callers pass it to their selected `Executor` or `ExecutionPool`.
- Provide one result parser that accepts a `CompletedExecution`, returns the JSON
  value on adapter success, and otherwise raises one narrow adapter exception
  without subcodes. The caller retains the completion as failure evidence.
- Add no runner wrapper, executor protocol, registry, scheduler, or pool mapping
  helper.

### Trust and Python targets

- Add a trusted Python target and matching record counterpart to the existing
  untrusted Python target.
- Both variants use the same runtime, isolated Python startup, protected
  protocol, fresh child, explicit environment, scratch workspace, budgets,
  cancellation, teardown, and recording implementation.
- The trusted variant carries no containment profile. The untrusted variant
  requires the existing process-boundary-only profile. Both run with the
  invoking user's permissions; trust does not select a stronger sandbox or a
  faster execution path.
- Classify the effective payload, not merely the imported package. A trusted
  callable that executes or interprets externally controlled candidate content
  belongs in the untrusted variant.

### Callable and import contract

- The callable is synchronous, accepts one positional strict JSON value, and
  returns one strict JSON value. JSON `null` represented by Python `None` is a
  valid result.
- Async functions, generators, keyword injection, context objects, emit
  callbacks, arbitrary object serialization, and caller-selected codecs are
  unsupported.
- Validate entrypoint syntax before spawn. Resolve the module and attribute only
  in the child; do not preflight imports in the parent.
- Require the module to be installed in the selected isolated interpreter.
  Support no working-directory imports, source paths, `PYTHONPATH` injection, or
  filesystem entrypoints.
- Provide no built-in registry. Callers that require an allowlist validate the
  declaration before building the job.

### Job granularity

- One execution job is one process-isolation, cancellation, failure-fate, and
  recording unit. It need not represent the caller's smallest logical function
  call.
- Its single JSON request may contain a finite caller-owned batch only when all
  members may share startup cost, cancellation, failure, and record evidence.
- The adapter provides no batch envelope, member interpretation, per-item
  result, retry, partial-result protocol, or mapping helper. Callers keep items
  separate when they require independent attribution or recovery.
- A Whetstone evaluation row may keep its local encode, decode, and score work
  in one job. A HumanEval candidate may run a caller-owned test-case batch in
  one job. One `dr-platform` stage remains one durable recovery unit even when
  its JSON request contains such a batch.

### JSON transport and budgets

- Wrap caller JSON in one fixed private `IdentityDocument` schema for the
  existing request and protected-output protocol, then unwrap the payload before
  invocation and after completion. Protocol position distinguishes request from
  result, so both directions use the same schema and version.
- Pin the envelope schema/version and generated driver literals with explicit
  persisted-format markers and golden tests.
- Generate driver source deterministically from the entrypoint declaration. The
  declaration therefore contributes to the existing canonical target identity;
  do not add source-version or package-identity fields.
- Job input and executor protocol budgets remain unbudgeted by default. A caller
  may select a finite per-job input budget and finite per-executor protocol
  frame, total-byte, output-count, or JSON-depth budgets.
- The adapter neither inspects executor configuration nor adds per-job protocol
  output limits. Fixed structural parser ceilings still apply when a policy
  budget is unbudgeted.
- The high-volume composition uses finite input, captured payload output,
  protocol frame, protocol total-byte, and protocol depth limits, with protocol
  output count set to one. These are existing job and executor settings, not
  adapter defaults.
- Keep canonical JSON compact. Bulk datasets, arrays, and blobs travel through
  caller-owned references or artifacts rather than through the adapter value.
- Canonicalize and hash each prepared request once, and reuse deterministic
  driver material for an entrypoint rather than rebuilding it per job.

### Interpretation and application ownership

- Reject syntactically invalid declarations and non-JSON requests before spawn.
  Import, lookup, invocation, and output failures retain the existing execution
  and protocol representations; add no wire frame or execution outcome kind.
- Treat a completion as adapter success only when it contains exactly one valid
  envelope. Validate only strict JSON and the private envelope; application
  schemas are decoded and validated by the caller after parsing.
- Inherit existing timeout, cancellation, process-group cleanup, attribution,
  and record semantics without adapter-specific evidence.
- The adapter provides no domain-effect API or side-effect-free guarantee.
  Provider policy, retries, commits, caching decisions, rewards, artifacts,
  workflow membership, recovery, and statistics remain application-owned.
- Deployment smoke tests may verify configured entrypoints out of band. Actual
  import and attribute resolution remain child-only.

### Concurrency and downstream use

- Standalone callers submit built jobs through the existing `ExecutionPool` and
  use submission context to retain caller identity.
- High-volume callers reuse long-lived runtime, executor, run-store, and pool
  instances. They select pool capacity from representative child, parent-thread,
  file-descriptor, record-I/O, and throughput measurements; capacity does not
  replace coherent job granularity.
- Each active protocol job consumes a fresh child, scratch workspace, transport
  threads and pipes, and lifecycle record writes. Pool capacity multiplies that
  resource profile rather than amortizing it.
- A durable workflow platform invokes one built job per scheduled stage without
  nesting a local pool by default.
- After a pinned release, downstream repositories delete their local process
  launchers and adapters in a hard cutover rather than wrap both paths.

### Recording prerequisite and ownership

- Land the backend-neutral run-record access plan before freezing or releasing
  the adapter API. Real lifecycle handles and receipts carry a serializable
  opaque `RunRecordReference`, and callers load records and finalized artifacts
  through the selected `RunStore`.
- Keep `DirectoryRunStore` as the initial implementation. Do not add a packed
  backend, adopt terminal `dr-store` artifact bundles, or weaken durable
  lifecycle semantics in the adapter change.
- Measure the directory store at representative volume after the reference
  cutover. A packed backend belongs in a later plan only if per-job evidence at
  the measured rate requires it.
- Do not describe the directory store as a 100,000-record-per-hour solution
  without that measurement.
- Retention, root partitioning, archival, and deletion remain run-store or
  operator policy. Caller batching reduces record count only when batch-level
  evidence and failure fate are intentional.

### Cross-repository ownership

- `dr-exec` owns local process execution and mutable execution-record lifecycle.
- `dr-store` owns generic content-addressed records and terminal artifact
  bundles; neither is substituted for a mutable run record by this plan.
- `dr-providers` owns provider calls and retries. Long-lived provider processes
  retain their connection pools.
- `dr-platform` owns durable scheduling and one recovery unit per admitted work
  item, without a nested local pool by default.
- Applications own JSON contents, application-level batching, result schemas,
  references or artifacts, and downstream effects.
- Neither plan introduces new `dr-store` or `dr-providers` coupling.

## Terminology and contract follow-ups

The proposed terms define the importable job as one isolation, cancellation,
failure, and recording unit whose exchange is subject to declared budgets. They
also add `run record reference` for backend-neutral record access. No application
batching term is needed. When the trusted Python target exists, add its exported
symbol to the existing `trusted payload` term rather than define another trust
term.

The proposed contracts own the following standing guarantees:

- `Callable input and output are JSON`: record the unbudgeted defaults, optional
  finite existing budgets, and fixed private identity envelope.
- `Importable JSON process jobs reuse the execution path`: record that trusted
  and untrusted Python targets share one mechanism and differ only in their trust
  and containment declarations and corresponding evidence.
- `Scheduling and domain effects remain caller-owned`: record the job fate unit
  and caller-owned finite batch without adding batch infrastructure.
- `Run-record access is store-owned and backend-neutral`: replace public paths
  with references, bounded artifact reads, and store-owned layout and retention.

The private envelope's exact schema/version is a persisted-format contract even
though it is not a public worker type. Its literals require contract markers and
golden tests in source; it does not require another conceptual term.

## Implementation sequence

1. Land [`run-record-reference-plan.md`](run-record-reference-plan.md), including
   its terminology, contract, hard cutover, and conformance evidence.
2. Apply the adapter terminology and contract follow-ups above, then freeze the
   adapter API and private envelope literals.
3. Add the trusted Python target and record variant, and broaden the runtime and
   shared declaration handling to both Python target variants.
4. Implement the deterministic importable-callable driver and private JSON
   envelope over the existing Python protocol.
5. Implement the trusted and untrusted job builders plus the shared result
   parser and narrow parser exception.
6. Add focused fake, real-process, trust, budget, record, and parser coverage.
7. Add a repeatable representative performance investigation outside ordinary
   timing-based tests.
8. Update public exports and package documentation, run the full repository
   validation, and release a pinned version.
9. Plan downstream hard-cutover migrations from the released API and final
   contracts.

## Validation bar

- Declaration tests cover module and attribute syntax, explicit trust selection,
  caller-owned allowlists, and deterministic target identity.
- Real-process tests use an installed fixture module and cover valid values,
  JSON `null`, unknown modules and attributes, non-callable attributes, worker
  exceptions, non-JSON results, nonzero exit, timeout, and cancellation.
- Parser tests cover zero, one, and multiple accepted outputs without adding new
  wire or outcome variants.
- Shared conformance tests establish equivalent callable and protocol behavior
  for trusted and untrusted Python targets; record assertions cover their
  intended declaration differences.
- Budget tests cover unbudgeted defaults, finite per-job input and captured
  payload output, and finite per-executor protocol frame, total-byte,
  output-count, and depth limits. The high-volume composition sets output count
  to one.
- Fake-executor tests cover job construction, exact call capture, and result
  interpretation without claiming import, runtime, containment, or durable-record
  behavior.
- Existing pool state and context are sufficient for standalone fan-out; no new
  scheduler state or mapping helper is introduced.
- A caller-owned batch example proves that the adapter neither interprets nor
  assigns independent fate to its members.
- A repeatable benchmark reports sustained jobs per second across representative
  capacities; child, parent-thread, and file-descriptor peaks; realistic import
  and payload sizes; and record object counts, logical bytes, and synchronization
  counts. Ordinary tests contain no timing assertions.
- Successful and failed real adapter paths retain evidence loadable through
  `RunRecordReference`; artifact reads exercise their required finite bound.
- Interleaving tests synchronize on explicit state rather than time.
- Public exports, terminology, contracts, persisted literals, documentation,
  formatting, lint, types, build, and full tests all pass before release.
