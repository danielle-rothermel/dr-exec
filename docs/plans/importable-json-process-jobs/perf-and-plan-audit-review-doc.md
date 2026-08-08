# Performance and Cross-Plan Audit Instructions

Status: required additions to the implementation plan and one prerequisite
recording decision before release.

## Purpose

Keep the importable JSON adapter a small reuse of the existing execution path
while making its intended job granularity, resource profile, and recording
boundary safe for the later workflow stack.

The adapter plan, proposed terms, and proposed contracts must incorporate the
instructions below before its source API and private envelope are frozen.

## Cross-repository ownership

- `dr-exec` owns local process startup, isolation declarations, protocol
  transport, limits, cancellation, teardown, and execution-record lifecycle.
- `dr-store` owns generic async content-addressed records and terminal local
  artifact bundles. Neither model directly represents a mutable `dr-exec`
  lifecycle record.
- `dr-providers` owns provider calls and retry transitions. Provider HTTP calls
  should remain in long-lived provider processes so connection pools are reused.
- `dr-platform` owns durable scheduling and one workflow recovery unit per
  admitted work item. It does not nest an `ExecutionPool` by default.
- Applications own the logical contents of one JSON request, application-level
  batching, result schema, and downstream effects.

Do not add a dependency from `dr-exec` to `dr-store` or `dr-providers` in this
plan.

## Decisions to retain

Retain the selected adapter design:

- a fresh child for every execution job, with no warm-worker variant;
- one importable module plus one module-level attribute declaration;
- explicit trusted and untrusted builders over the same mechanism;
- one strict JSON value in each direction through the protected protocol;
- an ordinary `ExecutionJob` plus a separate completion parser;
- no registry, scheduler, mapping helper, custom codec, or domain callback; and
- reuse of existing execution, cancellation, pool, and record semantics.

Warm children would change isolation and state-contamination semantics and are
not a performance optimization for this primitive.

## Required adapter revisions

### Define job granularity explicitly

One execution job is one process-isolation, cancellation, failure-fate, and
recording unit. It is not necessarily one smallest logical function call. The
single JSON request may contain a finite caller-owned batch when every item may
share startup cost and the same cancellation and failure fate.

`dr-exec` supplies no batch envelope, per-item retry, partial-result protocol,
or mapping helper. Callers must not combine items merely to increase throughput
when they need independent attribution or recovery.

For the intended downstream paths:

- one Whetstone evaluation row may keep its local encode/decode/score work in
  one job rather than spawning a process for each internal function;
- one HumanEval candidate may execute a caller-owned batch of test cases in one
  job; and
- one `dr-platform` stage remains one durable recovery unit even when its JSON
  payload contains a deliberate local batch.

### Document the high-volume resource profile

High-volume callers reuse long-lived runtime, executor, run-store, and pool
instances. They select pool capacity from representative measurements of child
count, thread count, file descriptors, record I/O, and sustained throughput.
Increasing capacity is not a substitute for coherent job granularity.

Keep globally unbudgeted defaults, but make the paved high-volume composition
explicit: finite job input bytes and finite protocol frame bytes, total bytes,
JSON depth, retained payload output, and output count of one. Bulk datasets,
arrays, and blobs travel by caller-owned references or artifacts; canonical
JSON carries compact control data and results.

The implementation should canonicalize and hash each prepared request once and
reuse deterministic entrypoint-dependent driver material. Deployment smoke
tests may verify configured entrypoints out of band, while actual import and
attribute resolution remain child-only.

## Required recording prerequisite

The public `RunStore` and real record receipts expose `Path`-shaped record
locations. That freezes `DirectoryRunStore` into every downstream persisted
receipt and prevents a packed or database-backed run store without another
schema cutover.

Before the adapter release expands downstream use, create a separate predecessor
plan and PR that hard-cuts recording to a serializable opaque
`RunRecordReference` owned by the run-store contract:

- `PreparedRun`, `RunningRun`, complete receipts, and degraded receipts carry
  the reference rather than `record_dir`;
- `RunStore.load` accepts the reference;
- the generic run-store surface supplies the artifact-read operation needed to
  recover finalized stdout/stderr without joining a public path; and
- `DirectoryRunStore` resolves its references and relative artifact names
  internally while preserving the existing durable lifecycle guarantees.

The reference must contain only the stable locator needed by the selected store,
not a filesystem path disguised as a string. Its persisted keys and
discriminator require contract markers and golden tests. The predecessor plan
must decide store/reference mismatch behavior, artifact streaming or bounded
read behavior, and how cached receipts identify their source record.

Do not silently implement a packed backend in the adapter PR. First land the
backend-neutral reference and access contract. Keep `DirectoryRunStore` as the
initial implementation, then measure representative recording. Add a packed
backend in its own plan only if per-job durable evidence at the measured volume
requires it.

`dr-store` artifact bundles are not a drop-in backend: they are terminal,
directory-shaped, and intentionally make a weaker crash-durability claim than
the current execution lifecycle.

## Recording and batching operational policy

The durable directory store remains valid for the immediate experiment scale,
but it must not be described as a 100,000-record-per-hour solution without
measurement. Caller batching lowers record count only by deliberately accepting
batch-level evidence and failure fate; it is not a substitute when per-item
attempt records are required.

Retention, root partitioning, archival, and deletion remain run-store/operator
policy. The adapter does not add them.

## Delivery instructions

### Predecessor PR: backend-neutral run-record access

Finalize the recording reference and artifact-access contract, hard-cut the
directory implementation and all receipts/callers, preserve durable lifecycle
semantics, and add conformance coverage that a non-path reference is sufficient
to load records and outputs.

### Adapter PR

Apply the existing implementation sequence after the predecessor lands. Amend
the proposed term and contracts to state declared budgets and the job-isolation
unit, without defining an application batching concept. Add the documented
finite high-volume configuration and compact-JSON guidance.

### Later packed-store decision

Use the representative benchmark below to decide whether a packed store is
needed. Its design belongs to `dr-exec`'s mutable execution-record contract; it
may use a lower-level storage capability only if that dependency preserves the
run-store lifecycle and synchronous executor boundary truthfully.

## Validation evidence

Keep functional tests state-synchronized. Put throughput measurements in a
repeatable benchmark or investigation script rather than timing assertions in
the ordinary suite.

The implementation handoff must include:

- sustained jobs/second across representative pool capacities;
- child, parent-thread, and file-descriptor peaks;
- representative import and payload sizes rather than only a trivial callable;
- record file/object counts, logical bytes, and synchronization counts;
- behavior under the finite high-volume budget composition;
- a caller-owned batch example proving that dr-exec does not interpret its
  members; and
- proof that all successful and failed adapter paths retain loadable execution
  evidence through `RunRecordReference`.
