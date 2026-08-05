# dr-exec v1 implementation plan

The stack starts from the canonical v1 API scaffold. Source owns exact names,
shapes, defaults, and signatures; the [planned v1 contracts](contracts.toml)
own behavior, scope, and governance; the [v1 design](v1-design.md) owns
implementation mechanics and qualification requirements. This document owns
one thing: the decomposition of the remaining behavioral work into PRs. Every
public stub stays unchanged until its complete v1 behavior replaces it in one
PR.

The shared serialization capabilities specified in
[dr-serialize additions](dr-serialize-additions.md) are delivered by the
released `dr-serialize==0.1.1` pin.
Durable-directory mechanics come from the released `dr-store` pin
(`>=0.1.1`): its Document Directory component owns collision-free
allocation, atomic durable Manifest publish, streamed truncating
digest-finalized Sidecars, and verified byte-level reads.

## PR 1: substrates — identities, run store, and protocol

Three engine-independent modules — the run store and protocol consume the
identity/serialization module — each qualified in isolation inside this
one PR. Nothing engine-shaped exists yet: `Runtime` and `RunStore`
implementations become real; `ProcessExecutor` stays stubbed.

Implement:

- integration of the merged dr-serialize capabilities;
- canonical safe model projections;
- declaration, environment, executor, executor-config, and runtime identity
  construction;
- executor source snapshot;
- isolated-host interpreter probe with runtime `prepare()` and `describe()`;
- nominal SHA-256 usage at every digest boundary;
- `DirectoryRunStore` per the design's durable recording sections, composed
  over the pinned `dr_store.DocumentDirectory` (`prefix="run"`,
  `manifest_name="record.json"`): dr-exec owns the manifest model, typed
  lifecycle handles, secret-safe projection, complete and degraded
  receipts, and strict load validation wrapping the primitive's typed
  errors into `RecordingFailure` entries and `RecordLoadError`; the
  primitive owns directory allocation, atomic durable replacement, sidecar
  streaming, truncation, and digests;
- the design's Python request, driver protocol handle, and protected
  protocol sections: request transport over stdin through EOF, the
  library-owned bootstrap wrapper and protected fd 3 writer, the frame
  codec, the prelude/output/completion state machine, finite protocol
  self-budgets, and accepted-output preservation on later failure.

Qualify:

- scalar and identity golden vectors and strict read/write behavior per the
  design's scalar wire spellings and validated serialization paths;
  executor provenance forms; the fixed `-I` probe; secret-free projections;
  public API import and model validation;
- the design's directory-store qualification list under its
  synchronization rules, against the pinned primitive — filesystem
  durability mechanics are qualified in dr-store and are not re-proven
  here;
- the design's protocol golden vectors, failure taxonomy, and exact finite
  frame/aggregate/count/depth edges, including unbudgeted axes without a
  hidden finite limit.

## PR 2: single-run macOS engine

Implement one private execution path for every target per the design's
execution boundary, child descriptor, and core engine sections: declaration
and platform validation, scratch workspace, exact environment grants, argv
resolution, the library-owned spawn bootstrap, concurrent stdin, payload
output, and protocol handling, workload budget enforcement, best-effort
attribution, teardown and reaping on every post-spawn exit path, scratch
cleanup, and the mandatory run-store lifecycle. Cut over
`ProcessExecutor.run()`.

This PR deliberately carries nothing else: it is the highest-risk,
invariant-dense diff and review attention on it stays undiluted.

Qualify: the design's containment, lifecycle, and core engine requirements
across trusted-command, untrusted-command, and untrusted-Python targets,
including isolation, budget edges, attribution races, teardown before every
post-spawn exit, and recording degradation.

## PR 3: fake executor, conformance, and execution pool

Complete the public capability surface on top of a working `run()`.

Implement:

- `FakeExecutor.run()` and `calls`: a thread-safe scripted response queue
  or a declaration-dependent responder (mutually exclusive), responder
  access to the call's cancellation token, immutable call capture,
  declaration validation parity, and fake receipt enforcement;
- the shared executor conformance suite, landing alongside the second
  executor it exists to compare;
- one scheduler core behind `ExecutionPool`, `ProcessExecutor.run_many()`,
  `ProcessExecutor.open_pool()`, and `ExecutionPool.run_stream()`, with the
  admission, capacity, completion-order, backpressure, cancellation, abort,
  and drain behavior of the design's scheduling and throughput sections.

Qualify:

- concurrent call isolation, deterministic response ordering and
  exhaustion, mismatched receipt rejection, production/fake validation
  parity, and the absence of process, scratch, or record side effects;
- the design's pool behaviors through deterministic synchronization gates,
  with sync and async entry points sharing scheduler semantics and no
  future, thread, or process per queued job.
