# Importable JSON Process Jobs

Status: design plan for local refinement; implementation has not started.

## Purpose

Ensure `dr-exec` is the one canonical foundation for invoking isolated local
Python work by an importable entry point with bounded JSON input and output.

The immediate consumers need to run evaluation-row workers in fresh processes
without maintaining a second process launcher, cancellation model, or local
pool. The new surface should be only the minimum adapter needed above the
existing execution engine.

## Existing foundation

`dr-exec` already owns `ExecutionJob`, the `Executor` protocol,
`ProcessExecutor`, execution records, cancellation/containment behavior, and a
bounded `ExecutionPool` for standalone local concurrency.

Application repositories also contain fan-out helpers that identify a Python
callable by module and name, pass serialized input, decode serialized output,
and manage process groups. Their domain callbacks and scheduling policy do not
belong in `dr-exec`, but the importable-callable process mechanism does.

The first design task is to determine whether the current public contract can
already express this safely. If it can, add only a documented adapter and
tests. Do not introduce a parallel executor abstraction.

## Intended ownership

### Importable JSON job adapter

Provide the narrowest useful representation of:

- an importable Python entry point, such as `module:function`;
- bounded canonical JSON input;
- an explicit Python runtime and execution limits inherited from
  `ExecutionJob`; and
- bounded canonical JSON output or a typed execution/protocol failure.

The adapter should construct or wrap an ordinary `ExecutionJob` and execute
through the existing `Executor`. Process creation, environment control,
timeouts, output bounds, cleanup, process-group containment, cancellation, and
recording must continue to have one implementation.

Entrypoint resolution must be auditable and fail closed. The design must state
whether arbitrary import paths are allowed, whether a caller provides an
allowlist/registry, and what module/runtime identity is recorded. JSON is the
process boundary; arbitrary Python object serialization is not supported.

The worker returns data, not domain side effects. Application-specific decode,
commit, caching, reward, and evidence publication happen after the foundational
execution result crosses back to the caller.

### Local concurrency

`ExecutionPool` remains the only local bounded-concurrency primitive. It may be
used by standalone `dr-code` or research commands.

When a durable workflow platform already schedules individual work items, each
stage should invoke `ProcessExecutor` directly. Nested pools must not become
the default integration pattern.

### Records and failure taxonomy

The adapter should preserve the underlying execution record and add only
protocol-specific facts that cannot be inferred from it, such as invalid JSON,
entrypoint-resolution failure, or output-schema rejection. Persisted keys and
discriminators require explicit contract markers and golden tests.

## Explicit non-goals

This foundation does not own:

- provider response classification, retry, backoff, or rate limiting;
- durable workflow scheduling, run membership, fan-in, or recovery;
- evaluation tasks, partial-row logs, resume policy, reward, or statistics;
- domain-specific commit callbacks or artifact manifests;
- arbitrary remote execution or a distributed queue; or
- arbitrary Python object serialization.

The existing process executor remains the paved road. Any future remote
implementation would need to satisfy the frozen `Executor` contract and its
conformance tests rather than expand this adapter opportunistically.

## Design questions to finalize locally

1. Can the adapter be implemented entirely as a job builder and result parser,
   with no new executor protocol?
2. Is `module:function` sufficient, or must callers provide an explicit
   registry/allowlist to make resolution safe and portable?
3. Which canonical JSON implementation and maximum input/output sizes form the
   public boundary?
4. How are entrypoint identity, Python environment identity, and source version
   represented without claiming portability the runtime cannot prove?
5. Which protocol failures are adapter results versus ordinary process
   failures?
6. What cancellation evidence is guaranteed if termination happens before any
   valid output is produced?
7. Does `ExecutionPool` already expose every state needed by standalone
   consumers, or is only a small mapping helper missing?

## Implementation sequence

1. Map the application fan-out requirements onto current `dr-exec` contracts
   and identify the irreducible missing surface.
2. Freeze the entrypoint, JSON envelope, limits, failure model, and identity.
3. Implement the smallest adapter over `ExecutionJob` and `Executor`.
4. Add conformance coverage for both fake and real process executors where the
   semantics apply.
5. Verify standalone bounded fan-out through `ExecutionPool` without adding a
   second scheduler.
6. Update public exports, terminology, contracts, and package documentation.
7. Release a pinned version before application migrations delete their local
   process launchers.

## Validation bar

- Valid importable jobs produce the same structured result through fake and
  real executor paths where meaningful.
- Tests cover unknown modules/functions, invalid input JSON, invalid output
  JSON, oversized input/output, nonzero exit, timeout, cancellation, and
  process-group cleanup.
- Interleavings use explicit synchronization rather than sleeps.
- The underlying execution record remains available for every terminal path.
- Pool tests establish exact admission, ordering contract, and terminal result
  cardinality without relying on elapsed time.
- Documentation clearly separates local pool concurrency from durable workflow
  scheduling.

## Downstream handoff

After release, inspect the final adapter and execution contracts before
planning `dr-platform` or `dr-code` changes. Standalone code can then use
`ExecutionPool`; platform-scheduled work can invoke one process job per durable
stage. Both paths should delete local process-management implementations rather
than wrap them for compatibility.
