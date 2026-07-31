# dr-exec target use cases

This document is the scenario map for dr-exec. It identifies the consumers and
the relationships among the execution shapes without restating their rules.
Repository-wide vocabulary is authoritative in
[`../../.defs/terms.toml`](../../.defs/terms.toml), and accepted behavior is
authoritative in
[`../../.defs/contracts.toml`](../../.defs/contracts.toml).

## Scenario map

1. **Untrusted Python source, call-scoped.** Execute model-generated Python in a
   disposable runtime. This is the direct-evaluation path for generated source
   and the foundation for Python-backed batch drivers.

   Structured coverage: “Payload trust and containment are explicit,”
   “Inherited state is granted, not ambient,” “Untrusted Python runs in a
   declared hermetic runtime,” and “Call-scoped lifecycle completes before
   return” in the [standing contracts](../../.defs/contracts.toml).

2. **Untrusted command, call-scoped.** Execute argv-general untrusted work such
   as compiled model output or an agent CLI driven by model-authored input. It
   shares the call-scoped engine with untrusted Python but does not assume a
   Python runtime.

   Structured coverage: “Execution is local, process-level, and argv-only,”
   “Payload trust and containment are explicit,” “Call-scoped lifecycle
   completes before return,” and “Completed runs are data with one failure
   owner” in the [standing contracts](../../.defs/contracts.toml).

3. **Untrusted batch.** Evaluate a dimensioned experiment such as candidates ×
   tasks × cases while reusing children at caller-selected sharing boundaries.
   The consumer retains the experiment's dimensional meaning; dr-exec supplies
   execution, incremental accounting, and the child-side driver boundary.

   Structured coverage: “Containment cost follows a declared sharing
   boundary” and “A batch preserves dimensional identity and partial work” in
   the [standing contracts](../../.defs/contracts.toml).

4. **Trusted tool invocation.** Harden calls to known tools such as `git`,
   `uv`, linters, and build programs. This includes both individual calls and
   research workflows that aggregate many calls without discarding per-call
   evidence.

   Structured coverage: “Execution is local, process-level, and argv-only,”
   “Completed runs are data with one failure owner,” “Output delivery modes are
   explicit,” and “Every run is durably and faithfully observable” in the
   [standing contracts](../../.defs/contracts.toml).

5. **Containment profiles.** Apply real filesystem, network, process, and
   resource containment to untrusted work. The enforcement mechanism remains a
   design choice; the target is a mechanism-independent declaration that can
   replace the execution responsibility currently served by dr-docker.

   Structured coverage: “Containment profiles are mechanism-independent and
   fail closed” and “Containment cost follows a declared sharing boundary” in
   the [standing contracts](../../.defs/contracts.toml).

6. **Supervised long-lived processes.** Own children that outlive one call,
   including agent servers and stdio RPC processes. Supervision is a separate
   lifecycle surface rather than a mode of the call-scoped runners.

   Structured coverage: “Supervised children retain visible ownership and
   identity” and “Supervision separates interaction, drain, and stop” in the
   [standing contracts](../../.defs/contracts.toml).

7. **Supervised worker fleets.** Operate a resource-bound slot map of
   supervised workers as one unit. Experiment orchestration, job queues, and
   sweep submission consume this surface; they are not part of the executor.

   Structured coverage: “A fleet is one declared slot map” in the
   [standing contracts](../../.defs/contracts.toml).

## Design progression

The call-scoped scenarios establish the shared execution and result boundary.
Batching adds a warm-child protocol over that boundary. Containment adds a
stronger enforcement mechanism without changing the declared reach model.
Supervision introduces persistent ownership and interaction, and fleets then
compose supervised children through stable slots and collective operations.

## Residual design work

The standing contracts intentionally leave implementation choices open. The
remaining high-level design work is to select containment mechanisms and their
first useful profiles, define how supervision is persisted and commanded across
local processes, and define how resource discovery binds a fleet declaration to
machine-specific slots. Those choices must preserve the scenario boundaries
above and become structured contracts when they settle.
