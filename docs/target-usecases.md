# dr-exec: target use cases

## Terminology

- **Payload** — the content a child process executes or interprets: source code, argv, stdin programs, prompts driving agent CLIs. Categorized by who authored that content, not by which binary runs it.
- **Trusted payload** — content we authored or pinned (first-party code, known tools with first-party arguments). Failures are bugs, not adversarial behavior.
- **Untrusted payload** — content from outside our control, above all model-generated: generated source, compiled artifacts of it, model-authored prompts or commands. Assumed arbitrary; runs only under an explicitly declared containment posture.
- **Inherited state** — what a child process receives automatically from its parent: environment variables (and any credentials in them), working directory, open file descriptors. Inheritance is never the default; it is always an explicit grant.
- **Environment passthrough** — an explicit grant of parent environment to the child, named-variable or overlay; the only route by which inherited environment reaches a child.
- **Containment** — the declared restrictions on what a payload can reach (filesystem, network, processes, resources): a posture ranging from bare process boundary to full sandbox, always stated, never implied.
- **Call-scoped** — the child's entire lifetime, spawn through reap, falls within a single executor call. Contrast: supervised long-lived processes, which outlive the call.
- **Supervised** — a child whose owner observes liveness and exit and is accountable for teardown; the opposite of fire-and-forget.
- **Liveness** — verified evidence that a process is still the child we spawned: identity, not a bare pid check.
- **Reap** — collect a dead child's exit status so it cannot linger as a zombie.
- **Hermetic** — no undeclared inputs: the runtime includes only what was declared. Distinct from containment (restricting what a payload can reach) and from POSIX session isolation (process-group lifecycle).
- **Budgeted** — a resource axis carries a budget when one is declared; every budget in force is visible, finite, and attributable to who declared it, and exceeding one is a distinguishable outcome, never silent. Per-axis names (deadline, output cap) refer to a single budget; the axes and default rules live in the Budgets section.
- **Captured** — output buffered by the executor and returned as part of the result at exit.
- **Streamed** — output delivered to the calling code incrementally as it is produced; still budgeted.
- **Stdio passthrough** — child output forwarded live to the operator's own stdio rather than to calling code.
- **Executor** — our machinery on the parent side of the process boundary: spawning, lifecycle enforcement, drivers, and result interpretation. Executor failure (our machinery broke) is always distinguishable from payload failure (the payload misbehaved).
- **Run result** — the structured record of a completed run: exit status, delivered output, and measurements (duration, budget consumption). Exists whenever the child ran, however it exited; executor failure is precisely the case where no run result exists.
- **Exit policy** — the caller-declared mapping from exit status to success, failure, or domain data. The default policy is report-only.

## Budgets

Defaults never guess the workload. The primary consumer is research code,
where strange shapes are the norm: hitting machine limits is expected
behavior, while hitting an interior limit invented "because" is a
library-abandonment event. So a default budget exists only to protect the
executor, is set at machine scale, and is scoped to the per-run aggregate —
never a task-scale guess, never a per-item proxy for the resource actually
being protected. A budget kill is never the artifact of an unnoticed
default. Preferring delivery modes that make permissiveness cheap (output
spilling to disk rather than accumulating in RAM) is part of the principle,
not an optimization.

Three kinds of budget:

- **Contract budgets** — caller-declared bounds derived from real
  downstream meaning (a max row size, a protocol field limit): exceeding
  one fails early and diagnosably instead of later and confusingly.
  Protocol budgets (per-item result size, traceback clipping) are contract
  budgets declared by the protocol.
- **Machine protection** — per-run aggregate bounds (RAM, disk) at actual
  machine scale. Their second role is diagnostic: a budget set at machine
  capacity converts a chaotic machine-limit death (an OOM kill arriving as
  an unattributable SIGKILL, ENOSPC surfacing wherever a write happened to
  land) into a clean, attributed budget failure — which is why "as
  permissive as possible" beats "non-existent" for this tier.
- **Executor self-budgets** — bounds on the executor's own operations
  (termination wait, startup deadline) so cleanup and supervision can never
  hang. The only tier with true built-in defaults, because they protect the
  machinery, not the workload.

Axes and their default rules:

- Wall-clock deadline — workload budget: declared or visibly absent, no
  default; overflow is always failure (time cannot truncate). Supervised
  children carry per-interaction deadlines instead of lifetime ones.
- CPU time — workload budget, containment-backed; distinct from wall-clock
  (catches spin-loops that a generous deadline misses); declared only.
- Output — any cap is contract-derived or machine-scale aggregate, never a
  small interior default; overflow policy is caller-declared, failure or
  visible truncation, never silent loss.
- Input — validated before spawn; an over-budget input is a caller error
  rejected without wasting the spawn, not a run failure.
- Memory — machine protection by default (per-run aggregate at machine
  scale); tighter only by declaration, containment-backed.
- Processes, file size, open files — containment-backed, declared only; no
  invented defaults.
- Termination wait, startup deadline — executor self-budgets with built-in
  defaults.
- Per-item result size — protocol budget, declared by the batch protocol.

## Use cases

1. **Untrusted Python source, call-scoped** — run generated Python in a budgeted, disposable runtime.
   - Fresh state per run — nothing a prior run did can affect this one.
   - Declared runtime — the interpreter and importable package set are explicit inputs, hermetic by default; nothing from the host leaks in by accident.
   - Environment by explicit grant — the child inherits nothing by default; named-variable and overlay environment passthrough are first-class so intentional grants are easy and visible.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
2. **Untrusted command, call-scoped** — same, argv-general: compiled artifacts of generated code, headless agent CLIs.
   - Absence is a distinct outcome — a missing or unresolvable program is its own distinguishable outcome, not a generic start failure.
   - Environment by explicit grant — the child inherits nothing by default; named-variable and overlay environment passthrough are first-class so intentional grants are easy and visible.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
3. **Untrusted batch** — many untrusted work items amortized through one budgeted child, with structured per-item results.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
4. **Trusted tool invocation** — hardened calls to known programs (git, uv, linters, docker) with the lifecycle rigor ad-hoc call sites never have.
   - Stdio passthrough is first-class — streaming a trusted tool's output to the operator is a supported mode, not a reason to bypass the executor.
   - Outcomes are data, not control flow — a completed run yields a run result; exceptions are reserved for executor failure, never for a tool's exit status.
   - Absence is a distinct outcome — a missing or unresolvable program is its own distinguishable outcome, not a generic start failure.
   - Exit interpretation is caller policy — the executor reports raw exit status; what counts as failure is declared per call, never assumed.
5. **Sandboxes** — real containment (filesystem/network/resource) for untrusted execution; mechanism to be researched (not necessarily docker); replaces and decommissions dr-docker.
   - Containment is verified, not assumed — execution refuses rather than silently degrading when the promised containment is unavailable.
6. **Supervised long-lived processes** — children that outlive the call (agent servers, stdio RPC); a fully targeted use case with its own distinct set of concerns (liveness, restart, streaming I/O, reaping across calls), designed as its own contract — never a mode of the call-scoped runners.
   - Supervised, never orphaned — every child has an accountable owner; spawn-and-forget with nobody responsible for observing exit and reaping does not exist.
   - Exit is an observed event — child death is detected and surfaced promptly with attribution (crash vs clean exit vs killed), not discovered as a broken pipe on next use.
   - Liveness is verified identity, not a pid probe — "still running" means verified to be the child we spawned, so pid reuse cannot impersonate a dead child.
   - Shutdown is deliberate and complete — stop means the whole process tree terminated, escalated, and reaped within a budget, with the same rigor as call-scoped teardown.
   - Ownership survives the owner — supervision can be persisted and reattached; a child is never unkillable because its spawner exited.
   - Interactions are budgeted even when the child is not — each request or stream carries budgets; unbounded accumulation is an explicit grant, never a default.
   - Incremental observation — output is observable as it is produced, not only at exit; observation itself stays budgeted.
   - Deadlock-free bidirectional exchange — when stdin and stdout are both live, the executor owns the concurrency; a caller cannot wedge on the executor's own pipes.
   - No silent replacement — a restarted child is a new child, visibly; supervision never swaps the process behind a handle.

For more info: subprocess usage audit results at /Users/daniellerothermel/drotherm/repos/dr-exec/docs/subprocess-usage-audit.md
