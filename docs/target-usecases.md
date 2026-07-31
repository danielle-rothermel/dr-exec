# dr-exec: target use cases

## Terminology

- **Payload** — the content a child process executes or interprets: source code, argv, stdin programs, prompts driving agent CLIs. Categorized by who authored that content, not by which binary runs it.
- **Trusted payload** — content we authored or pinned (first-party code, known tools with first-party arguments). Failures are bugs, not adversarial behavior.
- **Untrusted payload** — content from outside our control, above all model-generated: generated source, compiled artifacts of it, model-authored prompts or commands. Assumed arbitrary; runs only under an explicitly declared containment profile.
- **Inherited state** — what a child process receives automatically from its parent: environment variables (and any credentials in them), working directory, open file descriptors. Inheritance is never the default; it is always an explicit grant.
- **Environment passthrough** — an explicit grant of parent environment to the child, named-variable or overlay; the only route by which inherited environment reaches a child. An overlay grant may carry named exclusions, verified absent before spawn.
- **Containment** — the declared restrictions on what a payload can reach (filesystem, network, processes, resources): a spectrum from bare process boundary to full sandbox, always stated, never implied.
- **Containment profile** — the complete declared containment for a run: per-axis reach grants, resource backing, and the enforcement's known limits. A named, reusable object independent of the mechanism that enforces it.
- **Call-scoped** — the child's entire lifetime, spawn through reap, falls within a single executor call. Contrast: supervised long-lived processes, which outlive the call.
- **Supervised** — a child whose owner observes liveness and exit and is accountable for teardown; the opposite of fire-and-forget.
- **Liveness** — verified evidence that a process is still the child we spawned: identity, not a bare pid check.
- **Slot** — a supervised child's declared logical position (gpu 3, worker 1): stable across replacements while process identity changes.
- **Fleet** — a set of supervised children declared as one slot map and operated on by collective verbs (spawn, drain, stop); the supervised analogue of a batch.
- **Reap** — collect a dead child's exit status so it cannot linger as a zombie.
- **Hermetic** — no undeclared inputs: the runtime includes only what was declared. Distinct from containment (restricting what a payload can reach) and from POSIX session isolation (process-group lifecycle).
- **Budgeted** — a resource axis carries a budget when one is declared; every budget in force is visible, finite, and attributable to who declared it, and exceeding one is a distinguishable outcome, never silent. Per-axis names (deadline, output cap) refer to a single budget; the axes and default rules live in the Budgets section.
- **Captured** — output buffered by the executor and returned as part of the result at exit.
- **Streamed** — output delivered to the calling code incrementally as it is produced; still budgeted.
- **Spooled** — output written directly to a caller-designated file as it is produced: the disk-backed delivery mode that makes permissive output budgets cheap, and the natural mode for long-lived children.
- **Stdio passthrough** — child output forwarded live to the operator's own stdio rather than to calling code.
- **Artifact paths** — files a payload writes at declared locations are payload output, not an executor delivery mode; the run record notes where they landed.
- **Driver** — executor machinery that runs inside a child to conduct payload work: it receives work items, executes them, and emits results. Part of the executor for attribution purposes despite its address.
- **Executor** — our machinery around the payload, whichever side of the process boundary it runs on: spawning, lifecycle enforcement, drivers, and result interpretation. Executor failure (our machinery broke) is always distinguishable from payload failure (the payload misbehaved).
- **Run result** — the structured record of a completed run: exit status, delivered output, and measurements (duration, budget consumption). Exists whenever the child ran, however it exited; executor failure is precisely the case where no run result exists.
- **Run record** — the durable twin of the run result: persisted while the run is live, kept regardless of outcome, surviving the process that made it.
- **Attribution** — the determination of which party a failure belongs to: payload, executor (driver included), channel, budget, or machine. Every failure carries exactly one. A channel claim requires evidence; unknown defaults to executor, never to payload — the payload is never blamed by elimination.
- **Channel** — the mechanism carrying bytes and enforcement between executor and payload: pipes, container daemons, sandbox runtimes, wired-in services. Channel failures are the retriable class — neither payload misbehavior nor executor bug.
- **Exit policy** — the caller-declared mapping from exit status to success, failure, or domain data. The default policy is report-only.
- **Dimension** — one direction of a batch cross-product (candidates, tasks, cases): results are indexed by dimensions and failure scopes bind to them. Distinct from budget axes, which are resource kinds.
- **Narration** — the executor's own progress logging: lifecycle events emitted as they happen, on the executor's channel, never the payload's.

## Budgets

Defaults never guess the workload. The primary consumer is research code,
where strange shapes are the norm: hitting machine limits is expected
behavior, while hitting an interior limit invented "because" is a
library-abandonment event. So a default budget exists only to protect the
executor, is set at machine scale, and is scoped to the per-run aggregate —
never a task-scale guess, never a per-item proxy for the resource actually
being protected. A budget kill is never the artifact of an unnoticed
default. Preferring delivery modes that make permissiveness cheap (spooled
output rather than RAM-accumulated capture) is part of the principle, not
an optimization.

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
  machinery, not the workload. Escalation waits are sized so the child's
  final diagnostics land — a kill that erases the evidence of why it was
  needed is mis-sized.

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

## Amortization

The unit of containment and the unit of work are different units. A test
case is milliseconds; a container is seconds — pay containment per case and
the experiment slows by orders of magnitude, silently invalidating every
plan made from the per-item cost. Amortization is therefore a contract
obligation, not a performance nicety:

- Containment setup and teardown attach to a declared outer dimension
  boundary (a sandbox per experiment, a process per candidate, nothing per
  case) and amortize over everything inside it.
- Budgets are enforcement limits, not reservations — a limit costs nothing
  until violated; a reservation costs its full amount while unused, turning
  worst-case guesses into hard concurrency ceilings. Reserving happens only
  by explicit declaration.
- The safe path must be affordable — when rigorous execution costs orders
  of magnitude more than a bare `subprocess.run`, callers will take the
  unsafe path and be right to; keeping the paved road's overhead
  proportionate to the work is what keeps it used.

## Observability

Defaults never guess what you won't need to know — the information twin of
the budgets principle. A tight default budget manufactures spurious
failures; a quiet default log manufactures unanswerable questions, and the
information case is harsher: an over-tight budget costs a restart, an
unlogged fact is gone. For research code the run is the experiment — a lost
intermediate is lost data, not lost convenience.

- Progress is observable while it runs — the executor narrates its
  lifecycle (spawned with what and where, streaming, waiting on deadline,
  killing, reaping), so "is it working, is it stuck, where is my output
  landing?" is answerable from outside at any moment. Consistent narration
  is also the antipattern detector: aggregate-in-memory-write-at-the-end
  becomes visible during the run, not at the post-mortem.
- Every run leaves a durable record — what was invoked (argv, cwd,
  environment grants), budgets in force, timestamps, outcome and
  attribution, and where outputs including intermediates landed. The run
  result is the in-memory answer; the record is its persistent shadow —
  current while the run is live, not only complete at exit, and persisting
  regardless of outcome: success never deletes the record.
- Verbose by default, quiet by declaration — the default level assumes a
  new or debugging user, which is when defaults matter; flags reduce it.
  Silence is unrecoverable, noise is filterable, and disk at machine scale
  makes verbosity cheap.
- The record is faithful — the executor never edits, filters, or
  transforms what it observed. Sensitivity is domain knowledge: redaction
  belongs to the caller, after capture, where the knowledge lives; a
  partially-redacting executor would mutate collected data at capture time
  and teach callers to stop thinking about secrets.
- Narration and payload output are separate channels, never conflated —
  the executor's own logging must not corrupt captured or protocol output,
  the same invariant the batch protocol enforces from the child's side.
- Narration never fails the run — the record and narration are
  best-effort infrastructure: a logging failure is itself reported, but it
  never breaks the execution it observes.

## Use cases

All execution is machine-local. The executor's guarantees — no survivors,
group kill, reaping, liveness — end at the machine boundary; invoking
`ssh` is a trusted-tool call whose far side is the caller's domain, not a
remote-execution mode.

1. **Untrusted Python source, call-scoped** — run generated Python in a budgeted, disposable runtime.
   - Fresh state per run — nothing a prior run did can affect this one.
   - Concurrent runs never collide — each run works in its own scratch workspace by default; shared assets are read-only views, never shared mutable state.
   - Declared runtime — the interpreter and importable package set are explicit inputs, hermetic by default; nothing from the host leaks in by accident.
   - Inherited state by explicit grant — the child inherits nothing by default: environment, working directory, file descriptors; named-variable and overlay environment passthrough are first-class so intentional grants are easy and visible; overlay grants may carry named exclusions, verified absent before spawn.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
   - No wedging on the executor's pipes — input feeding and output draining are concurrent whenever both are live; a caller can never deadlock a run through the executor's own plumbing.
   - No survivors — when a run ends, by any path, the child's entire process tree is gone before the call returns.
2. **Untrusted command, call-scoped** — same, argv-general: compiled artifacts of generated code, headless agent CLIs.
   - Absence is a distinct outcome — a missing or unresolvable program is its own distinguishable outcome, not a generic start failure.
   - Inherited state by explicit grant — the child inherits nothing by default: environment, working directory, file descriptors; named-variable and overlay environment passthrough are first-class so intentional grants are easy and visible; overlay grants may carry named exclusions, verified absent before spawn.
   - Concurrent runs never collide — each run works in its own scratch workspace by default; shared assets are read-only views, never shared mutable state.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
   - Argv only — commands are argument vectors; nothing is ever interpreted by a shell.
   - No wedging on the executor's pipes — input feeding and output draining are concurrent whenever both are live; a caller can never deadlock a run through the executor's own plumbing.
   - No survivors — when a run ends, by any path, the child's entire process tree is gone before the call returns.
3. **Untrusted batch** — work structured as a cross-product of dimensions (candidates × tasks × cases), amortized through shared children at declared dimension boundaries, with results indexed by the dimensions.
   - The batch is a cross-product, not a list — its dimensions are first-class, and every result is indexed by them.
   - Sharing boundaries are declared — which dimension crossings share a warm child and which get fresh state is the batch's central design decision, made explicitly by the caller, never an implementation accident.
   - Failure scope follows dimensions — a payload failure fells its own sub-batch (one candidate's cases), never the whole batch; partial results are partial per dimension, and attribution names the level that failed.
   - The driver is the executor's agent inside the child — attribution splits within the child process: driver failure is executor failure, payload failure is payload failure, even when they die together.
   - Every item is accounted for — exactly one result per item; missing, duplicate, unknown, or shape-invalid results are executor-side protocol failures, detected at the boundary.
   - A result once produced is never lost — results leave the child incrementally, so a death at item N costs items N onward, not the batch.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
4. **Trusted tool invocation** — hardened calls to known programs (git, uv, linters, docker) with the lifecycle rigor ad-hoc call sites never have.
   - Stdio passthrough is first-class — streaming a trusted tool's output to the operator is a supported mode, not a reason to bypass the executor.
   - Outcomes are data, not control flow — a completed run yields a run result; exceptions are reserved for executor failure, never for a tool's exit status.
   - Absence is a distinct outcome — a missing or unresolvable program is its own distinguishable outcome, not a generic start failure.
   - Exit interpretation is caller policy — the executor reports raw exit status; what counts as failure is declared per call, never assumed.
   - Budget discipline applies — trusted tools hang and flood too; every call's budgets are declared or visibly absent, never unstated.
   - Argv only — commands are argument vectors; nothing is ever interpreted by a shell.
   - No survivors — when a run ends, by any path, the child's entire process tree is gone before the call returns.
5. **Sandboxes** — real containment (filesystem/network/resource) for untrusted execution; mechanism to be researched (not necessarily docker); replaces and decommissions dr-docker.
   - Containment is verified, not assumed — execution refuses rather than silently degrading when the promised containment is unavailable.
   - Reach is granted per axis — filesystem read and write separately, network, environment, process-spawning — deny by default, each grant an explicit list.
   - The declared profile is the contract — the same profile is executable by different isolation mechanisms; callers never write mechanism vocabulary.
   - Containment constrains what a payload may touch, never what it may look like — structural gating of payload code is not containment and is not this layer's job.
   - Every profile declares its limits — what it does not contain is part of the contract, not a docstring caveat.
   - Containment composes with any lifetime — call-scoped or supervised; a profile is a reach declaration, not a lifecycle, and the supervised behaviors apply unreduced inside it.
6. **Supervised long-lived processes** — children that outlive the call (agent servers, stdio RPC); a fully targeted use case with its own distinct set of concerns (liveness, restart, streaming I/O, reaping across calls), designed as its own contract — never a mode of the call-scoped runners.
   - Supervised, never orphaned — every child has an accountable owner; spawn-and-forget with nobody responsible for observing exit and reaping does not exist.
   - Exit is an observed event — child death is detected and surfaced promptly with attribution (crash vs clean exit vs killed), not discovered as a broken pipe on next use.
   - Liveness is verified identity, not a pid probe — "still running" means verified to be the child we spawned, so pid reuse cannot impersonate a dead child.
   - Shutdown is deliberate and complete — stop means the whole process tree terminated, escalated, and reaped within a budget, with the same rigor as call-scoped teardown.
   - Drain is distinct from stop — a supervisor can stop accepting work and let in-flight work finish; abandoning paid-for work is never the only shutdown.
   - External deadlines are anticipated — when the environment will kill the run at a known time, shutdown begins with enough headroom that teardown and the final record are ours, not the environment's.
   - Ownership survives the owner — supervision can be persisted and reattached; a child is never unkillable because its spawner exited.
   - Supervision is commandable from outside — stop and drain can be requested by something other than the spawning process.
   - Interactions are budgeted even when the child is not — each request or stream carries budgets; unbounded accumulation is an explicit grant, never a default.
   - Interaction cancellation is first-class — in-flight work can be cancelled through the child's protocol without killing the child; signals address the process, cancellation addresses the request.
   - Incremental observation — output is observable as it is produced, not only at exit; observation itself stays budgeted.
   - Deadlock-free bidirectional exchange — when stdin and stdout are both live, the executor owns the concurrency; a caller cannot wedge on the executor's own pipes.
   - No silent replacement — a restarted child is a new child occupying the same declared slot, visibly; slot identity is first-class, never reconstructed from names, and supervision never swaps the process behind a handle.
7. **Supervised worker fleets** — many supervised children as one first-class unit: a slot map over resources, collective lifecycle, aggregate observability. Fleet is to supervised what batch is to call-scoped; experiment orchestration (job queues, sweeps, submission) consumes this contract and is never part of it.
   - The fleet is a slot map, not a list of children — its shape is declared against discovered or declared resources (N workers per GPU, M per CPU); each child occupies a slot, and every supervised behavior applies to each unreduced.
   - Resource discovery is an input, not an assumption — the slot map binds to the resources actually present, so the same declaration is the same fleet on different hardware.
   - Collective verbs are atomic — spawn, drain, and stop apply to the fleet as single operations with per-slot attribution; the caller never loops over children.
   - Aggregate health is first-class — fleet-level state (slots alive, restarts per slot, work remaining) is queryable and durably recorded as one current view, a different question from any child's liveness.
   - Restart meets the work supply — per-slot restart policy is gated on remaining work, so a fleet winds down as work drains instead of respawning idle workers.
