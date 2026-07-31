# dr-exec: target use cases

## Terminology

- **Payload** — the content a child process executes or interprets: source code, argv, stdin programs, prompts driving agent CLIs. Categorized by who authored that content, not by which binary runs it.
- **Trusted payload** — content we authored or pinned (first-party code, known tools with first-party arguments). Failures are bugs, not adversarial behavior.
- **Untrusted payload** — content from outside our control, above all model-generated: generated source, compiled artifacts of it, model-authored prompts or commands. Assumed arbitrary; runs only under an explicitly declared containment posture.
- **Inherited state** — what a child process receives automatically from its parent: environment variables (and any credentials in them), working directory, open file descriptors. Inheritance is never the default; it is always an explicit grant.
- **Containment** — the declared restrictions on what a payload can reach (filesystem, network, processes, resources): a posture ranging from bare process boundary to full sandbox, always stated, never implied.
- **Call-scoped** — the child's entire lifetime, spawn through reap, falls within a single executor call. Contrast: supervised long-lived processes, which outlive the call.
- **Hermetic** — no undeclared inputs: the runtime includes only what was declared. Distinct from containment (restricting what a payload can reach) and from POSIX session isolation (process-group lifecycle).
- **Budgeted** — every resource axis — wall-clock, output, input — carries an explicit finite budget; exceeding one is a distinguishable failure, never silent. Per-axis names (deadline, output cap) refer to a single budget.
- **Executor** — our machinery on the parent side of the process boundary: spawning, lifecycle enforcement, drivers, and result interpretation. Executor failure (our machinery broke) is always distinguishable from payload failure (the payload misbehaved).

## Use cases

1. **Untrusted Python source, call-scoped** — run generated Python in a budgeted, disposable runtime.
   - Fresh state per run — nothing a prior run did can affect this one.
   - Declared runtime — the interpreter and importable package set are explicit inputs, hermetic by default; nothing from the host leaks in by accident.
   - Environment by explicit grant — the child inherits nothing by default; named-variable and overlay passthrough are first-class so intentional grants are easy and visible.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
2. **Untrusted command, call-scoped** — same, argv-general: compiled artifacts of generated code, headless agent CLIs.
   - Environment by explicit grant — the child inherits nothing by default; named-variable and overlay passthrough are first-class so intentional grants are easy and visible.
   - Failure attribution — crash, kill, timeout, output overflow, and ordinary nonzero exit are distinguishable, and payload failure is distinguishable from executor failure.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
3. **Untrusted batch** — many untrusted work items amortized through one budgeted child, with structured per-item results.
   - Trusted vs untrusted payload categorization is declared, not inferred — running an untrusted payload requires an explicit call-site acknowledgment of its containment; accidental invocation fails loudly.
4. **Trusted tool invocation** — hardened calls to known programs (git, uv, linters, docker) with the lifecycle rigor ad-hoc call sites never have.
5. **Sandboxes** — real containment (filesystem/network/resource) for untrusted execution; mechanism to be researched (not necessarily docker); replaces and decommissions dr-docker.
   - Containment is verified, not assumed — execution refuses rather than silently degrading when the promised containment is unavailable.
6. **Supervised long-lived processes** — children that outlive the call (agent servers, stdio RPC); a fully targeted use case with its own distinct set of concerns (liveness, restart, streaming I/O, reaping across calls), designed as its own contract — never a mode of the call-scoped runners.

For more info: subprocess usage audit results at /Users/daniellerothermel/drotherm/repos/dr-exec/docs/subprocess-usage-audit.md
