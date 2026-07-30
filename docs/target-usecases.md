# dr-exec: target use cases

1. **Untrusted Python source, one-shot** — run generated Python in a bounded, disposable interpreter.
2. **Untrusted command, one-shot** — same, argv-general: compiled artifacts of generated code, headless agent CLIs.
3. **Untrusted batch** — many untrusted work items amortized through one bounded child, with structured per-item results.
4. **Trusted tool invocation** — hardened calls to known programs (git, uv, linters, docker) with the lifecycle rigor ad-hoc call sites never have.
5. **Sandboxes** — real isolation (filesystem/network/resource) for untrusted execution; mechanism to be researched (not necessarily docker); replaces and decommissions dr-docker.
6. **Supervised long-lived processes** — children that outlive the call (agent servers, stdio RPC); designed later with its own contract, never a mode of the one-shot runners.

For more info: subprocess usage audit results at /Users/daniellerothermel/drotherm/repos/dr-exec/docs/subprocess-usage-audit.md
