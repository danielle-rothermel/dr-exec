# dr-exec PR 10 — single-run macOS engine

PR: https://github.com/danielle-rothermel/dr-exec/pull/10

Deferred review suggestions recorded on the PR, as standing todos:

- [ ] **Narration surface is absent** while the design and contracts require it (cleanup failure "narrated", executor narration "verbose by default"). `ExecutorSelfBudgets.narration_bytes` is a declarable axis reaching no enforcement point. Needs a decision: add narration to the frozen public API in a dedicated PR, or weaken the design and contract claims to match reality.
- [ ] **Scratch-cleanup failure is silently absorbed** (`shutil.rmtree(..., ignore_errors=True)`). Cleanup is attempted on every exit path and never replaces a trustworthy result; the docstring states the gap honestly. Wire into a narration channel if one lands.
- [ ] **The final reap in `_tear_down` is untimed**, so a child stuck in uninterruptible sleep would block inside a `finally` past every declared budget. Arguably covered by the contract's "unbudgeted termination and join axes provide no bounded return guarantee" — worth stating explicitly in the design so the limit is auditable rather than implicit.
- [ ] **`SpawnAbsentOutcome.executable` reports the declared `argv[0]`**, not the PATH-resolved path actually exec'd. They differ only when `which` succeeds but `execv` still returns ENOENT (missing shebang interpreter, TOCTOU deletion). No contract pins the spelling and the record drops the field, so no durable evidence is affected.
- [ ] **The request identity digest is computed twice** in `_target_of` for the Python target. Both call sites derive it from the same `prepared.request`, so no drift is possible; a local would deduplicate it.
