# v1 completion and next steps

## What just finished

- **dr-store 0.1.1** — the Document Directory component (`dr_store.docdir`:
  collision-free allocation, atomic durable Manifest publish, streamed
  truncating digest-finalized Sidecars, verified reads), built, reviewed,
  merged, and released; dr-exec's `DirectoryRunStore` composes the pin.
- **dr-exec v1** — the three-PR stack, merged in order:
  [#9](https://github.com/danielle-rothermel/dr-exec/pull/9) (canonical
  API scaffold, consolidated planned contracts, and the three substrates:
  identities/serialization, run store, protected protocol),
  [#10](https://github.com/danielle-rothermel/dr-exec/pull/10) (single-run
  macOS engine and the `ProcessExecutor.run()` cutover),
  [#11](https://github.com/danielle-rothermel/dr-exec/pull/11)
  (FakeExecutor, the shared conformance suite, and the execution pool).
  No `NotImplementedError` remains.
- Both pool-semantics decisions are implemented: a broken pool drains
  already-buffered completions before raising, and `ExecutionPool.state`
  is ratified public surface (an observational snapshot, never a
  synchronization primitive).
- **dr-exec 0.1.1 is released on PyPI** via the tag-triggered trusted
  publisher, and the terms and contracts sheet is live at
  [danielle-rothermel.github.io/dr-exec](https://danielle-rothermel.github.io/dr-exec/).
- macOS-specific engine behavior is local-qualification-only by decision:
  darwin-marked tests skip in ubuntu CI, and their local passes are the
  qualification evidence recorded on the PRs.

## Next steps

Only what has been discussed; nothing new.

1. **Activate the planned contract set.** `docs/v1-plan/contracts.toml`
   was gated on the complete v1 passing repository qualification at the
   pre-release tip; that gate has been met, so the planned set moves into
   the active `.defs/contracts.toml`.
2. **dr-code cutover.** Rebuild dr-code's execution path on the released
   `dr-exec==0.1.1` pin, per the standing authority principle: the
   dr-exec contract is the deliberate design, dr-code's pinned behaviors
   are prior art re-adjudicated against it.
3. **Triage the deferred review suggestions.** Each PR's deferred ledger
   is recorded in this directory, one file per PR, as standing todos:
   - [pr9-substrates.md](pr9-substrates.md)
   - [pr10-engine.md](pr10-engine.md)
   - [pr11-executor-surface.md](pr11-executor-surface.md)
   - [dr-store-pr2-document-directory.md](dr-store-pr2-document-directory.md)
     (cross-repo; adopted items land in dr-store and arrive by pin)
4. **Danielle's validation pass.** The per-PR review checklists on the
   merged PRs remain her non-blocking to-check list before standing
   behind results.
