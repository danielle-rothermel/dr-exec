# dr-exec PR 9 — scaffold, planned contracts, and substrates

PR: https://github.com/danielle-rothermel/dr-exec/pull/9

References to "PR 2" inside entries mean the engine stage of the
implementation plan, which landed as
[PR 10](https://github.com/danielle-rothermel/dr-exec/pull/10).

Deferred review suggestions recorded on the PR, as standing todos:

- [ ] **Observability self-budgets are declared but unenforced** (`manifest_bytes`, `narration_bytes`, `recording_failure_count`, `failure_detail_bytes`). They participate in executor-config identity as persisted policy but no code consumes them; the manifest read is now bounded by a stated structural ceiling rather than by the declared axis. PR 2 should decide deliberately whether the engine enforces them before calling the store, or whether the design text weakens to "recorded policy, not enforced".
- [ ] **`mark_running` publication failure has no degraded-receipt path.** It returns `RunningRun` and cannot express degradation, so a publish fault propagates as an exception. Its public signature is frozen; satisfying the post-start degradation guarantee requires the PR 2 engine to catch it and finalize from the retained `PreparedRun` handle.
- [ ] **Degraded receipt can name a lifecycle state that is not valid on disk.** When `_load_record` itself fails, the receipt falls back to the handle's own state. Documented and tested, but marginally stronger than what is delivered; the truthful alternative makes `latest_state` optional, which is a public field-type change.
- [ ] **Identity role payloads validate in Python mode from the decoded value.** No semantic hole today (only `str`, `Literal`, and budget unions, and strict mode still rejects bool-as-int), but a future `bytes` or timestamp field on an identity payload would silently stop honoring the read rule, and nothing tests the distinction.
- [ ] **No tripwire pinning the child wrapper's canonical JSON against dr-serialize.** `_dr_exec_canonical` must reimplement the profile in the isolated host, and matches today; a corpus test asserting byte equality with `canonical_json_bytes` over non-ASCII, floats, `-0.0`, and large integers would make a future profile change loud at the child-observable wire boundary.
- [ ] **Byte-identity on record reads still depends on a dr-store API shape.** The store now reads and validates the stored bytes itself; the remaining durable improvement is a dr-store API returning verified bytes alongside the decoded payload, which is a dr-store change, released and re-pinned.
- [ ] **`SidecarSummary.produced`/`dropped` cross-check at finalization.** Asserting `produced == head_length + tail_length` and `dropped == 0` would enforce the "the writer never drops a byte here" claim, but adds a failure mode the design does not specify.
- [ ] **Driver-wrapper test helper writes the full request before draining fd 3.** Unreachable today (all drivers emit well under the pipe buffer, and the wrapper reads stdin to EOF first), but a future large-output driver case added there would hang rather than fail. Concurrent feed/drain duplicates the engine concurrency PR 2 owns.
- [ ] **`_executor_source_snapshot()` has no production caller yet.** The per-process cache is exercised only by tests; PR 2's record-header construction is the intended consumer. Worth a deliberate confirmation rather than letting an unreferenced production symbol persist.
- [ ] **Latent (not a current bug): strict Python-mode validation of decoded values would fail for `Path`/`UUID`-typed fields**, so a future refactor that validates a decoded value instead of the original bytes would break silently — currently prevented only by contract prose plus the JSON-mode call sites, with no mechanical guard.
- [ ] **Optional regression guard (not a defect): no test pins object-key ordering for non-ASCII keys through the child wrapper.** Behavior is correct and structurally guaranteed by both paths being the same `json.dumps` call, but a golden vector asserting `{"Z":1,"a":3,"é":2}` would cheaply document the key-vs-value ordering distinction that this bot misread.
