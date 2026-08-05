# dr-store PR 2 — Document Directory component

PR: https://github.com/danielle-rothermel/dr-store/pull/2

Cross-repo: these belong to dr-store; any adopted item lands there and
reaches dr-exec through a released pin. Also open from review: the
declined findings' justifications live only in PR threads — if those
boundaries should survive as auditable rules, the dr-store vocabulary
sheet or a contract entry is the place.

Deferred review suggestions recorded on the PR, as standing todos:

- [ ] **Finalize/abort semantics for interrupted sidecar streams**: an aborted stream currently yields a summary indistinguishable from a complete one; guarded or idempotent finalize and exception-aware context-manager behavior would distinguish them.
- [ ] **Memory bound**: `tail_cap=None` with a finite `head_cap` buffers the entire stream remainder in memory.
- [ ] **Directory-entry durability for sidecar creation/finalization**: sidecar files are flushed but the containing directory entry is not fsynced on create/finalize (the manifest publish path does flush the directory).
- [ ] **`verify_sidecar` segment checking**: it checks the two expected segment lengths only as their sum, a weaker check than per-segment verification.
- [ ] **Prefix validation** admits names that produce awkward directory names.
- [ ] **Crash-suite child driver** has no watchdog on `readline()`.
- [ ] **Duplicate sidecar names**: opening the same sidecar name twice yields a summary whose digest does not describe the stored bytes.
- [ ] **Document Directory: `open_sidecar` does not serialize concurrent writers** on the same sidecar name -- two writers interleave (observed `b'BBAAAAAAAA'`) and both returned summaries' digests mismatch the stored bytes. Out of scope: the doc claims one writer by construction, not by locking. Revisit only if a consumer needs intra-directory write serialization.
- [ ] **docdir `verify_sidecar`**: negative `expected_head_length` / `expected_tail_length` pass when their sum matches the stored length (digest still pins the bytes, so no wrong bytes are admitted) -- consider rejecting negative segment lengths if `verify_sidecar` ever gains a caller-input-validation mandate.
- [ ] **`SidecarWriter.write`**: slice `remainder` to the tail buffer's free space before `extend` so peak allocation stays ~`tail_cap` instead of ~chunk size (stored bytes, produced/dropped, and digest already correct; efficiency-only, no documented claim at stake).
