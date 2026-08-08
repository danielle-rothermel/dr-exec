# Backend-Neutral Run-Record Access

Status: selected prerequisite design; implementation has not started.

## Planning sources

Repository terminology and guarantees remain authoritative in
[`../../../.defs/terms.toml`](../../../.defs/terms.toml) and
[`../../../.defs/contracts.toml`](../../../.defs/contracts.toml). During
planning, use the proposed `run record reference` term in
[`plan-terms.toml`](plan-terms.toml) and the proposed backend-neutral access
contract in [`plan-contracts.toml`](plan-contracts.toml).

This plan must land before the
[`importable JSON process job`](plan.md) adapter freezes or releases its public
API.

## Goal

Replace public filesystem-shaped record locations with a serializable opaque
reference and store-owned access. Preserve the existing mutable lifecycle,
durability, degradation, and recording evidence while allowing a later store
layout without another receipt-schema cutover.

## Selected design

### Reference and lifecycle surface

- Add a closed serialized `RunRecordReference` boundary model. The initial
  directory reference contains a backend discriminator and one generated opaque
  record identifier; it contains no root, filesystem path, or encoded relative
  path.
- Treat the discriminator and locator keys as persisted-format literals with
  explicit contract markers and golden tests.
- Replace `record_dir` with the reference in `PreparedRun`, `RunningRun`,
  `CompleteRecordReceipt`, and `DegradedRecordReceipt`.
- `RunStore.load` accepts the reference. Callers neither inspect it nor combine
  it with an artifact name.
- Keep fake receipts free of production record claims.

### Reference resolution and mismatch

- A run store accepts only reference variants it owns and resolves their opaque
  identifiers within its configured storage boundary.
- An unsupported discriminator, malformed identifier, absent record, or
  reference that cannot be resolved by that store raises `RecordLoadError`
  without probing another backend or interpreting the locator as a path.
- The reference does not encode a store root identity. Passing a valid
  directory reference to a different directory root therefore fails as an
  unresolved record rather than receiving a separate mismatch taxonomy.

### Finalized artifact access

- Add one synchronous `RunStore.read_artifact` operation accepting a run-record
  reference, an artifact from its loaded finalized record, and a required
  finite maximum byte count.
- Reject a non-finalized record, an artifact not owned by that record, or an
  artifact whose declared size exceeds the caller's bound before reading its
  bytes. Verify the stored size and digest before returning complete bytes.
- V1 provides no artifact streaming, partial read, public path, or direct file
  handle. A caller may inspect the finalized artifact metadata before selecting
  its finite bound.
- `DirectoryRunStore` alone resolves the reference and stored relative artifact
  name. Relative names remain record metadata, not caller filesystem access.

### Cached receipts

- Extend the cache value schema to preserve the source record reference when
  the source completion has a real receipt.
- `CachedRecordReceipt` carries that source reference for production cache
  entries so callers can load the original evidence. Entries created from fake
  completions carry no source reference and make no production-record claim.
- Retention may make a previously valid source reference unresolved. That is a
  visible load failure under store/operator policy, not permission to embed a
  second copy of the run record in the cache receipt.

### Directory implementation and ownership

- Keep `DirectoryRunStore` as the only implementation in this change. It
  derives its internal directory path from the opaque record identifier and
  preserves prepared, running, finalized, sidecar, and degraded-receipt
  semantics.
- Retention, root partitioning, archival, and deletion remain run-store or
  operator policy. This change adds none of them.
- Do not add a packed backend or substitute a terminal `dr-store` artifact
  bundle. Measure the directory implementation after the hard cutover and plan
  another backend only if representative per-attempt evidence volume requires
  it.

## Hard cutover

- Remove every public `record_dir` field and every caller join of a receipt path
  with manifest or artifact names. Add no alias, compatibility property, path
  reference variant, or dual load method.
- Update engine, cache, tests, fixtures, README examples, public exports,
  terminology, contracts, and persisted golden vectors in the same change.
- Preserve directory-path inspection only inside `DirectoryRunStore` tests that
  directly verify its private storage implementation; capability conformance
  tests use references and store operations only.

## Implementation sequence

1. Finalize the reference, bounded artifact-read, receipt, and cache-value
   serialized shapes and pin their persisted literals.
2. Change the `RunStore` protocol and internal lifecycle handles to references.
3. Hard-cut `DirectoryRunStore` to generate and resolve its reference and to
   provide bounded verified artifact reads.
4. Change real, degraded, cached, and fake receipt handling without weakening
   their existing evidence claims.
5. Remove path-based access from engine callers, tests, examples, and public
   documentation.
6. Run focused model, store, engine, cache, pool, conformance, golden-vector,
   documentation, formatting, lint, type, build, and full-suite checks.

## Validation bar

- Golden tests pin the reference discriminator, locator key, receipt keys, and
  changed cache-value schema.
- Model tests reject paths, malformed locators, unknown variants, and receipt or
  execution identity mismatches.
- Run-store conformance tests prove that a non-path reference is sufficient to
  prepare, mark running, finalize, load, and recover stdout and stderr.
- Artifact tests cover finite-bound preflight, complete verified reads,
  non-finalized records, foreign artifacts, corruption, truncation, and missing
  data without returning partial bytes.
- Mismatch tests cover unknown kinds and valid directory references used with
  the wrong root, with no path fallback.
- Cache tests cover production source references, fake entries without source
  references, schema mismatch, source retention, and unresolved source records.
- Crash and degradation tests preserve the latest valid durable state and keep
  recording health distinct from execution outcome.
- A repository-wide search finds no public `record_dir`, receipt-path join, or
  `RunStore.load(Path)` usage.
