# V1 plan review discussion topics

- [x] V1 provides isolated host Python through a resolved host interpreter and
      pinned `-I -c` invocation semantics. It does not claim a provisioned
      package closure or general hermetic execution. The next runtime boundary
      is specified in the
      [verified Python runtime plan](../future-plans/verified-python-runtime.md).
      The structured contract proposals must use the isolated-host-Python name
      and make no claim that v1 verifies a declared interpreter or package set.
- [x] Default every v1 workload budget axis to explicitly unbudgeted through
      `Budgets.unbudgeted()`; do not invent finite limits without caller meaning.
- [x] When marked truncation is selected, retain a deterministic head and tail
      for each payload stream and record the dropped-byte counts. Protocol
      messages are never truncated: an oversized or malformed message is an
      executor protocol failure. The parent preserves previously accepted
      complete protocol outputs and reports an incomplete stream without
      synthesizing domain item results. Protocol and payload channels have
      separate accounting.
- [x] Real executor runs always record and cannot opt out. Tests that exercise
      the real executor write through temporary output buffers or stores whose
      lifetime the test owns; fake calls do not create run records.
- [x] **Machine utilization:** Make one caller-defined `ExecutionJob` the
      sharing and scheduling unit.
      `ExecutionPool` runs many fresh-child jobs with bounded host-level
      concurrency, bounded prefetch, and completion-order delivery;
      `run_many()` consumes finite lazy batches and `run_stream()` pulls from a
      durable source only as capacity becomes available. HumanEval packages one
      generated sample and its complete sequential test suite into each job so
      compilation is paid once while samples run concurrently. See the
      [accepted public API and topology](v1-design.md#public-type-and-protocol-design).
- [ ] Keep JSON or NDJSON for executor control and incremental protocol data and
      use domain formats for bulk artifacts, but build on `../dr-serialize`
      wherever possible. Identify any missing serialization features that belong
      in dr-serialize rather than reimplementing them in dr-exec.
- [x] Filesystem and network sandboxing are out of scope for v1. The primary v1
      environment is macOS, where the proposed Linux containment mechanisms do
      not provide a suitable inexpensive portable path.
- [x] Use `ExecutionTarget` for the closed execution-kind declaration; apply
      the agreed `Id`, `Env`, and `Config` short names; use `StrEnum` members
      inside Pydantic discriminator `Literal`s; and define `Executor`, `Runtime`,
      and `RunStore` as stable behavioral Protocols with qualified concrete
      implementations and shared conformance suites. `ExecutionPool` remains
      the one concrete v1 scheduling policy. The complete type graph is in the
      [v1 public type and protocol design](v1-design.md#public-type-and-protocol-design).

## Contract contradiction notes

- [x] **Contradiction 1 — budgets come from declared meaning:** Loosen the core
      RAM-protection claim. V1 defaults RAM to explicitly unbudgeted and does not
      promise aggregate RAM protection it cannot enforce faithfully.
- [x] **Contradiction 2 — call-scoped lifecycle:** Loosen the universal
      process-tree teardown claim. V1 states the exact process-group guarantee
      it can provide on its supported macOS path and does not claim that a
      re-sessioned descendant is terminated.
- [x] **Contradiction 3 — durable observability:** Every real run writes through
      a `DirectoryRunStore` with atomically published `prepared`, `running`, and
      `finalized` manifests plus retained-output sidecars. On supported local
      macOS filesystems, every published state is valid and crash-consistent;
      abrupt death leaves a visibly incomplete record. `RecordReceipt` reports
      recording degradation separately from the execution outcome. Real-engine
      tests use the same store in temporary directories; fake calls do not
      record. See
      [the v1 durable-observability design](v1-design.md#durable-observability-and-record-layout).
