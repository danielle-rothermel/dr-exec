# dr-exec v1 implementation plan

## Status

- **Starting point:** canonical v1 API scaffold.
- **Strategy:** fresh implementation; one canonical path per behavior.
- **Platform:** macOS first.
- **Primary acceptance workload:** high-volume HumanEval-style evaluation.
- **Bias:** smallest implementation satisfying accepted v1 contracts.
- **Deferred:** robustness, portability, and extension work without a current
  contract requirement.

## Implementation rules

- Build from the canonical source API; do not carry forward alternate public
  entry points, record options, batch models, or execution paths.
- Keep every public stub unchanged until its complete v1 behavior can replace
  it in one PR.
- Add tests with the first implemented behavior; every later PR owns tests for
  its contract.
- Use explicit state synchronization in concurrency/lifecycle tests; timeouts
  are watchdogs only.
- Keep domain concepts outside dr-exec:
  - no HumanEval-specific models;
  - no experiment dimensions;
  - no LLM workflow state;
  - no dr-platform lease state.
- Preserve useful failure scenarios as requirements; rederive code and tests
  against the canonical contracts instead of transplanting incompatible
  implementations.
- Maintain one open implementation stack.

## Prerequisite: dr-serialize

Implement and release only the capabilities specified in
[dr-serialize additions](dr-serialize-additions.md):

1. **Canonical byte access**
   - canonical strict-JSON UTF-8 bytes;
   - canonical identity-document UTF-8 bytes;
   - existing canonical text and hash results unchanged.
2. **Bounded strict JSON decode**
   - bytes-first;
   - strict UTF-8;
   - duplicate-key rejection;
   - non-finite-number rejection;
   - one complete value;
   - explicit byte and depth bounds;
   - bounded typed diagnostics.
3. **Nominal full SHA-256 value**
   - exactly 64 lowercase hexadecimal characters;
   - explicit parse failure;
   - compatible with existing full-hash APIs.
4. **Reusable conformance vectors**
   - canonical text;
   - canonical bytes;
   - identity bytes;
   - full hashes.

Non-goals:

- execution models;
- Pydantic base models/codecs;
- NDJSON scanning or protocol state;
- path or file operations;
- sidecar hashing;
- schema registries;
- bulk data formats.

Release the completed capabilities; then replace dr-exec's temporary local
dependency with the released pin before declaring v1 complete.

## Dr-exec implementation stack

### PR 1: serialization, identities, and runtime preparation

Implement:

- pin/local-source integration for the completed dr-serialize capabilities;
- canonical safe model projections;
- declaration, environment, executor, executor-config, and runtime identity
  construction;
- executor source snapshot;
- isolated-host interpreter probe;
- isolated-host runtime `prepare()` and `describe()`;
- nominal SHA-256 usage at every digest boundary.

Verify:

- exact canonical scalar spellings;
- identity schema/version/key goldens;
- clean, dirty, and unknown executor provenance;
- runtime probe under `-I`;
- secret-bearing values absent from projections;
- public API import and model validation.

### PR 2: directory run store

Implement:

- collision-free run directories;
- typed prepared/running/finalized handles;
- complete lifecycle manifests;
- canonical manifest bytes;
- stdout/stderr retained-output sidecars;
- streaming sidecar SHA-256;
- same-filesystem atomic manifest replacement;
- macOS flush behavior;
- complete and degraded receipts;
- strict load validation.

Verify:

- lifecycle transition validity;
- abrupt-death recovery after explicit committed-state events;
- sidecar length/digest checks;
- deterministic head/tail reconstruction;
- unwritable and failed-finalization degradation;
- concurrent collision-free writers;
- malformed/mismatched record rejection.

### PR 3: protected Python request and protocol

Implement:

- canonical identity-document request bytes;
- stdin-through-EOF request transport;
- protected fd 3 driver writer;
- canonical NDJSON frame encoding;
- LF frame acquisition;
- strict bounded decode and canonical byte equality;
- prelude/output/completion state machine;
- request identity binding;
- finite protocol self-budgets;
- accepted-output preservation on later failure.

Verify:

- zero, one, and multiple outputs;
- every ordering and completion-count failure;
- malformed UTF-8/JSON and duplicate keys;
- non-canonical bytes;
- missing LF and trailing bytes;
- identity mismatch;
- exact finite frame/aggregate/count/depth edges;
- unbudgeted policy without a hidden finite limit.

### PR 4: single-run macOS engine

Implement one private execution path for every target:

- declaration and platform validation;
- fresh scratch workspace;
- exact environment grant;
- direct argv resolution;
- macOS spawn/file actions;
- fresh session and process group;
- intended fd 0-3 mapping only;
- concurrent stdin, payload stdout/stderr, and protocol handling;
- deterministic structural head/tail retention;
- finite supported workload budgets;
- pinned attribution precedence;
- group-targeted teardown and direct-child reaping;
- scratch cleanup;
- mandatory run-store lifecycle;
- `ProcessExecutor.run()` cutover.

Verify:

- trusted command, untrusted command, and untrusted Python targets;
- inherited-state and descriptor isolation;
- input/output/wall-time budget edges;
- pipe backpressure and full-duplex progress;
- spawn absence versus other spawn failure;
- output/deadline/exit attribution races;
- process-group termination and re-session escape non-claim;
- cleanup and recording degradation;
- no returned result before required teardown/reap completes.

### PR 5: fake executor and conformance

Implement:

- thread-safe scripted response queue;
- immutable call capture;
- declaration validation parity;
- fake record receipt enforcement;
- `FakeExecutor.run()` and `calls`;
- shared executor conformance suite.

Verify:

- concurrent call isolation;
- deterministic response ordering;
- exhaustion behavior;
- mismatched job/attempt/receipt rejection;
- no scratch workspace, process, or real record creation;
- production and fake declaration-validation parity.

### PR 6: execution pool, finite batch, and streaming

Implement one scheduler core used by:

- `ExecutionPool`;
- `ProcessExecutor.run_many()`;
- `ProcessExecutor.open_pool()`;
- `ExecutionPool.run_stream()`.

Required behavior:

- automatic capacity resolved once from usable CPUs;
- fixed positive capacity;
- one active slot per job;
- bounded prefetch;
- bounded completion buffering;
- lazy finite input;
- capacity-driven streaming intake;
- completion-order delivery;
- ordinary per-job failures as completion data;
- scheduler-wide failure as pool failure;
- normal drain;
- abort with active process-group termination;
- closed-pool finality;
- caller context preserved in memory only;
- one native numeric-library thread per job.

Verify with deterministic gates:

- active count never exceeds capacity;
- intake never exceeds capacity plus prefetch;
- slow consumers apply backpressure;
- completions arrive in completion order;
- per-job failure does not fail the stream;
- drain accepts no new work and completes active work;
- abort terminates active work;
- no future, thread, or process per queued job;
- sync and async entry points share scheduler semantics.

### PR 7: throughput qualification

Add a reproducible, domain-shaped benchmark without domain types:

- one job compiles/loads one generated sample once;
- the child runs its cheap internal cases sequentially;
- independent jobs run concurrently;
- input is lazy and substantially larger than pool capacity.

Record:

- jobs per second;
- internal cases per second;
- active-job high-water mark;
- prefetched/queued high-water mark;
- child-startup share;
- peak memory;
- effective capacity and native-thread policy.

Acceptance:

- local evaluation exceeds the pinned representative upstream sample-production
  rate;
- no scheduler-created unbounded process, thread, queue, or result growth;
- every job produces one completion and one durable attempt record;
- no container, provisioned runtime, or process per internal test case.

## Delivery sequence

1. Publish dr-serialize prerequisite PR; obtain review.
2. Use that worktree as dr-exec's temporary local source.
3. Build the dr-exec PRs in order; keep reviews non-blocking while stacking.
4. Run adversarial review per PR and exact-tip validation at stack completion.
5. Release dr-serialize; replace local source with released pin.
6. Run full dr-exec qualification and throughput benchmark.
7. Release dr-exec.
8. Integrate consumers only through the released pin.

## Review checkpoints

- **After PR 1:** identity and serialization bytes are stable enough to persist.
- **After PR 2:** record durability and secret-safe evidence survive fault
  injection.
- **After PR 4:** one real run satisfies the full call-scoped lifecycle.
- **After PR 6:** finite and streaming scheduling remain bounded.
- **After PR 7:** the representative workload satisfies the product acceptance
  criterion.
