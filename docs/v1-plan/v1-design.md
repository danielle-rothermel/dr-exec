# dr-exec v1 design

## Authority

- **Primary platform:** macOS.
- **Primary workload:** high-volume HumanEval-style local evaluation.
- **API authority:** pinned releases of the
  [public package surface](../../src/dr_exec/__init__.py) and
  [stable capability boundaries](../../src/dr_exec/capabilities/protocols.py).
- **Planned behavioral authority:** [v1 contracts](contracts.toml), activated
  only after the complete implementation passes repository qualification at the
  pre-release tip.
- **Implementation decomposition:**
  [implementation plan](implementation-plan.md).
- **Dependency plan:** [dr-serialize additions](dr-serialize-additions.md).
- **Future runtime:**
  [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md).

Exact public imports, names, constructors, fields, defaults, unions, exceptions,
and signatures belong in source. This document specifies behavior, wire formats,
persistence, safety boundaries, ownership, and qualification.

### Repository contracts

The [standing repository contracts](../../.defs/contracts.toml) remain active
until the [planned v1 contracts](contracts.toml) qualify and replace them. Core
and v1-specific vocabulary remain in [core terminology](../../.defs/terms.toml)
and [v1 terminology](terms.toml), respectively.

Source changes must preserve accepted boundaries and qualification obligations.

## Scope and consumers

V1 provides machine-local, call-scoped execution and bounded job scheduling for
the initial HumanEval-style workload, plus the captured-output subset of trusted
tool execution. It uses direct argv invocation, explicit inherited state,
captured output, process-group lifecycle management, and durable local records.
See [target use cases](../high-level-plan/target-usecases.md).

Consumers that require a deferred surface retain their existing execution path:

| Consumer | Missing surface |
| --- | --- |
| Build hooks; packaging tests | Arbitrary cwd; passthrough output. |
| Repository provenance capture | Arbitrary cwd. |
| Long-lived development servers | Supervision; readiness. |
| Interactive multi-process harnesses | Mid-run polling; rendezvous. |

## Execution boundaries

### Isolated host Python

- Resolve and validate one concrete host interpreter as an absolute path.
- Probe it once at runtime construction and retain the resulting runtime record.
- Invoke `<interpreter> -I -c <library-wrapper-source>`.
- Use no Python-specific stdio framing or undeclared environment injection.

The [planned runtime contract](contracts.toml) defines the deliberately limited
public claim. The implementation path to a verified runtime remains
[outside v1](../future-plans/verified-python-runtime.md).

### Containment and lifecycle

- The bootstrap creates the fresh session and process group before payload
  execution.
- Every post-spawn exit path, including a completed-result return and a raise
  caused by machinery failure, performs configured teardown of the original
  process group and reaps the direct child before the executor call exits.
- Descendants that create a new session may escape the original process group
  and this teardown claim.
- Finite termination and join limits control escalation and return deadlines.
- **Finite join exhaustion:** after group teardown, if inherited pipes still do
  not reach EOF, close the parent ends and raise `ExecutorFailure`; output and
  measurements are not trustworthy enough to manufacture a result, and the
  latest durable lifecycle record remains incomplete/degraded.
- Scratch cleanup runs on every exit path; cleanup failure is narrated and does
  not replace an otherwise trustworthy result.

## Serialization dependencies

- Pin released `dr-serialize` (`==0.1.1`); do not copy or fork its
  canonicalization or identity behavior. Any logically-unordered
  collection field serializes through its `canonical_sorted_values`,
  never local sorting.
- Pin Pydantic; its JSON-mode conversion contributes to bytes later validated
  and canonicalized by dr-exec.
- Pin released `dr-store` (`>=0.1.1`) for the Document Directory; do not
  reimplement its allocation, atomic publish, or sidecar mechanics.
- Required shared additions and qualification:
  [dr-serialize additions](dr-serialize-additions.md).

## Validated serialization paths

### Write

```text
dr-exec or domain boundary model
  -> explicit secret-safe Pydantic JSON-mode projection
  -> dr-serialize strict JSON validation
  -> dr-serialize canonical JSON bytes
  -> protected protocol write or DirectoryRunStore transaction
```

### Read

```text
boundary-limited bytes acquired by dr-exec
  -> dr-serialize bounded strict JSON decode
  -> dr-serialize canonical re-encode and byte-for-byte equality check
  -> dr-exec strict Pydantic JSON-mode validation of the same original bytes
  -> dr-exec protocol, identity, and lifecycle validation
```

### Validation ownership

- Dr-exec applies each boundary's acquisition limit before decode. Manifest
  loading uses a static size preflight followed by a whole-file read; concurrent
  growth or replacement can exceed that preflight and is outside its memory-bound
  claim.
- Shared decoder rejects:
  - invalid UTF-8;
  - duplicate keys;
  - non-finite numbers;
  - malformed or trailing data;
  - depth overflow.
- Dr-exec canonicalizes the decoded `Jsonable` only to verify byte-for-byte
  equality with the original input. It passes those same verified original
  bytes to `model_validate_json(..., strict=True)` or
  `TypeAdapter.validate_json(..., strict=True)`; it never passes the decoded
  `Jsonable` to Pydantic Python-mode validation.
- Dr-exec translates shared failures into its closed protocol or record-load
  taxonomy.
- Dr-exec additionally validates:
  - frame grammar;
  - message order;
  - aggregate limits;
  - identities;
  - lifecycle meaning.
- Persisted keys/discriminants: explicit literals; never enum iteration or
  implementation-field reflection.

### Boundary application

- **Python request:** one canonical identity document on stdin; then EOF.
- **Protocol frame:** one canonical closed JSON object plus LF on the protected
  descriptor.
- **Request/result document:** identity document only when schema, version, and
  payload define the boundary.
- **Record manifest:** closed, versioned dr-exec model; canonical-byte path;
  lifecycle validation after strict JSON-mode model validation.
- **Payload stdout/stderr:** raw bytes; streamed to Document Directory
  Sidecars; exact lengths and digests from the finalized summary; no JSON
  normalization.
- **Bulk domain formats:** outside generic JSON lane; adapter records media
  type, size, digest, and schema identity as required.

## Scalar wire spellings

| Scalar | Required spelling |
| --- | --- |
| UUID | Lowercase hexadecimal; 36 characters; hyphenated `8-4-4-4-12`. |
| Path | POSIX string; executable absolute; artifact reference normalized relative; no empty, `.`, or `..` component; serialization never resolves or follows symlinks. |
| Timestamp | UTC RFC 3339; trailing `Z`; exactly six fractional digits; reject naive/non-UTC values. |
| Duration | Integer nanoseconds; `_ns` field suffix; no ISO 8601 string or floating-point seconds. |
| JSON bytes | Padded RFC 4648 URL-safe base64. |
| Transport bytes | Command stdin, payload output, and sidecars retain their separate raw-byte contracts. |
| Unicode | Preserve code-point sequence; no normalization; canonical JSON owns escaping and ASCII-only representation. |
| Integer | JSON integer syntax; no plus, leading zero, exponent, or fraction; booleans rejected as integers. |
| Enum | Exact pinned string value. |
| SHA-256 | Exactly 64 lowercase hexadecimal characters; no prefix; no abbreviated boundary value. |

Golden vectors cover each scalar alone and nested in every relevant request,
frame, identity, and record model under exact pinned dependency releases.

## Capability mechanics

### Executor

- Production execution follows one sequence:
  1. validate declaration;
  2. prepare durable state;
  3. create scratch workspace;
  4. launch one fresh child;
  5. exchange protocol messages;
  6. capture payload output;
  7. enforce budgets;
  8. tear down and reap;
  9. finalize record;
  10. return result.
- A cancellation observed before spawn records `CancelledOutcome` and finalizes
  without launching a child; one observed after spawn enters the same teardown
  step.

### Fake executor

- `FakeExecutor` validates each declaration and returns one consumer-scripted
  `CompletedExecution` from its queue or responder.
- It synchronizes call capture and queue selection and passes the call's
  cancellation token to the responder.
- The consumer script owns completion job binding, attempt identity, and
  cancellation outcome. Production allocation, lifecycle, and recording
  semantics belong to `ProcessExecutor`.

### Outcomes and exceptions

- The engine constructs outcome data for recognized spawn, child, budget,
  cancellation, and protocol states.
- Invalid pre-spawn declarations and parent-observed machinery failures that
  prevent a trustworthy result raise typed exceptions.
- Exception translation preserves underlying cause.
- Record prepare failure: prevent spawn; raise.
- Recording degradation after attempt start: receipt data; does not replace
  execution outcome.
- Later protocol failure: preserve previously accepted complete outputs.
- HumanEval normal result: one aggregate domain output containing per-test
  outcomes.

## Runtime and transport contracts

### Runtime responsibility

- Resolve and validate absolute executable at construction.
- Run one fixed construction-time `-I` probe and retain its runtime record.
- Runtime identity includes the resolved absolute executable path, so equal
  interpreter builds at different paths compare as distinct runtimes; this
  false-split bias and its remedies are documented in
  [isolated-host runtime identity portability](../future-plans/isolated-host-runtime-identity-portability.md).
- Prepare fixed `<executable> -I -c <library-wrapper-source>` command. The
  wrapper source is the OS-level `-c` argv element and contains the declared
  consumer `driver_source` as inert data within the wrapper representation. The
  payload sees CPython's own `sys.argv` (`["-c"]`), with no domain source
  argument. The wrapper opens the protocol handle, decodes the request, resolves
  `dr_exec_main`, and intentionally evaluates the embedded driver; no shell
  interprets either source string.
- Do not:
  - spawn per preparation or payload invocation;
  - choose budgets;
  - resolve environment grants;
  - write records.
- Future verified runtime may implement the same capability; conformance does
  not add it to the v1 support matrix.

### Child descriptors

| Target | fd 0 | fd 1 | fd 2 | fd 3 |
| --- | --- | --- | --- | --- |
| Untrusted Python | Request stdin | Payload stdout | Payload stderr | Protected protocol write pipe |
| Command | Stdin | Payload stdout | Payload stderr | Not inherited |

- Callers cannot grant arbitrary descriptors.
- fd 3 is never part of an environment grant.
- macOS engine:
  - create close-on-exec pipes;
  - start one library-owned Python bootstrap with `close_fds=True` and only the
    intended pipe ends plus a close-on-exec setup-status pipe;
  - never use a shell or caller-controlled command composition;
  - in the fresh bootstrap process: create the session, change to the scratch
    directory, duplicate intended child ends to fds 0–3, close originals, and
    `exec` the declared command directly;
  - report bootstrap/setup failure through the private status pipe; successful
    payload `exec` closes that pipe and is observed as EOF;
  - never mutate parent-global descriptor numbers;
  - never use a pre-exec callback;
  - never run caller or package Python callbacks between a possible fork and
    the first bootstrap `exec`.

This deliberately favors a small Python bootstrap over a native macOS spawn
extension. It adds one fixed helper interpreter startup per job.

### Python request

- Content: complete canonical identity-document bytes.
- Framing: parent writes stdin, closes it, then child reads through EOF; no BOM,
  length prefix, delimiter, or trailing LF.
- Before spawn: compare canonical length with workload input budget.
- Measurement: canonical length is recorded input bytes.
- Driver: read through EOF; bounded strict decode and canonical byte
  verification; strict Pydantic JSON-mode identity validation of the same
  original bytes.
- Invalid request: no protocol output.

### Driver protocol handle

- Library bootstrap opens fd 3 before domain code.
- Retain handle even if domain code replaces language-level stdout/stderr.
- Consumer `driver_source` defines exactly
  `dr_exec_main(request, emit)`. The library-owned wrapper decodes and validates
  the request, evaluates the source, resolves that function, and supplies an
  emitter that validates and writes complete canonical output frames.
- Missing/non-callable entrypoint, source-load failure, or callback failure
  stops the child without a completion frame. The parent observes an incomplete
  protocol stream and preserves accepted outputs.
- When a child-side protected-writer failure leaves the protocol incomplete, v1
  has no separate child-to-parent signal that distinguishes it from another
  incomplete stream. Absent higher-precedence evidence, the parent can classify
  only `INCOMPLETE_STREAM`, with the existing payload attribution; it does not
  infer the child-internal cause.
- A parent-observed transport-worker failure is executor machinery failure and
  raises after lifecycle cleanup instead of becoming a protocol outcome.
- Honest limitation: payload can discover, close, or write inherited
  descriptors directly.
- Malformed protected bytes: executor protocol failure; not in-process tamper
  resistance.

## Budget and retention contracts

### Payload-output retention

- Streams: stdout and stderr; separate raw bytes.
- No in-band framing, decoding, or newline normalization.
- Artifact paths: recorded separately; not an output-delivery mode.
- Executor narration: parent-owned channel; never payload stdout/stderr.
- Unbudgeted output: retain every produced byte.
- Finite aggregate budget allocation:
  - stdout head;
  - stdout tail;
  - stderr head;
  - stderr tail.
- Allocation: declaration-pinned; independent of drain scheduling.
- Production within limit: retain every byte.
- Fail-on-overflow: same aggregate total is termination threshold.
- Marked truncation: continue drain/count through EOF; retain no more than
  declared total.
- Returned per stream:
  - head;
  - tail;
  - produced bytes;
  - dropped bytes.
- Head/tail remain separate; executor inserts no marker and never represents
  them as contiguous output.
- Measurements count bytes produced after retention limit.

### Other finite limits

- Protocol volume/structure overflow: protocol failure; preserve earlier
  accepted outputs.
- Manifest/narration/record-detail exhaustion: observability degradation; do
  not replace execution outcome.
- Startup/termination/join limits: watchdog/escalation deadlines.
- Unbudgeted time axis: no bounded-return guarantee.

## Protected protocol

### Wire

- Descriptor: fd 3.
- Format: strict canonical NDJSON.
- Frame bytes: canonical UTF-8 encoding of exactly one closed model, then one
  LF.
- Boundary property: canonical JSON contains no raw line breaks; LF is
  unambiguous.
- Separate accounting from payload stdout/stderr.
- Forbidden:
  - BOM;
  - blank line;
  - CRLF;
  - leading/trailing whitespace;
  - missing terminal LF;
  - bytes after completion.

| Frame | Required fields | Meaning |
| --- | --- | --- |
| Prelude | `version: 1`; `kind: "prelude"`; full `request_id_sha256` | Open stream; bind canonical request identity. |
| Output | `version: 1`; `kind: "output"`; nonnegative `sequence`; identity `document` | Carry one validated domain output at its zero-based position. |
| Complete | `version: 1`; `kind: "complete"`; nonnegative `output_count` | Terminate stream; declare output-frame count. |

### State machine

1. Exactly one prelude; first; request digest matches.
2. Zero or more outputs; consecutive zero-based sequence.
3. Exactly one completion; last; count equals accepted outputs.
4. EOF only after completion frame and LF.

Duplicate, skipped, reordered, post-completion, or incomplete frames fail the
protocol.

### Failure taxonomy

| Condition | Failure |
| --- | --- |
| Invalid UTF-8/JSON; duplicate keys; non-canonical bytes; closed-model failure | `MALFORMED_FRAME` |
| Prelude/frame in wrong position | `UNEXPECTED_FRAME` |
| Request digest mismatch | `ID_MISMATCH` |
| Repeated sequence | `DUPLICATE_OUTPUT` |
| EOF; missing terminal LF; completion-count mismatch | `INCOMPLETE_STREAM` |
| Configured finite protocol limit exceeded | `OVERSIZED_FRAME` |

### Acquisition and validation

- Finite frame budget: scan for LF without acquiring beyond limit.
- Unbudgeted frame budget: no executor cap; machine-resource exhaustion remains
  possible.
- After acquiring a finite frame:
  1. bound decoder by actual byte length and maximum structural depth;
  2. strict decode;
  3. canonical re-encode;
  4. require byte-for-byte equality;
  5. validate the closed Pydantic frame model from the same original frame
     bytes in strict JSON mode.
- Never head/tail truncate protocol bytes.
- Any protocol failure preserves previously accepted complete outputs.
- Domain owns result completeness; dr-exec never synthesizes missing outputs.
- Payload overflow does not corrupt trusted structured outputs.
- Per-frame, aggregate-byte, depth, and output-count limits apply only when
  their self-budget axes are finite.

Golden vectors cover valid zero/one/multiple-output streams and every ordering,
framing, configured-limit, identity, duplicate-key, and incomplete-stream case.

## Core engine requirements

- **Command resolution:**
  - use granted environment `PATH`;
  - absent granted `PATH`: only absolute executable resolves;
  - relative executable without `PATH`: pre-spawn declaration error;
  - spawn `ENOENT`: spawn absence;
  - other spawn errors: preserve errno; machine attribution.
- **Best-effort attribution precedence after teardown:**
  1. spawn absence;
  2. output budget;
  3. wall-clock budget;
  4. exit-status interpretation.
- **Attribution meaning:** outcome carries the observed failure category;
  `owner` is a best-effort diagnostic classification, not causal proof or a
  retry guarantee. Evidence selects among payload, executor, and machine; when
  evidence is insufficient, the existing executor fallback remains explicitly
  non-probative. Executor/bootstrap machinery failures raise when no trustworthy
  result exists.
- **Race rule:** recorded output violation beats deadline or clean exit.
- **Timing:**
  - duration: spawn through reap; monotonic clock;
  - parent setup excluded;
  - teardown duration separate;
  - record timestamps UTC.
- **Scratch cleanup:** every exit path.
- **Cleanup failure:** narrated; executor-attributed; does not replace otherwise
  trustworthy result.
- **Digests:** SHA-256 over explicitly canonicalized bytes.
- **Environment identity:** sorted declared names plus canonical name/value
  payload digest; values never persisted.

## Durable recording

### Store contract

- `DirectoryRunStore.prepare()` allocates one run directory, publishes the
  complete `prepared` manifest, and returns a prepared handle.
- Every successfully published lifecycle state is valid.
- `mark_running()` follows only a successful spawn, publishes the process-bearing
  `running` manifest, and returns a running handle.
- `finalize()` accepts either lifecycle handle. On successful publication it
  flushes retained-output sidecars and publishes the `finalized` manifest; its
  receipt reflects finalization or degradation and the latest valid lifecycle
  state.
- A recognized pre-child outcome may finalize directly from the `prepared`
  handle.
- Distinct lifecycle handles make invalid transitions unrepresentable through
  one ambiguous handle.
- Prepare failure prevents spawn; finalization or post-start publication failure
  is reflected in the receipt and latest valid lifecycle state without replacing
  the execution outcome.
- `load()` validates the canonical manifest, lifecycle state, safe relative
  paths, sidecar lengths, and sidecar digests.

### Secret-safe durable evidence

- Invocation evidence: target discriminant plus full digest of canonical,
  versioned declaration.
- Never expose recoverable:
  - argv;
  - source;
  - stdin;
  - request payload;
  - environment-value excerpt.
- Python additions: request identity, containment profile, runtime evidence.
- Diagnostics: failed field/rule only; no rejected secret-bearing value.
- Environment grant:
  - parent-derived values snapshotted at live-grant construction;
  - record declared names, exclusions, canonical value digest;
  - never record values.
- Production receipt:
  - complete or machine-readable degradation;
  - latest valid lifecycle state;
  - structured failures.
- Fake receipt: not applicable; never represented as a production no-record
  option.

### On-disk layout

```text
<record-root>/
  run-<utc-timestamp>-<uuid>/
    record.json
    stdout.bin
    stderr.bin
```

- One collision-free directory per run, realized by the pinned
  `dr_store.docdir` Document Directory with `prefix="run"` and
  `manifest_name="record.json"`.
- `record.json`: versioned canonical JSON manifest.
- Sidecars: retained payload evidence; recording representation only.
- Truncated sidecar layout: head then tail.
- Manifest records segment lengths; readers never infer contiguity.
- Manifest paths: normalized relative to run directory.
- Normal finalization: sidecar size plus content digest.

### Manifest content

Includes:

- record schema;
- executor and executor-config identities;
- target durable invocation evidence;
- environment-grant identity;
- containment profile;
- complete effective workload budgets;
- resolved Python interpreter, when applicable;
- lifecycle timestamps;
- outcome and attribution;
- measurements;
- every accepted protocol output, inline;
- truncation metadata;
- sidecar references and digests.

Excludes:

- pool capacity;
- queue facts;
- lease facts;
- worker-session facts;
- separate protocol-output artifacts;
- digest-only replacement for accepted outputs;
- secret environment values;
- raw input.

### Lifecycle states

| State | Commit point | Meaning after abrupt parent death |
| --- | --- | --- |
| `prepared` | Before spawn; complete declaration recorded. | Spawn completion unknown. |
| `running` | After successful spawn. | Child started; no trustworthy final outcome. |
| `finalized` | After any required teardown and sidecar finalization. | Complete run. |

- Recognized pre-child outcome: finalize directly from `prepared`.
- Recovery: report incomplete state as incomplete; never infer success from
  sidecars; never manufacture final outcome.

### Crash consistency

- Every transition publishes through the pinned Document Directory's
  atomic durable replace: complete temporary manifest in the run
  directory, flushed, atomically renamed onto `record.json`, directory
  entry flushed.
- Normal finalization:
  1. flush retained sidecars (finalized `SidecarWriter` summaries);
  2. publish manifest with final digests.
- macOS durability is the pinned primitive's claim:
  [`F_FULLFSYNC`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
  where available with `os.fsync` fallback, and
  [same-filesystem atomic replacement](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html).
- Qualified storage: supported local macOS filesystems.
- Outside claim: network mounts, cloud-synchronized directories, filesystems
  without required flush/replace behavior.

> Every successfully published lifecycle state is valid and crash-consistent;
> normal finalization leaves digest-verified retained output artifacts; abrupt
> termination leaves a valid visibly incomplete record; and any recording
> degradation visible to the caller is machine-readable.

### Degradation behavior

- No claim that permissions, capacity, operating system, or hardware never
  fail.
- Record/sidecar failure:
  - never replaces execution outcome;
  - never stops required pipe draining;
  - produces degraded record receipt;
  - preserves last valid on-disk state when available;
  - emits separate executor narration.
- Receipt includes:
  - run identity;
  - allocated record location, if any;
  - latest valid lifecycle state;
  - completion/degradation status;
  - structured recording failures.
- Narration:
  - separate from payload streams;
  - verbose by default;
  - not required as a fourth durable artifact;
  - failure is observability degradation, not payload failure.
- Canonicalization: pinned shared path. dr-exec owns the manifest model,
  safe projection, lifecycle validation, and receipt semantics; the pinned
  `dr_store.docdir` primitive owns allocation, atomic durable publish,
  sidecar streaming/truncation/digests, and verified byte-level reads.
- Load boundary: malformed bytes, invalid lifecycle models, unsafe paths, and
  sidecar length/digest mismatches raise `RecordLoadError`; the original shared
  decoding or validation exception remains the cause.

### Qualification

Directory-store verification covers:

- valid `prepared`, `running`, `finalized` transitions;
- abrupt parent death after an explicit committed-state event;
- valid incomplete recovery;
- atomic finalization with digest-matching retrievable sidecars;
- exact head/tail recovery and produced/dropped counts;
- unwritable, exhausted, and failed-finalization degradation without changed
  attribution;
- concurrent collision-free writers;
- malformed/mismatched manifest and sidecar rejection;
- records for successful and failed real runs.

Synchronization rules:

- synchronize on explicit store events and terminal outcomes;
- timeouts are watchdogs only;
- no sleeps or elapsed-time evidence for committed lifecycle state;
- real-engine tests use fixture-owned temporary directories on supported
  filesystems;
- pure serialization units may use buffers; buffers do not qualify filesystem
  durability.

## Scheduling and throughput

### Acceptance criterion

- **Representative workload:** approximately 100,000 independent generated
  samples; approximately 1,000 cheap tests per sample; supported Mac mini.
- **Upstream topology:** dr-platform durable workflow; dr-graph graph; generated
  jobs arrive continuously after LLM steps.
- **Required paved path:**
  - amortize compile/load/interpreter startup across cases sharing one sample;
  - evaluate independent samples concurrently;
  - bound machine-level concurrency;
  - avoid scheduler-created unbounded process, thread, queue, or result growth;
  - preserve every per-sample result and record.
- **Unbudgeted exposure:** per-run data may exhaust memory/disk; policy is not
  capacity protection.
- **Automatic failure of criterion:**
  - container/provisioned environment per sample;
  - process per test case;
  - sequential outer sample stream;
  - full-sweep materialization before progress;
  - consumer must rebuild admission/concurrency control.

### Execution job

- One submission contains one immutable job and caller context.
- Admission assigns one capacity slot and one fresh child to the submission.
- The child retains the protected protocol handle and emits complete validated
  outputs incrementally while its domain work may remain sequential.
- Every completion pairs exactly the caller context from its submission with the
  one completed execution and never serializes that context; accepted outputs
  survive a later protocol or child failure.

### Pool capacity and admission

- `ProcessExecutor.open_pool()`, `ProcessExecutor.run_many()`, and
  `ExecutionPool.run_stream()` route through the same scheduler.
- Pool reuses scheduling capacity; never interpreters or children.
- No outer process pool around subprocess engine.
- One job consumes one slot.
- Automatic capacity:
  - resolve once at pool open;
  - use usable CPU count;
  - minimum one active slot.
- Fixed capacity: caller-selected positive slot count.
- Heterogeneous weighted jobs: outside v1.
- Record effective capacity.
- Numeric-library thread policy: caller-owned through the explicit environment
  grant; dr-exec does not inject or reject numerical-library settings.
- Admission bound: active capacity.
- Authoritative backlog stays with the caller or durable workflow.
- Streaming intake: request work only when capacity exists.
- Completion buffering: bounded; slow consumer eventually backpressures intake.
- Resident scheduling bound: running plus completed-but-undelivered submissions
  never exceeds active capacity.
- A completed result continues to occupy that bound until delivered; completion
  does not admit replacement work when the bound is full.
- Caller context: every completion carries exactly the context paired with its
  submission; dr-exec never serializes it.

### Completion and lifecycle

- Yield order: completion order.
- Per-job failure: completion data; stream continues.
- Scheduler-wide failure: pool breaks.
- Finite iteration:
  - same scheduler as streaming;
  - lazy input consumption;
  - no full materialization;
  - no future/thread/process per job.
- Normal close: stop intake; drain active work.
- Cancellation boundary: `Executor.run()` accepts one optional `CancelToken`,
  and the pool creates one token per active call.
- `ProcessExecutor` observes pre-spawn cancellation by finalizing a recorded
  `CancelledOutcome` without spawning. After spawn it performs group-targeted
  teardown, reaps the direct child, finalizes the record, and returns
  `CancelledOutcome`.
- `FakeExecutor` passes the token to its responder but does not manufacture or
  rewrite a cancellation outcome; the consumer script owns that completion.
- Abort: stop intake, cancel every active token, wait for executor calls to
  finish required teardown, then close.
- Closed pool: cannot reopen.

### Finite batch

- `run_many()` consumes the finite iterable lazily through the shared pool
  scheduler and admits only while resident capacity exists.
- It yields in completion order, preserves per-job failures as completion data,
  drains admitted work after input exhaustion, and then closes the pool.

### HumanEval adapter

- One job per generated sample plus complete test suite.
- Inside fresh child:
  1. compile/load sample once;
  2. run all tests sequentially;
  3. aggregate per-test outcomes;
  4. return one identity-bearing HumanEval result document.
- Across samples: bounded concurrency.
- Individual test: never an executor scheduling unit.

### Durable streaming worker

- One long-lived execution pool per host.
- Connect pool to dr-platform durable workflow queue.
- Lease next evaluation only when local admission capacity exists.
- Translate workflow payload -> execution job.
- Publish interpreted completion idempotently.

Ownership:

| Owner | Responsibilities |
| --- | --- |
| dr-platform | Durable backlog; leases; renewal; retries; workflow transitions; idempotent result publication. |
| Evaluation worker | Workflow/dr-exec translation. |
| dr-exec | Local bounded execution; durable per-attempt record. |

- Delivery model: at least once.
- Stable logical job IDs plus distinct attempt IDs prevent retries from being
  mistaken for exactly-once physical execution.
- Multiple source loops may feed one host pool.
- Multiple host pools require explicit non-overlapping capacity; each cannot
  independently claim automatic host capacity.

## Verification ownership

- **Engine suite:**
  - lifecycle fault injection;
  - process-group reach;
  - output retention;
  - protocol integrity;
  - bounded pool behavior;
  - durable recording.
- **Domain adapters:**
  - request/result schemas;
  - internal item completeness;
  - workflow interpretation of execution results.
- **Consumer logic tests:** fake executor.
- **Production parity/oracles:** real engine plus temporary directory store.

## Verification antipatterns

- **Golden scope creep.** Golden tests exist only where exact bytes are the
  contract: digest payloads, identity documents, wire literals, persisted
  keys and discriminants, and canonical scalar spellings. Model reprs,
  exception messages, diagnostics, narration, and manifests whose byte layout
  is not load-bearing never get goldens. Every golden beyond a byte contract
  trains maintainers to regenerate goldens reflexively, which erodes the
  tripwire value of the real ones.
- **Mechanical golden regeneration.** A failing golden is never resolved by
  updating the expected bytes to match current output. The failure signals
  that persisted identity is changing meaning; the resolution is a decision —
  revert the drift, or deliberately bump the schema version with new goldens
  for the new version.

## Future design hooks

Outside v1:

- [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md);
- [isolated-host runtime identity portability](../future-plans/isolated-host-runtime-identity-portability.md);
- filesystem/network containment profiles with concrete backends;
- declared-cwd grant;
- public spooled output;
- stdio-passthrough output;
- resource enforcement inside one execution job;
- caller-meaningful per-stream budget declarations beyond deterministic v1
  aggregate allocation;
- high-volume record indexing, sharding, retention management;
- supervised execution;
- interactive execution;
- fleet execution.

These are directions, not compatibility guarantees. Any extension must preserve
the single-engine boundary and revise relevant standing and structured
contracts before implementation.
