# dr-exec v1 design

## Unresolved planning gaps

- None.

## Status and authority

- **Planning status:** high-level v1 decisions closed.
- **Primary platform:** macOS.
- **Primary workload:** high-volume HumanEval-style local evaluation.
- **API authority:** pinned releases of the
  [public package surface](../../src/dr_exec/__init__.py) and
  [stable capability boundaries](../../src/dr_exec/protocols.py).
- **Behavioral authority:** this document plus the structured contracts and
  terms listed below.
- **Dependency plan:** [dr-serialize additions](dr-serialize-additions.md).
- **Future runtime:**
  [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md).
- **Proposed package constraints:** single engine; pinned releases; see
  [new contracts](new-contracts.toml).

Exact public imports, names, constructors, fields, defaults, unions, exceptions,
and signatures belong in source. This document specifies behavior, wire formats,
persistence, safety boundaries, ownership, and qualification.

### Repository contracts

Standing repository authority:

- [core terminology](../../.defs/terms.toml);
- [standing contracts](../../.defs/contracts.toml).

V1 proposals, classified against a frozen standing-contract revision:

- [v1 terminology](terms.toml);
- [compatible contract extensions](contract-extensions.toml);
- [contract contradictions](contract-contradictions.toml);
- [new contracts](new-contracts.toml);
- [intentional scope exclusions](intentionally-out-of-scope.toml);
- [unaddressed contracts](unaddressed-contracts.toml).

Source changes must preserve accepted boundaries and qualification obligations.

## V1 boundary summary

| Area | V1 decision |
| --- | --- |
| Runtime | Isolated host Python only; resolved interpreter; `-I`; no hermetic claim. |
| Identity | Versioned role-specific identity documents plus nominal full SHA-256 digests. |
| Budgets | Every workload and executor axis: finite or explicitly unbudgeted; default: unbudgeted. |
| Recording | Mandatory for real executions; execution-local records; temporary stores in engine tests. |
| Containment | Process-boundary-only; no filesystem, network, or full-tree containment claim. |
| Lifecycle | Fresh session/process group; direct child reaped; group-targeted teardown completed before return. |
| Output | Raw stdout/stderr payload bytes; deterministic structural head/tail retention. |
| Protocol | Protected canonical NDJSON stream on fd 3; independent accounting and failures. |
| Scheduling | One job per independent setup-sharing unit; bounded concurrent fresh children. |
| Streaming | One host-level pool; durable backlog, leases, and retries remain in dr-platform. |

## Scope and consumers

### Included

- Target use cases:
  - use cases 1 and 2;
  - captured-output subset of trusted-tool use case 4;
  - job-oriented local scheduling for the initial HumanEval workload.
- Execution:
  - local, call-scoped;
  - direct argv invocation;
  - explicit inherited state;
  - captured payload output;
  - process-group lifecycle management;
  - durable local recording.
- Test support:
  - self-invocation probes;
  - library-owned execution fake;
  - production engine owns spawn-path verification.
- Foundation:
  - one shared call-scoped engine for later use cases;
  - no partial containment, supervision, or fleet surface.

See [target use cases](../high-level-plan/target-usecases.md).

### Excluded

- Filesystem sandboxing.
- Network sandboxing.
- Aggregate RAM enforcement.
- Full process-tree containment.
- Supervised ownership.
- Interactive multi-process orchestration.
- Fleet behavior.
- Full dimension-aware batch contract from use case 3.
- Multi-call trusted-tool aggregation.

### Deferred consumers

Consumers needing a deferred surface retain their own execution path:

| Consumer | Missing surface |
| --- | --- |
| Build hooks; packaging tests | Arbitrary cwd; passthrough output. |
| Repository provenance capture | Arbitrary cwd. |
| Long-lived development servers | Supervision; readiness. |
| Interactive multi-process harnesses | Mid-run polling; rendezvous. |

## Execution boundaries

### Isolated host Python

- **Mode:** isolated host Python; only v1 Python runtime.
- **Construction:** resolve and validate one concrete host interpreter as an
  absolute path.
- **Invocation:** `<interpreter> -I -c <source>`.
- **Python isolation provided:**
  - no user site directory;
  - no current/script directory on `sys.path`;
  - no `PYTHON*` environment influence.
- **Additional executor behavior:**
  - no Python-specific stdio framing;
  - no undeclared environment injection.
- **Not provided:**
  - interpreter-byte verification;
  - standard-library-byte verification;
  - installed-package closure identity;
  - filesystem restriction;
  - network restriction;
  - general hermetic execution;
  - manifest-bearing provisioned runtime.
- **Public claim:** isolated host invocation; never hermetic execution.

Next boundary:
[verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md).

### Workload and executor budgets

- **Representation:** every axis is finite or explicitly unbudgeted.
- **Defaults:** every axis is unbudgeted.
- **Forbidden states:** unset; inferred finite; adaptive; hidden finite.
- **Workload axes:**
  - wall clock;
  - input;
  - output;
  - memory;
  - CPU time;
  - process count;
  - file size;
  - open-file count;
  - disk, where represented by the declaration model.
- **Unsupported enforcement:** axis remains explicitly unbudgeted in the
  effective declaration and durable record.
- **Unsupported finite declaration:** fail validation before spawn.
- **Executor self-budgets:** separate mechanics; same finite-or-unbudgeted
  representation; same unbudgeted defaults.
- **Mandatory spawn validation:** platform constraints, including source and
  aggregate argv-plus-environment limits; not caller workload budgets.
- **RAM:**
  - no default enforcement;
  - no aggregate enforcement;
  - no RAM-protection claim;
  - future aggregate protection must be faithfully enforceable;
  - future protection cannot reinterpret v1 `unbudgeted`.
- **Meaning of unbudgeted:** no library policy limit; not infinite machine
  capacity.
- **Observable external limits:** memory exhaustion, disk exhaustion,
  operating-system limits, machinery failure.

### Containment and lifecycle

- **Profile:** process-boundary-only.
- **Required acknowledgment:** every untrusted target explicitly accepts the
  profile.
- **Payload reach:** invoking user's filesystem, network, credentials, and
  process-spawning permissions.
- **Security claim:** none beyond the declared process boundary.
- **Per-run lifecycle:**
  - fresh session;
  - fresh process group;
  - direct child reaped;
  - configured group-targeted termination completed before a result returns.
- **Finite termination/join self-budget:** escalation and return deadlines.
- **Unbudgeted termination/join:** may wait indefinitely; no bounded-return
  guarantee.
- **Finite join exhaustion:** after group teardown, if inherited pipes still do
  not reach EOF, close the parent ends and raise `ExecutorFailure`; output and
  measurements are not trustworthy enough to manufacture a result, and the
  latest durable lifecycle record remains incomplete/degraded.
- **Reach limit:** original process group only.
- **Known escape:** descendant creates a new session; may survive.
- **Reproducibility/lifecycle controls, not containment:**
  - explicit environment grant;
  - fresh scratch cwd;
  - closed descriptor table;
  - direct argv invocation;
  - workload budgets.

## Serialization ownership

### Dependencies

- Pin released `dr-serialize`; do not copy or fork its canonicalization or
  identity behavior.
- Pin Pydantic; its JSON-mode conversion contributes to bytes later validated
  and canonicalized by dr-exec.
- Required shared additions and qualification:
  [dr-serialize additions](dr-serialize-additions.md).
- Shared behavior is pinned by dr-serialize's internal goldens; v1 does not add
  a public conformance-corpus loader or packaged vector data.
- Forbidden completion state: dr-exec codec or record code depends on an
  unreleased sibling checkout or locally duplicates a proposed shared
  capability.

### Ownership matrix

| Owner | Responsibilities |
| --- | --- |
| `dr-serialize` | Strict materialized-JSON validation; canonical JSON text/bytes; `IdentityDocument` canonicalization and hashing; bounded strict decode of one complete JSON value; validated full SHA-256 value. |
| `dr-exec` | Contract models; scalar spellings; secret-safe projections; execution identity payloads; request/frame schemas; frame scanning/state; lifecycle records; sidecar references; path safety; atomic store publication. |
| Domain adapters | Request/result identity-document schemas; domain completeness; bulk artifact formats; accepted-output interpretation. |

### Forbidden serializer uses

- `Serializer.to_jsonable()` does not define:
  - request data;
  - protocol frames;
  - records;
  - identity material;
  - raw payload bytes;
  - secret-bearing values.
- Reason: normalization is intentionally lossy.
- `model_dump_json()` is not the canonical wire or persistence format.
- Required path: validated Pydantic JSON-mode projection -> strict
  `dr-serialize` validation -> canonical UTF-8 bytes.

## Identity scheme

### Role selection

- **Scheme:** intentionally loose, versioned hybrid.

| Material | Identity form | Rationale |
| --- | --- | --- |
| Executor | `IdentityDocument` | Versioned semantics. |
| Executor configuration | `IdentityDocument` | Versioned policy semantics. |
| Runtime | `IdentityDocument` | Versioned runtime facts. |
| Domain request | `IdentityDocument` | Domain-owned schema and completeness. |
| Protocol output | `IdentityDocument` | Domain-owned schema and completeness. |
| Target declaration | Full nominal SHA-256 | Opaque value-sensitive material; safe structure recorded separately. |
| Canonical environment values | Full nominal SHA-256 | Secret-derived value material; values never persisted. |

### Executor identity

- **Schema:** `dr_exec.executor`.
- **Version:** `1`.
- **Payload keys:** `kind`, `package_version`, `source_commit`, `source_state`,
  `session_id`.
- **Kind:** `process_executor`.
- **Source commit:**
  - full Git object ID embedded at package build when available;
  - editable fallback inspects package source checkout, never process cwd;
  - snapshot when the executor is constructed.
- **Source state:** `clean`, `dirty`, or `unknown`.
- **Session identity:** dirty or unknown source gets a construction-scoped
  session ID; unverified states cannot compare equal only because they share a
  commit or package version.

### Executor-configuration identity

- **Schema:** `dr_exec.executor_config`.
- **Version:** `1`.
- **Payload:** complete effective executor self-budgets.
- **Default representation:** every axis explicitly unbudgeted.
- **Separation:** source provenance and success-affecting execution policy use
  different identities.

### Runtime identity

- **Schema:** `dr_exec.isolated_host_python_runtime`.
- **Version:** `1`.
- **Payload keys:** `kind`, `resolved_executable`, `implementation`,
  `python_version`, `cache_tag`, `platform`.
- **Construction probe:** selected interpreter; once; under `-I`.
- **Distinguishes:** ordinary host-runtime changes.
- **Does not verify:** interpreter bytes, standard library, installed packages.

### Domain identities

- Domain adapter owns schema, version, complete payload, and change rules.
- Adding an identity-bearing field requires a schema-version change.
- Fields never grow silently under one version.

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
bounded bytes acquired by dr-exec
  -> dr-serialize bounded strict JSON decode
  -> dr-serialize canonical re-encode and byte-for-byte equality check
  -> dr-exec strict Pydantic JSON-mode validation of the same original bytes
  -> dr-exec protocol, identity, and lifecycle validation
```

### Validation ownership

- Dr-exec bounds bytes before decode.
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
- **Payload stdout/stderr:** raw bytes; direct sidecar writes; exact lengths;
  streaming SHA-256; no JSON normalization.
- **Bulk domain formats:** outside generic JSON lane; adapter records media
  type, size, digest, and schema identity as required.

### Implementation order

1. Add and adversarially qualify shared `dr-serialize` capabilities; preserve
   existing canonical text and digest results.
2. Release `dr-serialize`; pin it and Pydantic in dr-exec.
3. Implement boundary models, safe projections, scalar goldens, role-specific
   identities.
4. Implement and qualify request transport, protected protocol codec, and
   state machine.
5. Implement and qualify lifecycle manifest, sidecars, and directory-store
   transaction protocol.
6. Run end-to-end conformance across canonical bytes, protocol failures,
   partial outputs, crash-consistent records, and adapter completeness.

## Representation contracts

### Model boundary

- Persistence, subprocess, fixture, and untrusted-input values: strict, frozen,
  closed validated models.
- Internal values that never cross a serialization boundary: frozen slotted
  dataclasses.
- Live execution job: never serialized wholesale; environment may contain
  secrets.
- Engine output: separate secret-safe request and record projections.
- Persisted vocabularies: unique pinned strings; closed variants.
- Persisted payload construction: explicit schemas; no enum iteration or field
  reflection.

### Scalar wire spellings

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

## Capability contracts

### Governance

- Stable Protocols: executor, runtime, run store.
- Purpose: freeze foundational behavior before multiple production
  implementations exist.
- Protocol addition/change: loud boundary change; explicit contract review.
- Implementation qualification: shared behavioral conformance; structural
  typing alone is insufficient.
- Serialized variants: closed validated models or discriminated unions; never
  Protocols.

### Executor

- One blocking, thread-safe operation per complete attempt.
- One optional call-scoped `CancelToken`; cancellation is cooperative at the
  Protocol boundary and lifecycle-enforced by each conforming implementation.
- Production sequence:
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
- Reusable executor stores no mutable per-attempt process state.
- Logical job identity: caller supplied and stable.
- Physical attempt identity: distinct for every attempt.
- Result and record receipt: same attempt identity.

### Outcomes and exceptions

- Outcome data:
  - recognized spawn failures;
  - child outcomes;
  - budget outcomes;
  - cancellation;
  - protocol failures.
- Typed exceptions:
  - invalid pre-spawn declarations;
  - machinery failures preventing a trustworthy result.
- Exception translation preserves underlying cause.
- Record prepare failure: prevent spawn; raise.
- Recording degradation after attempt start: receipt data; does not replace
  execution outcome.
- Later protocol failure: preserve previously accepted complete outputs.
- HumanEval normal result: one aggregate domain output containing per-test
  outcomes.

### Fake executor

- Implements the same thread-safe executor Protocol.
- Validates declarations.
- Records immutable calls.
- Selects behavior from the complete declaration through an optional responder
  callable that receives the job and its cancellation token.
- Supports an in-order scripted-result queue as a mutually exclusive
  convenience.
- Returns scripted results.
- Uses explicit not-applicable fake receipt.
- Never executes payloads, creates scratch workspaces, or creates production
  run records.

## Runtime and transport contracts

### Runtime responsibility

- Resolve and validate absolute executable at construction.
- Run one fixed construction-time `-I` probe and retain its runtime record.
- Prepare fixed `<executable> -I -c <library-wrapper-source>` command; the
  wrapper embeds the declared consumer `driver_source` as data, opens the
  protocol handle, decodes the request, resolves `dr_exec_main`, and invokes it.
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
extension. It adds one fixed helper interpreter startup per job; the throughput
qualification measures that cost before v1 acceptance.

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
- Missing/non-callable entrypoint, source-load failure, or callback failure is
  a payload-owned protocol outcome; bootstrap or writer machinery failure is
  executor-owned.
- Honest limitation: payload can discover, close, or write inherited
  descriptors directly.
- Malformed protected bytes: executor protocol failure; not in-process tamper
  resistance.

## Budget and retention contracts

### General rules

- Every workload/self-budget axis: finite or explicitly unbudgeted.
- No unset, adaptive, machine-derived, or capacity-derived limit.
- Unit-specific finite values keep bytes, durations, and counts distinct.
- Effective executor policy participates in executor-config identity.
- Unbudgeted affects volume/wait policy; never validity.
- Always mandatory:
  - canonical framing;
  - closed schemas;
  - message order;
  - request identity;
  - secret exclusion;
  - operating-system constraints.
- Request has no hidden second cap: known canonical length is both caller input
  check and safe decoder materialization bound.
- V1 finite workload enforcement supports wall time, input bytes, and payload
  output only.
- Memory, CPU time, process count, file size, open-file count, and disk must be
  explicitly unbudgeted; a finite declaration for one of those axes is rejected
  before record preparation or spawn.

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
- **Production provenance:** loose versioned executor, executor-config, and
  runtime documents defined above.
- **Fake provenance:** no production record; no manufactured production
  provenance.
- **Pinned identity details:** exact keys, literals, canonicalization, and
  identity strings live in validated models and golden tests; never mutable
  field reflection.

## Durable recording

### Store contract

- Concrete v1 store: `DirectoryRunStore`.
- Real execution: recording mandatory; part of production engine path.
- Engine tests: same store pointed at temporary test-owned storage.
- Fake execution: no run record; explicit fake receipt.
- Lifecycle handles: distinct types; invalid transitions not representable by
  one ambiguous handle.
- Prepare failure: prevent spawn; raise.
- Post-start finalization degradation: receipt data; do not replace outcome.
- Published state: complete execution-local snapshot.
- Excluded header context: pool capacity, queue depth, worker lease,
  pool-session reference.
- External ownership:
  - dr-platform: workflow and lease context;
  - worker telemetry/domain integration: host scheduling observations.

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

- One collision-free directory per run.
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
| `finalized` | After teardown and sidecar finalization. | Complete run. |

- Recognized pre-child outcome: finalize directly from `prepared`.
- Recovery: report incomplete state as incomplete; never infer success from
  sidecars; never manufacture final outcome.

### Crash consistency

- Every transition:
  1. write complete temporary manifest in run directory;
  2. flush manifest;
  3. atomically replace `record.json` on same local filesystem.
- Normal finalization:
  1. flush retained sidecars;
  2. publish manifest with final digests.
- macOS durability:
  - use
    [`F_FULLFSYNC`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
    where available;
  - use
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
  - latest known state;
  - completion/degradation status;
  - structured recording failures.
- Narration:
  - separate from payload streams;
  - verbose by default;
  - not required as a fourth durable artifact;
  - failure is observability degradation, not payload failure.
- Canonicalization: pinned shared path; dr-exec owns manifest model, safe
  projection, lifecycle validation, and transaction semantics.
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
- **Performance qualification:** measure the representative workload in its
  first domain integration; report optimization or hardening recommendations
  separately instead of adding a package benchmark to v1.

### Execution job

- One job = one independently schedulable setup-sharing boundary.
- Caller chooses shared setup, runtime, scratch workspace, child lifetime.
- Dr-exec chooses number of active independent jobs.
- Dr-exec never interprets generated samples, compilers, or tests.
- Every job gets one fresh child.
- Work inside a job may be sequential; expensive setup paid once.
- Persistent child reuse across unrelated jobs: outside v1; prevents poisoned
  interpreter/process state from crossing job boundaries.
- Python driver:
  - retain protected protocol handle before consumer stdout replacement;
  - emit complete validated outputs incrementally;
  - direct descriptor writes remain a declared containment hole.
- Load-phase failure:
  - complete domain output when domain schema supports it;
  - otherwise executor protocol failure.
- Partial work: preserve accepted outputs; never manufacture domain items.

### Pool capacity and admission

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
- Caller context: travels with submission/completion; never serialized by
  dr-exec.

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
- Cancellation boundary: `Executor.run()` accepts one optional `CancelToken`;
  the pool creates one token per active call, and every supported executor runs
  the shared cancellation conformance cases.
- Pre-spawn cancellation: finalize a recorded `CancelledOutcome` without
  spawning.
- Post-spawn cancellation: perform group-targeted teardown, reap the direct
  child, finalize the record, and return `CancelledOutcome`.
- Abort: stop intake, cancel every active token, wait for executor calls to
  finish required teardown, then close.
- Closed pool: cannot reopen.

### Finite batch

- Consume lazily through one pool.
- Admit active capacity only.
- Yield in completion order.
- Drain finite input; then close.
- One job failure never fails fast or erases other results.
- Hundreds of thousands of jobs require neither collection materialization nor
  one scheduler primitive per job.

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

## Future design hooks

Outside v1:

- [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md);
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
