# dr-exec v1 design

## Accepted boundary decisions

V1 uses role-specific identity documents for semantic identities and nominal
digests for opaque value-sensitive material. Run manifests remain
execution-local and contain no pool-admission snapshot or pool-session
reference. Workload and executor self-budget axes are all finite or explicitly
unbudgeted, and every axis defaults to unbudgeted without adaptive or hidden
finite limits. The detailed serialization additions are specified in
[dr-serialize additions](dr-serialize-additions.md).

V1 targets high-volume HumanEval-style evaluation, self-invocation probes, and
execution fakes. Its primary workload is a dr-platform durable workflow whose
dr-graph graph produces generated-code evaluation jobs continuously. V1
implements a narrow call-scoped slice of the
[target use cases](../high-level-plan/target-usecases.md) without making deferred
containment, supervision, or fleet behavior appear partially supported. Its
shared call-scoped foundation supports later use cases without introducing a
second execution engine.

## Decision and contract status

Repository-wide accepted vocabulary and behavior live in:

- [core terminology](../../.defs/terms.toml)
- [standing contracts](../../.defs/contracts.toml)

The v1 plan is classified against a frozen revision of those contracts. These
files are structured proposals rather than standing repository authority:

- [v1 terminology](terms.toml)
- [compatible contract extensions](contract-extensions.toml)
- [contract contradictions](contract-contradictions.toml)
- [new contracts](new-contracts.toml)
- [intentional scope exclusions](intentionally-out-of-scope.toml)
- [unaddressed contracts](unaddressed-contracts.toml)

The source package is canonical for exact public imports, names, constructors,
fields, defaults, unions, exceptions, and signatures. This design owns the
behavioral, wire, persistence, safety, and ownership contracts, aligned with the
structured v1 contracts and terms. Serialization ownership and proposed
shared-library additions are recorded below and in the
[dr-serialize additions](dr-serialize-additions.md). The v1 high-level planning
decisions are closed; source changes must preserve these boundaries and their
qualification obligations.

## Scope and consumer map

V1 serves target use cases 1 and 2 and the captured-output subset of trusted-tool
use case 4. It supplies the job-oriented local scheduling needed by the initial
HumanEval workload without claiming the full dimension-aware batch contract of
use case 3. Multi-call trusted-tool aggregation is not part of the v1 API. V1
also supplies the library-owned fake and owns the production engine's spawn-path
tests.

The primary v1 platform is macOS. V1 provides local call-scoped execution,
captured payload output, process-group lifecycle management, explicit inherited
state, and durable local recording. It does not provide filesystem or network
sandboxing, aggregate RAM enforcement, full process-tree containment,
supervised ownership, interactive multi-process orchestration, or fleet
behavior.

Consumers that require a deferred surface stay on their own execution path
until that complete surface exists:

- build hooks and packaging tests need arbitrary cwd and passthrough output;
- repository provenance capture needs arbitrary cwd;
- long-lived development servers need supervision and readiness; and
- interactive multi-process harnesses need mid-run polling and rendezvous.

## Accepted execution boundaries

### Isolated host Python

V1 has one Python execution mode: isolated host Python. The executor resolves a
concrete host interpreter to an absolute path before spawn, records that path,
and invokes it as:

```text
<interpreter> -I -c <source>
```

The pinned shape supplies Python's isolated invocation behavior, including no
user site directory, no current/script directory on `sys.path`, and no
`PYTHON*` environment influence. The executor adds no Python-specific stdio
framing and injects no undeclared environment values.

This mode does not verify the interpreter or standard-library bytes, identify
an installed package closure, restrict filesystem or network reach, or provide
general hermetic execution. V1 therefore exposes no hermetic-runtime mode or
manifest-bearing provisioned runtime.

The next runtime boundary is the platform-specific, content-verified design in
the [verified uv-provisioned Python runtime plan](../future-plans/verified-python-runtime.md).

### Workload budgets and RAM

Every workload budget axis has one visible value: finite or explicitly
unbudgeted. Until a caller supplies a meaningful finite bound, policy is
explicitly unbudgeted. There is no unset or inferred finite state.

The v1 workload axes include wall clock, input, output, memory, CPU time,
process count, file size, open-file count, and disk where represented by the
declaration model. Unsupported enforcement axes remain explicitly unbudgeted in
the effective declaration and durable record.

Platform-derived spawn validation, such as source and aggregate argv-plus-
environment limits, remains mandatory validation rather than a caller workload
budget. Executor self-budget axes are distinct executor mechanics, but they use
the same finite-or-explicitly-unbudgeted representation and also default to
unbudgeted. V1 has no adaptive or hidden finite executor limits.

V1 performs no default or aggregate RAM enforcement and makes no RAM-protection
claim. A future machine-protection mechanism may add a faithfully enforceable
aggregate limit, but it cannot silently reinterpret the v1 unbudgeted value.
Likewise, an unbudgeted executor axis promises no policy limit rather than
unlimited machine capacity: memory exhaustion, disk exhaustion, operating-
system limits, and machinery failure remain possible and observable.

### Containment and process lifecycle

Filesystem and network sandboxing are out of scope for v1. Every untrusted
execution target requires explicit acknowledgment of the v1
process-boundary-only profile, which grants the payload the invoking user's
filesystem, network, credential, and process-spawning reach. The profile is an
honest reach declaration, not a security sandbox.

Each spawned run starts a fresh session and process group. When the executor
returns a completed run, it has completed the configured group-targeted
termination policy and reaped the direct child. A finite termination or join
self-budget supplies escalation and return deadlines; the unbudgeted default
may wait indefinitely and therefore carries no bounded-return guarantee. This
reach extends only to the original process group. A descendant that creates a
new session can escape that group and may survive; v1 does not claim otherwise.

The environment grant, fresh scratch cwd, closed descriptor table, direct argv
invocation, and workload budgets still constrain child-observable state. Those
properties are reproducibility and lifecycle controls, not filesystem or
network containment.

## Package navigation

The source package, consumed through pinned releases, is the sole navigation
and API authority. See the [public package surface](../../src/dr_exec/__init__.py)
and [stable capability boundaries](../../src/dr_exec/protocols.py). The
single-engine boundary and pinned-release rule are proposed in
[new contracts](new-contracts.toml).

## Serialization ownership and integration

Dr-exec v1 builds on a released, pinned version of `dr-serialize`; it does not
copy or fork that package's canonicalization and identity behavior. It also
pins Pydantic because Pydantic's JSON-mode conversion is part of the bytes that
dr-exec subsequently validates and canonicalizes. The required additions and
their qualification criteria are specified in
[dr-serialize additions](dr-serialize-additions.md).

The ownership boundary is:

| Owner | V1 responsibility |
| --- | --- |
| `dr-serialize` | Strict validation of materialized JSON values; canonical JSON text and bytes; `IdentityDocument` canonicalization and hashing; bounded strict decoding of one complete JSON value; and the validated full SHA-256 value. |
| `dr-exec` | Contract models and scalar spellings; secret-safe projections; execution identity payloads; request and protocol frame schemas; frame scanning and state; lifecycle records; sidecar references; path safety; and atomic store publication. |
| Domain adapters | Request and result `IdentityDocument` schemas, domain completeness rules, bulk artifact formats, and interpretation of accepted protocol outputs. |

`Serializer.to_jsonable()` is not used for request data, protocol frames,
records, identity material, raw payload bytes, or secret-bearing values. Its
normalization behavior is intentionally lossy and therefore cannot define a
wire, identity, or persistence contract.

### V1 identity roles

V1 deliberately starts with a loose, versioned hybrid identity scheme:

- Executor, executor-config, runtime, domain-request, and protocol-output
  identities are `IdentityDocument`s because their schema and version explain
  the identity's meaning.
- Target declarations and canonical environment values use nominal full
  SHA-256 digests. Their records carry safe non-secret structure beside the
  digest rather than wrapping opaque or secret-derived values in another
  identity document.

The production executor document uses schema `dr_exec.executor`, schema version
1, and payload keys `kind`, `package_version`, `source_commit`, `source_state`,
and `session_id`. `kind` is `process_executor`. `source_commit` is the complete
Git object ID embedded at package-build time when possible; an editable-source
fallback inspects the package source checkout, never the process cwd, and
snapshots the result when `ProcessExecutor` is constructed. `source_state` is
one of `clean`, `dirty`, or `unknown`. Dirty or unknown source receives an
executor-construction `session_id` so distinct unverified source states do not
compare equal merely because they share a commit or package version.

The executor-config document uses schema `dr_exec.executor_config`, schema
version 1, and a payload containing the complete effective
`ExecutorSelfBudgets`. This keeps source provenance distinct from policy that
can change whether the same execution succeeds. Every v1 default axis is
recorded explicitly as unbudgeted.

The isolated-host runtime document uses schema
`dr_exec.isolated_host_python_runtime`, schema version 1, and payload keys
`kind`, `resolved_executable`, `implementation`, `python_version`, `cache_tag`,
and `platform`. `IsolatedHostPythonRuntime` probes those facts once from the
selected interpreter under `-I` when constructed. They distinguish ordinary
host-runtime changes but do not verify interpreter bytes, the standard library,
or installed packages.

Domain adapters own the schema, version, complete payload, and change rules for
request and protocol-output documents. Adding an identity-bearing field changes
the owning document's schema version; v1 fields never grow silently under the
same version.

### Validated write and read paths

The canonical write path is:

```text
dr-exec or domain boundary model
  -> explicit secret-safe Pydantic JSON-mode projection
  -> dr-serialize strict JSON validation
  -> dr-serialize canonical JSON bytes
  -> protected protocol write or DirectoryRunStore transaction
```

The validated read path is:

```text
bounded bytes acquired by dr-exec
  -> dr-serialize bounded strict JSON decode
  -> dr-exec Pydantic model or frame validation
  -> dr-exec protocol, identity, and lifecycle validation
```

Dr-exec bounds bytes before decoding. The shared decoder owns general JSON
failures such as invalid UTF-8, duplicate keys, non-finite numbers, malformed
or trailing data, and depth overflow. Dr-exec translates those failures into
its closed protocol or record-load taxonomy and separately enforces frame
grammar, message order, aggregate limits, identities, and lifecycle meaning.

`model_dump_json()` is not the canonical wire or persistence format. Pydantic
converts a validated model into its explicit JSON-mode projection;
`dr-serialize` validates that value and produces the canonical UTF-8 bytes.
Golden tests pin the resulting UUID, path, timestamp, duration, byte, Unicode,
integer, enum, and digest spellings under the exact pinned dependency versions.
Persisted keys and discriminants are explicit literals, not values derived by
iterating enums or reflecting over implementation field names.

### Application to v1 boundaries

- The Python request is one canonical `IdentityDocument` on stdin followed by
  EOF. Each protected protocol frame is one canonical JSON object followed by
  LF on the executor-owned descriptor. Both use closed dr-exec models and the
  shared decoder and canonical-byte path. A finite executor self-budget supplies
  a protocol limit; the default supplies none.
- Request and result documents use `IdentityDocument` only where the owning
  schema, version, and payload are themselves the boundary. Dr-exec defines
  the role-specific payload model and completeness checks; `dr-serialize`
  supplies only the generic document envelope and canonical identity digest.
- `record.json` is a closed versioned dr-exec model serialized through the same
  canonical-byte path. Lifecycle-state validation occurs after strict decode
  and model validation.
- Payload stdout and stderr remain raw bytes. `DirectoryRunStore` writes their
  retained segments directly, records exact lengths, and hashes the raw
  sidecar bytes with streaming SHA-256. They never pass through JSON
  normalization.
- Caller-owned bulk formats remain outside both libraries' generic JSON lane.
  A domain adapter records their declared media type, size, digest, and schema
  identity where its contract requires them.

### Dependency and implementation order

Serialization implementation proceeds in this order:

1. Add and adversarially qualify the required general capabilities in
   `dr-serialize`, preserving all existing canonical text and digest results.
2. Release `dr-serialize`, then pin that release and the selected Pydantic
   release in dr-exec.
3. Implement dr-exec boundary models, safe projections, scalar goldens, and
   role-specific identities.
4. Implement and qualify the request transport and protected protocol codec
   and state machine.
5. Implement and qualify the lifecycle manifest, sidecars, and
   `DirectoryRunStore` transaction protocol.
6. Run end-to-end conformance tests across canonical bytes, protocol failures,
   partial outputs, crash-consistent records, and domain-adapter completeness.

No dr-exec codec or record implementation is complete while it depends on an
unreleased sibling checkout or locally duplicates a proposed `dr-serialize`
capability.

## Behavioral type and capability contracts

The source files linked under [Package navigation](#package-navigation) own the
exact Python API. This section records the behavior those declarations must
continue to express without duplicating their shape.

### Representation boundary and scalar wire spellings

Persistence, subprocess, fixture, and untrusted-input boundaries use strict,
frozen validated models with closed fields. Live internal value objects that
never cross a serialization boundary use frozen slotted dataclasses. A live
execution job is never serialized wholesale because its resolved environment
may contain secrets; the engine derives separate secret-safe request and record
projections.

Closed persisted vocabularies have unique pinned string values and closed
variants. Persisted payloads are built from explicit schema declarations, never
by iterating an enum or reflecting over implementation field names.

V1 owns these dependency-independent scalar wire spellings; dr-serialize owns
their canonical JSON escaping and bytes:

- UUIDs are lowercase 36-character hexadecimal strings with hyphens in the
  `8-4-4-4-12` form.
- Absolute and relative paths are POSIX path strings. Resolved executable paths
  are absolute; run-artifact references are normalized relative paths that
  contain no empty, `.` or `..` component. Serialization never resolves a path
  or follows a symlink.
- Timestamps are UTC RFC 3339 strings with a trailing `Z` and exactly six
  fractional-second digits. Non-UTC and naive datetimes fail validation rather
  than being normalized silently.
- Durations are integer nanoseconds in fields suffixed `_ns`; ISO 8601 duration
  strings and floating-point seconds are not wire forms.
- Bytes in JSON-bearing models are padded RFC 4648 URL-safe base64 strings.
  Command stdin, payload output, and sidecar transport bytes retain their
  separately specified byte contracts and are not base64-wrapped in transit.
- Unicode strings preserve their code-point sequence without normalization.
  Dr-serialize's canonical JSON profile determines escaping, including its
  ASCII-only wire representation.
- Integers use JSON integer syntax with no leading plus, leading zero, exponent,
  or fractional form. Boolean values never validate as integers at strict model
  boundaries.
- Enums use their exact pinned string values.
- SHA-256 digests are exactly 64 lowercase hexadecimal characters without a
  prefix; abbreviated display digests are never accepted at a boundary.

Golden vectors cover each scalar alone and nested in every relevant request,
frame, identity, and record model under the exact pinned Pydantic and
dr-serialize releases.

### Capability governance and execution outcomes

The executor, runtime, and run-store capability boundaries are stable
behavioral Protocols. They deliberately freeze foundational behavior before
multiple production implementations exist. Adding or changing one is a loud
boundary change requiring explicit contract review. Every supported
implementation must satisfy shared behavioral conformance; structural typing
alone does not establish semantic or durability qualification. Serialized
variants remain closed validated models or discriminated unions rather than
Protocols.

The executor capability is one blocking, thread-safe operation for one complete
attempt. The production path validates the declaration, prepares durable state,
creates a scratch workspace, launches one fresh child, exchanges protocol
messages, captures payload output, enforces budgets, tears down and reaps the
child, finalizes the record, and returns. Mutable per-attempt process state does
not live on the reusable executor.

Recognized spawn, child, budget, cancellation, and protocol failures are outcome
data. Invalid pre-spawn declarations and machinery failures that prevent a
trustworthy result raise the source-defined typed exceptions; underlying causes
remain available. A record-prepare failure prevents spawn, while recording
degradation after an attempt begins remains receipt data and does not replace
the execution outcome. Previously accepted complete protocol outputs survive a
later protocol failure. A HumanEval attempt ordinarily returns one aggregate
domain output containing all per-test outcomes.

The caller supplies stable logical job identity, while every physical attempt
receives a distinct identity. A result and its record receipt always identify
the same attempt. The contract-enforcing fake uses the same executor capability,
is thread-safe, records immutable calls, and returns an explicit not-applicable
receipt instead of pretending a production run record exists.

### Runtime and child transports

The isolated host runtime resolves and validates its absolute executable when
constructed and always prepares the fixed
`<executable> -I -c <driver-source>` command. Runtime implementations do not
spawn processes, choose budgets, resolve environment grants, or write records.
The future verified uv-provisioned runtime may implement the same capability,
but conformance alone does not add it to the v1 support matrix.

An untrusted Python child inherits stdin on fd 0, payload stdout on fd 1,
payload stderr on fd 2, and the executor-owned protocol write pipe on fd 3.
Command targets receive only fds 0, 1, and 2. Callers cannot grant arbitrary
descriptors, and fd 3 is never part of an environment grant.

On macOS, the engine creates close-on-exec pipes and uses spawn file actions to
duplicate only the intended child ends onto fds 0 through 3, close the originals,
and create the child session in the same spawn operation. It neither mutates
parent-global descriptor numbers nor uses a pre-exec callback, so concurrent
executor calls do not inherit Python runtime state across a fork boundary.

The Python request is the complete canonical identity-document bytes on stdin,
with no byte-order mark, length prefix, delimiter, or trailing newline; the
parent then closes stdin. Its canonical length is checked against the workload
input budget before spawn and becomes the recorded input-byte measurement. The
driver reads through EOF, performs bounded strict JSON and identity-document
validation, and emits no protocol output for an invalid request.

The driver opens fd 3 before executing domain code and retains the handle even
if payload code replaces its language-level stdout or stderr objects. Payload
code can still discover, close, or write directly to inherited descriptors;
malformed protocol bytes are therefore an executor protocol failure under the
honest process-boundary-only profile, not a claim of in-process tamper
resistance.

### Durable store and secret-safe records

The durable-store capability uses distinct lifecycle handles so invalid state
transitions are not represented by one ambiguous handle. The qualified v1
directory store implements the crash-consistent lifecycle described below. A
prepare failure prevents spawn and raises; after an attempt begins, finalization
degradation is returned in the record receipt without replacing the execution
outcome.

Every published state is one complete execution-local snapshot. Its header
contains record schema and executor provenance, never pool capacity, queue
depth, worker lease, or a pool-session reference. Dr-platform owns durable
workflow and lease context; worker telemetry and the release benchmark own
host-level scheduling observations.

The target discriminant and full digest of the canonical, versioned declaration
together form durable invocation evidence. Records expose no recoverable argv,
source, stdin, request payload, or environment-value excerpt. Python records
add request identity, containment profile, and runtime evidence without
changing that rule. Projection and decoding diagnostics identify the failed
field or rule without embedding rejected secret-bearing values.

Parent-derived environment values are snapshotted when the live grant is
created. Durable records retain the declared names, exclusions, and canonical
value digest, never the values themselves.

Production receipts distinguish complete recording from machine-readable
degradation and report the latest valid lifecycle state and structured failures.
The fake's not-applicable receipt is not a production no-record option.

### Budgets and deterministic retention

Every workload and executor self-budget axis is either finite or explicitly
unbudgeted; there is no unset state, adaptive limit, machine-derived limit, or
capacity-derived limit. Unit-specific finite values keep byte, duration, and
count limits distinct. Unsupported finite workload axes fail declaration
validation before spawn rather than being silently treated as unbudgeted. The
complete effective executor policy participates in executor-config identity.

Unbudgeted affects volume and waiting policy, not validity. Canonical framing,
closed schemas, message order, request identity, secret exclusion, and
unavoidable operating-system limits remain mandatory. The request has no hidden
second byte cap: its known canonical length is checked against the caller's
input budget and also supplies the decoder's safe materialization bound.

A finite output declaration allocates its entire retained-byte total across the
stdout head, stdout tail, stderr head, and stderr tail. That allocation makes
retention deterministic and independent of drain scheduling. If aggregate
production remains within the limit, every byte is retained. Fail-on-overflow
uses the same total as a termination threshold; marked truncation continues
draining and counting through EOF while retaining no more than the declared
total.

Finite protocol volume or structural limits map overflow to a protocol failure
while preserving earlier accepted outputs. Exhausted manifest, narration, or
recording-detail limits degrade observability without replacing the run outcome.
Finite startup, termination, and join values provide watchdog and escalation
deadlines; explicitly unbudgeted time axes carry no bounded-return guarantee.

### Execution sharing, pool capacity, and lifecycle

One execution job is the independent scheduling and setup-sharing boundary. The
caller chooses the work that shares runtime setup, scratch space, and one child
lifetime; dr-exec chooses how many independent jobs are active. Every job uses
one scheduling slot and a fresh child process. The pool reuses scheduling
capacity, not interpreters or child processes, and does not wrap the subprocess
engine in another process pool.

Automatic capacity resolves once when a pool opens from the usable CPU count,
with at least one active slot. Fixed capacity uses a caller-selected positive
slot count. Heterogeneous weighted jobs are outside v1. Effective capacity and
the single-native-thread policy are recorded. The production path adds standard
numeric-library thread limits to resolved job environments; an incompatible
caller value is a declaration error rather than an implicit override. This
controls known oversubscription but does not enforce CPU use by arbitrary
payload code.

Admission is bounded by active capacity plus explicitly configured prefetch;
without prefetch, the authoritative backlog stays in the caller or durable
workflow queue. Streaming intake requests new work only when capacity exists,
does not eagerly consume its source, and bounds completed-result buffering so a
slow consumer eventually applies backpressure. Caller context travels with a
submission and completion but is never serialized by dr-exec.

Completions are yielded in completion order. Ordinary per-job failures remain
completion data and do not terminate the stream; a scheduler-wide failure
breaks the pool. Finite iteration uses the same scheduler without materializing
the input or creating one future, thread, or process per job.

Normal closure stops intake and drains active work. Abort stops intake and
terminates active process groups under the accepted v1 lifecycle contract. A
closed pool cannot reopen.

## Core engine behavior

The following details remain v1 obligations independent of the Python API
shape:

- Command resolution uses the granted environment's `PATH`. With no granted
  `PATH`, only an absolute executable resolves; a relative executable is a
  pre-spawn declaration error. Spawn-time `ENOENT` is spawn absence, while
  other spawn errors preserve their errno and receive machine attribution.
- Attribution is selected after teardown in this order: spawn absence, output
  budget, wall-clock budget, then exit-status interpretation. A recorded output
  violation wins a race with the deadline or a clean exit.
- Duration covers spawn through reap on a monotonic clock, parent setup is
  excluded, teardown duration is separate, and record timestamps are UTC.
  Output measurements count bytes produced after the retention limit so an
  overflowing run still provides sizing evidence.
- Scratch cleanup runs on every exit path. A cleanup failure is narrated and
  executor-attributed without replacing an otherwise trustworthy run result.
- Persisted digests use SHA-256 over explicitly canonicalized bytes.
  Environment identity includes sorted declared names and a digest of the
  canonical name/value payload without persisting the values themselves.
- Production executor, executor-config, and runtime identities use the loose
  versioned documents defined above. Fake executions have no production run
  record and therefore do not manufacture production provenance. Exact keys,
  literals, canonicalization, and identity strings belong in validated models
  and golden tests rather than being derived from mutable code field names.

## Payload retention and protocol integrity

V1 captures and returns payload stdout and stderr as separate raw byte streams.
It adds no in-band framing, decoding, or newline normalization. Caller-owned
artifact paths are recorded separately and are not an executor output-delivery
mode. Executor narration remains on a parent-owned channel and never shares
payload stdout or stderr.

Unbudgeted output retains every produced byte. When a finite payload-output
budget uses marked truncation, each stream retains a deterministic head and
tail and reports at least:

```text
head
tail
produced_bytes
dropped_bytes
```

The retained pieces remain structurally separate. The executor does not insert
a marker or concatenate them as if the omitted bytes had never existed. The
allocation of a finite aggregate output budget between stdout head, stdout
tail, stderr head, and stderr tail is pinned by the declaration and cannot
depend on drain-thread scheduling. The fail-on-overflow action may
terminate the run, while marked truncation continues draining through EOF; both
return the retained structural evidence and exact produced/dropped counts.

Execution protocol output is strict canonical NDJSON on fd 3 with separate
accounting. Each frame is the dr-serialize canonical UTF-8 encoding of exactly
one closed frame model followed by one LF byte. Canonical JSON contains no raw
line breaks, so LF is an unambiguous frame boundary. The wire permits no BOM,
blank line, CRLF, leading or trailing whitespace, missing terminal LF, or bytes
after the completion frame.

The closed v1 wire schema is:

| Frame | Required fields | Meaning |
| --- | --- | --- |
| Prelude | `version`: `1`; `kind`: `"prelude"`; `request_id_sha256`: full SHA-256 digest | Opens the stream and binds it to the canonical request identity. |
| Output | `version`: `1`; `kind`: `"output"`; `sequence`: nonnegative integer; `document`: identity document | Carries one complete, validated domain output at its zero-based position. |
| Complete | `version`: `1`; `kind`: `"complete"`; `output_count`: nonnegative integer | Terminates the stream and declares the number of output frames. |

Exactly one prelude comes first and echoes the full request-identity digest.
Zero or more outputs follow with consecutive zero-based sequence numbers.
Exactly one completion comes last, and its `output_count` must equal the number
of accepted output frames. EOF is valid only after the complete frame and its
LF. Duplicate, skipped, reordered, or post-completion frames fail the protocol.

Invalid UTF-8, invalid JSON, duplicate keys, non-canonical bytes, or a frame
that fails its closed model maps to `MALFORMED_FRAME`; a prelude or frame in the
wrong position maps to `UNEXPECTED_FRAME`; a request digest mismatch maps to
`ID_MISMATCH`; a repeated sequence maps to `DUPLICATE_OUTPUT`; and EOF, a
missing terminal LF, or a mismatched completion count maps to
`INCOMPLETE_STREAM`. A configured finite protocol limit maps its overflow to
`OVERSIZED_FRAME`; with the unbudgeted default there is no size- or count-based
protocol failure.

With a finite frame budget, the parent scans for LF without acquiring beyond
that limit. With the unbudgeted default, it scans without an executor policy cap
and may exhaust machine resources. Once one finite frame has been acquired, the
parent gives the decoder its actual byte length and maximum structurally
possible depth as materialization bounds, re-encodes the decoded value and
requires byte-for-byte canonical equality, then performs Pydantic frame
validation. Protocol bytes are never head/tail truncated. An oversized under a
finite policy, malformed, non-canonical, identity-mismatched, duplicate, or
incomplete stream is an executor protocol failure; the parent preserves every
previously accepted complete protocol output. The owning domain decides whether
those outputs constitute a complete internal result and never relies on dr-exec
to synthesize missing domain items. Payload output can therefore overflow
without corrupting trusted structured outputs.

Per-frame, aggregate-byte, JSON-depth, and output-count limits apply only when
their executor self-budget axes are finite. Golden vectors pin valid zero-,
one-, and multiple-output streams plus every ordering, framing, configured
limit, identity, duplicate-key, and incomplete-stream failure.

## Durable observability and record layout

Every real run writes through one concrete `DirectoryRunStore`. Recording is
part of the production engine path, not a caller-selected delivery mode. A fake
call creates no run record, while tests of the real engine point the same store
implementation at temporary test-owned storage.

### On-disk shape

The initial store uses one collision-free directory per run:

```text
<record-root>/
  run-<utc-timestamp>-<uuid>/
    record.json
    stdout.bin
    stderr.bin
```

`record.json` is a versioned JSON manifest. The output sidecars contain the
retained payload evidence and are a recording representation, not the public
spooled-output delivery mode. When truncation occurs, each sidecar stores its
stream's head followed by its tail; the manifest records both segment lengths,
so readers never mistake their concatenation for a contiguous payload stream.
Manifest paths are relative to the run directory; normal finalization records
each sidecar's size and content digest.

The execution-local manifest contains record schema, executor, and
executor-config identities; the target record that serves as durable invocation
evidence; environment-grant identity; containment profile; complete effective
workload budgets; resolved Python interpreter when relevant; lifecycle
timestamps; outcome; attribution; measurements; accepted protocol outputs;
truncation metadata; and output sidecar references and digests. It contains no
pool capacity, queue, lease, or worker-session facts. Every accepted complete
protocol output remains inline in the finalized `record.json`; v1 does not create
separate protocol-output artifacts or replace them with digest-only references.
Secret environment values and raw input do not enter the manifest merely
because their digests do.

### Crash-consistent lifecycle

The store publishes three valid lifecycle states:

1. `prepared` is committed before spawn and records the complete declaration.
2. `running` is committed after successful spawn.
3. `finalized` is committed after teardown and sidecar finalization.

Spawn absence or another recognized pre-child outcome finalizes directly from
`prepared`. An abrupt parent death leaves the latest successfully published
state intact: `prepared` means spawn completion is unknown, `running` means a
child started without a trustworthy final outcome, and `finalized` is the only
complete-run state. Recovery reports an incomplete record as incomplete; it
does not infer success from sidecars or manufacture a final outcome.

Every state transition writes a complete temporary manifest in the run
directory, flushes it, and replaces `record.json` atomically on the same local
filesystem. Normal finalization flushes retained output sidecars before
publishing the manifest that references their final digests. The macOS path
uses [`F_FULLFSYNC`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
where available before the
[same-filesystem atomic replacement](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/rename.2.html).

The v1 durability claim applies to supported local macOS filesystems. Network
mounts, cloud-synchronized directories, and filesystems without the required
flush and atomic-replace behavior are outside that claim until separately
qualified.

The accepted guarantee is:

> Every successfully published lifecycle state is valid and crash-consistent;
> normal finalization leaves digest-verified retained output artifacts; abrupt
> termination leaves a valid visibly incomplete record; and any recording
> degradation visible to the caller is machine-readable.

No storage API can promise that permissions, capacity, the operating system,
or hardware never fail. A record or sidecar failure therefore does not replace
the execution outcome or stop required pipe draining. It produces a degraded
`RecordReceipt`, preserves the last valid on-disk lifecycle state when one
exists, and emits separate executor narration. The receipt carries the run
identity, record location when allocated, latest known state, completion or
degradation status, and structured recording failure details.

Narration remains separate from payload streams and is verbose by default. V1
does not require the complete narration log to be a fourth durable artifact;
the manifest, sidecars, and machine-readable receipt own the durable contract.
Narration failure is reported as observability degradation rather than payload
failure.

The record manifest uses the pinned `dr-serialize` canonical-byte path described
above. Dr-exec owns the manifest model, safe projection, lifecycle validation,
and transaction semantics; it does not create a second general canonicalization
system merely for records.

### Durable-observability verification

The real directory-store suite covers:

- valid `prepared`, `running`, and `finalized` state transitions;
- abrupt parent death after an explicit committed-state event, leaving a valid
  incomplete record;
- atomic finalization with retrievable digest-matching sidecars;
- head/tail retained-output recovery and exact produced/dropped counts;
- unwritable, exhausted, and failed-finalization stores returning degraded
  receipts without changing execution attribution;
- concurrent writers with collision-free run directories;
- malformed or mismatched manifests and sidecars failing validation; and
- successful and failed real runs both producing records.

Concurrency and abrupt-death tests synchronize on explicit store events and
terminal outcomes. Timeouts are watchdogs only; sleeps and elapsed time are not
evidence that a lifecycle state was committed. Real-engine tests use temporary
directories on the supported filesystem and clean them through test fixture
ownership. Pure serialization units may use buffers, but buffers alone do not
prove filesystem durability.

## Batch, streaming, and fake details

### High-throughput acceptance criterion

High-throughput local evaluation is a primary v1 product requirement, not a
consumer-owned optimization. The representative target is a stream of roughly
100,000 independently generated code samples, each evaluated against a test
suite of roughly 1,000 cheap cases, on the supported Mac mini.

The paved path must let setup and interpreter startup amortize across the cases
that share one generated sample while evaluating many independent samples with
bounded machine-level concurrency. On a representative workload, local
evaluation capacity must exceed the rate at which the upstream LLM pipeline
produces samples, without scheduler-created unbounded active-process, thread,
or queue growth or lost per-sample results and records. Explicitly unbudgeted
per-run data can still exhaust machine memory or disk; the benchmark must make
that exposure measurable rather than treating unbudgeted policy as capacity
protection.

V1 fails this criterion if its default topology starts a container or
provisioned environment per sample, starts a process per test case, evaluates
the outer sample stream sequentially, materializes the full sweep before making
progress, or requires every consumer to reconstruct admission and concurrency
control.

The release benchmark records samples per second, cases per second, active and
queued request high-water marks, child-startup share, peak memory, and the
effective concurrency configuration. The representative test-suite cost and
upstream sample-production rate are pinned with the benchmark so the acceptance
claim is reproducible rather than inferred from synthetic process-start timing
alone.

### Domain-free execution unit

One `ExecutionJob` is one independently schedulable sharing boundary. The
caller decides which work benefits from one setup, runtime, scratch workspace,
and child lifetime; dr-exec decides how many independent jobs should be active.
The pool never interprets domain concepts such as generated samples, compilers,
or test cases.

The protected Python driver retains its protocol-output handle before consumer
code can replace `sys.stdout`; direct file-descriptor writes remain a declared
hole of the process-boundary-only profile. The driver emits complete validated
outputs incrementally. It represents load-phase failure as a complete domain
output when the domain contract supports that representation; otherwise the
stream ends as an executor protocol failure. Dr-exec preserves accepted outputs
but never manufactures per-item domain results. These choices preserve
paid-for partial work without mixing protocol output with payload stdout or
stderr.

Every `ExecutionJob` receives one fresh child. Work inside that job may be
sequential so expensive setup is paid once. Persistent children reused across
unrelated jobs are outside v1 because untrusted code can poison interpreter and
process state.

### Finite batch execution

Finite batch execution consumes jobs lazily through one pool, admits only its
active capacity plus bounded prefetch, yields completed executions in completion
order, drains the finite input, and closes. One job failure does not fail fast
or erase other results.

A caller may produce hundreds of thousands of jobs without materializing the
collection or creating one future, thread, or process per job.

### HumanEval execution shape

The HumanEval adapter constructs one job per generated code sample and complete
test suite. Inside that job's fresh child, domain code:

1. compiles or loads the generated sample once;
2. runs all test cases sequentially against that loaded sample;
3. aggregates the per-test outcomes; and
4. returns one identity-bearing HumanEval result document.

Different samples are separate jobs and therefore run concurrently. Individual
tests are not executor scheduling units. This gives sequential setup-sharing
within one sample and bounded parallelism across samples.

### Durable streaming worker

A long-lived evaluation worker keeps one `ExecutionPool` open and connects it
to dr-platform's durable workflow queue. The worker leases the next evaluation
only when local admission capacity exists, translates the workflow payload into
an execution job, and publishes the interpreted completion idempotently.
Dr-platform owns the durable backlog, leases, lease renewal, retry policy,
workflow transitions, and idempotent result publication. The worker owns
translation between workflow payloads and dr-exec values. Dr-exec owns local
bounded execution and durable per-attempt records.

The integration assumes at-least-once delivery. Stable `JobId`s, distinct
`AttemptId`s, and idempotent workflow completion prevent a retried lease from
being confused with exactly-once physical execution.

V1 runs one machine-level pool per host. Multiple source loops may feed that
pool, but multiple pools must receive explicit non-overlapping capacity
allocations; they cannot each independently claim the host's automatic
capacity.

`FakeExecutor` supports behavior selected from the complete declaration, with
an in-order queue as a convenience. It does not execute payloads, create scratch
workspaces, or create run records. Consumer logic tests use the fake;
production-engine and consumer parity oracles use the real engine with a
temporary `DirectoryRunStore`. The fake validates declarations, records the
immutable jobs it receives for assertion, returns scripted results with
`FakeRecordReceipt`, and satisfies the same thread-safe `Executor` Protocol.

The engine suite owns lifecycle fault-injection, process-group reach,
output-retention, protocol-integrity, bounded-pool, and durable-recording cases.
Domain adapters own request and result schemas, internal item completeness, and
the interpretation of `ExecutionResult` for their workflows.

## Future design hooks

The following extensions remain outside v1:

- the [verified uv-provisioned Python runtime](../future-plans/verified-python-runtime.md);
- filesystem or network containment profiles with concrete platform backends;
- a declared-cwd grant;
- public spooled and stdio-passthrough output delivery;
- resource enforcement for work internal to one `ExecutionJob`;
- caller-meaningful per-stream budget declarations beyond the deterministic v1
  aggregate retention allocation;
- high-volume record indexing, sharding, and retention management; and
- supervised, interactive, and fleet execution.

These are design directions, not compatibility guarantees. Any extension must
preserve the single-engine boundary and revise the relevant standing and
structured contracts before implementation.
