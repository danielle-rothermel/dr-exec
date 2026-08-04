# Proposed dr-serialize additions for dr-exec v1

## Status and objective

This document proposes the small general-purpose additions dr-serialize needs
to support dr-exec v1 without creating a second canonicalization or strict-JSON
system in dr-exec. It does not broaden dr-serialize into an execution protocol,
Pydantic model codec, persistence library, schema registry, or domain-format
owner.

The accepted dr-exec plan continues to own the meaning of every target, frame,
identity payload, result, record, and failure. Dr-serialize owns only reusable
strict-JSON validation, exact canonical bytes, and identity primitives.

## Existing capabilities to reuse unchanged

Dr-exec builds on these existing dr-serialize capabilities:

- `validate_strict_json()` for already-materialized, already-bounded values;
- `canonical_json()` and its compact, sorted-key, finite-number JSON profile;
- `IdentityDocument`, including its exact three-field shape and snapshot
  semantics;
- `canonical_identity_json()` and `identity_document_hash()`; and
- the existing canonical and identity golden vectors.

Dr-exec never uses `Serializer.to_jsonable()` on protocol data, records,
identity material, raw payload bytes, or secret-bearing values. That API is an
explicitly lossy normalization lane: it may stringify keys, replace bytes, and
apply caller-selected handlers. Its output is useful for diagnostics and other
normalization consumers, not for v1 wire or persistence contracts.

## Required addition 1: canonical JSON bytes

### Public behavior

Add a public byte-oriented canonicalization function with behavior equivalent
to:

```python
def canonical_json_bytes(value: Jsonable, /) -> bytes:
    ...
```

It returns the UTF-8 encoding of the exact text produced by
`canonical_json(value)`. The canonical profile remains:

- keys sorted by their JSON string value;
- compact `,` and `:` separators;
- ASCII escaping under the existing `json.dumps` behavior;
- non-finite numbers rejected;
- no normalization, coercion, custom handlers, or caller-selected formatting;
  and
- no byte-order mark or trailing newline.

`json_hash()` must hash these exact bytes rather than reimplementing the
encoding boundary internally. Existing canonical text and hash results do not
change.

### Identity-document bytes

Also expose the byte sequence already hashed by the identity lane:

```python
def canonical_identity_json_bytes(
    document: IdentityDocument,
    /,
) -> bytes:
    ...
```

`identity_document_hash()` must hash these exact bytes. This gives request and
protocol writers a typed route from an `IdentityDocument` to its canonical
wire bytes without converting it back to an untyped mapping at each call site.

### Verification

Golden tests pin, byte for byte:

- equality with the UTF-8 encoding of existing canonical text vectors;
- empty, scalar, nested, Unicode, escaped-string, integer, and finite-float
  values;
- rejection parity with `canonical_json()`;
- identity-document bytes across schema and version changes; and
- unchanged `json_hash()` and `identity_document_hash()` results.

## Required addition 2: bounded strict JSON decoding

### Public behavior

Add one bytes-first strict decoder. The exact public name may follow
dr-serialize conventions, but its behavior is equivalent to:

```python
def decode_strict_json_bytes(
    data: bytes,
    /,
    *,
    max_bytes: int,
    max_depth: int,
) -> Jsonable:
    ...
```

The decoder:

1. rejects input larger than `max_bytes` before JSON materialization;
2. decodes UTF-8 strictly, with no byte replacement;
3. rejects malformed JSON, trailing non-whitespace input, and multiple root
   values;
4. rejects duplicate object keys before a dictionary can collapse them;
5. rejects `NaN`, positive or negative infinity, and other non-JSON numeric
   constants;
6. enforces `max_depth` before recursive parsing or validation can exhaust the
   Python stack;
7. returns only strict `Jsonable` values, without coercion or normalization;
   and
8. raises typed dr-serialize errors with bounded diagnostics that do not echo
   the complete input.

The byte and depth bounds are mandatory arguments or come from an explicit
immutable limits value. The decoder itself never acquires an unbounded stream.
When a dr-exec executor self-budget is unbudgeted, dr-exec first acquires one
complete finite value without a policy cap, then passes its actual byte length
and maximum structurally possible depth as the decoder's materialization bounds.
Those per-value bounds provide parser safety without inventing a finite
executor policy.

### Error contract

Callers must be able to distinguish at least:

- byte-limit overflow;
- depth-limit overflow;
- invalid UTF-8;
- malformed or trailing JSON;
- duplicate object keys; and
- non-finite numeric input.

The exact exception hierarchy is implementation-plan work. It remains a
dr-serialize error taxonomy rather than importing dr-exec protocol failure
codes. Dr-exec translates these general failures into its closed protocol or
record-load errors at its own boundary.

### Scope boundary

The decoder consumes one complete JSON value. It does not scan NDJSON streams,
find frame boundaries, enforce frame order, understand Pydantic models, select a
schema, or validate domain completeness. Dr-exec first acquires one bounded
complete frame or manifest, then calls this decoder, then applies its own
`TypeAdapter` or model validation.

### Verification

Tests cover:

- exact acceptance parity with `validate_strict_json()` for valid materialized
  values;
- invalid UTF-8 and byte-order marks;
- duplicate keys at the root and nested objects;
- `NaN`, `Infinity`, and `-Infinity`;
- empty, whitespace-only, truncated, malformed, and trailing-value input;
- root scalars, arrays, and objects;
- exact byte-bound edges;
- exact depth-bound edges without relying on `RecursionError`; and
- bounded error diagnostics for large adversarial input.

## Required addition 3: validated full SHA-256 values

Provide one public `Sha256Digest` validation boundary for a full SHA-256 digest:

- exactly 64 characters;
- lowercase hexadecimal only;
- no prefix, whitespace, alternate case, or truncation; and
- an explicit parse failure rather than acceptance as an arbitrary `str`.

`Sha256Digest` is one nominal public value rather than a collection of scattered
validators. Full identity and canonical JSON hash APIs return or are directly
validatable as this value. Display prefixes remain ordinary presentation
strings and never satisfy the full-digest type.

This addition does not make dr-serialize responsible for deciding what bytes a
digest covers. Dr-exec still owns declaration, environment, executor, runtime,
manifest, and sidecar digest payloads. `DirectoryRunStore` continues to hash
raw sidecar bytes with a streaming standard-library SHA-256 implementation; a
generic file-hashing wrapper is not required.

Verification covers exact valid boundaries, every non-hexadecimal class,
uppercase input, lengths 63 and 65, display-prefix rejection, and round trips
through the existing full-hash functions.

## Explicit non-goals for dr-serialize

The v1 integration does not add any of the following to dr-serialize:

- a Pydantic base model or generic Pydantic model codec;
- execution target, budget, outcome, receipt, record, or protocol schemas;
- NDJSON scanning, delimiter policy, frame order, completion accounting, or
  protocol state;
- request/result schema selection or domain completeness validation;
- secret redaction or reflection-based safe projection;
- path containment, sidecar layout, file hashing, atomic writes, fsync, or
  same-filesystem replacement;
- Parquet, Arrow, NumPy, tensor, or other bulk domain formats;
- schema registries or migrations;
- a public conformance-corpus loader or packaged vector data; or
- the future verified-runtime tree-digest algorithm.

Those behaviors derive meaning from execution, persistence, or a consuming
domain and remain with their owners.

## How dr-exec consumes the additions

The canonical write path is:

```text
dr-exec/domain model
  -> explicit safe JSON-mode projection
  -> dr-serialize strict JSON validation
  -> dr-serialize canonical JSON bytes
  -> protocol write or DirectoryRunStore transaction
```

The validated read path is:

```text
bounded bytes acquired by dr-exec
  -> dr-serialize strict bounded JSON decode
  -> dr-exec Pydantic model or frame validation
  -> dr-exec protocol, identity, or lifecycle semantics
```

`model_dump_json()` is not the canonical persistence format. Pydantic owns
field conversion into JSON-mode values; dr-serialize owns the final canonical
JSON profile. Exact Pydantic spellings for UUIDs, paths, timestamps, and base64
bytes remain dr-exec golden contracts under a pinned Pydantic release.

## Delivery and release sequence

1. Implement canonical JSON bytes and identity-document bytes in dr-serialize
   without changing existing text or hash results.
2. Implement the bounded strict decoder and its adversarial test matrix.
3. Implement the validated full SHA-256 value.
4. Release dr-serialize with internal golden coverage for the shared behavior.
5. Pin that release and Pydantic in dr-exec before implementing its boundary
   models, protocol codec, or record codec.

The dr-serialize release is a prerequisite for dr-exec serialization work, but
not for implementing unrelated dr-exec scheduling and declaration models.
