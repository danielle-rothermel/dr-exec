# Verified uv-provisioned Python runtime

Status: future plan; not part of v1.

V1 provides isolated host Python: it resolves a host interpreter and invokes it
with the pinned `-I -c` shape. That controls important Python-level ambient
inputs, but it does not identify or verify the interpreter, standard library,
or installed package bytes.

The next runtime boundary provides a platform-specific, content-identified
Python installation and package closure provisioned by `uv`. The executor
verifies that closure before use, invokes its interpreter directly, and records
its identity with every run. This guarantee is deliberately named **verified
Python runtime and package closure**, not hermetic execution.

## Target contract

A verified runtime has these properties:

- its CPython patch version, implementation, ABI, platform, and architecture are
  declared;
- its managed Python installation and installed packages live under one runtime
  root, with no symlink resolving outside that root;
- its package set comes from a checked, unchanged lockfile with explicit groups
  and extras;
- editable installs, local path dependencies, and unpinned direct references
  are rejected;
- its identity covers the lockfile, provisioning inputs, interpreter and
  standard-library bytes, installed package bytes, and invocation mode;
- a malformed, incomplete, wrong-platform, or content-mismatched runtime fails
  before child spawn, with no fallback to host Python;
- execution invokes the verified interpreter directly with `-I -c`; `uv` is not
  in the per-run process tree and no network access is needed to execute;
- the runtime identity and manifest schema version are present in every durable
  run record; and
- the installed runtime is immutable by convention and made read-only after
  verification. Read-only permissions are a reproducibility guard, not a
  security boundary against the owning user.

The contract does not provide an operating-system closure, cross-platform
artifact portability, filesystem or network containment, deterministic clocks,
or protection from a privileged actor. Those are distinct boundaries and must
not be implied by the runtime name.

## Ownership and lifecycle

`dr-exec` owns one canonical provision-and-verify path. Callers declare the
runtime inputs; they do not assemble a partially compatible environment or
provide an arbitrary interpreter path.

Provisioning is an explicit setup operation outside the execution hot path. A
runtime is provisioned once into a staging directory, fully verified, made
read-only, and atomically installed at a content-addressed path. Normal
execution loads an installed runtime, verifies it once per executor process,
and reuses the verified in-memory reference for subsequent calls.

The runtime root is disposable derived state. A lockfile and the declared
provisioning inputs are the source of truth; deleting the cache must never lose
the ability to reconstruct a runtime.

## Provisioning inputs

Provisioning accepts a small frozen declaration:

- an exact CPython patch version;
- a pinned `uv` version;
- the lockfile bytes or their verified digest;
- the explicitly selected dependency groups and extras;
- the target platform and architecture; and
- the pinned invocation mode, initially `isolated_c` for `-I -c`.

The `uv` version participates in identity. `uv` supplies managed Python builds,
and an exact Python language version alone is not evidence that two provisioned
installations contain identical bytes.

Provisioning does not accept a pre-existing virtual environment, ambient
`VIRTUAL_ENV`, ambient Python selection, or an unconstrained package request.

## Provisioning workflow

The canonical provisioner performs these steps:

1. Validate the declaration and lockfile before creating runtime state.
2. Create a private staging directory on the same filesystem as the runtime
   cache.
3. Use the pinned `uv` release to install the exact managed CPython build under
   the staging root. Python selection must be forced to that managed
   interpreter rather than allowing system-Python fallback.
4. Create the environment from the absolute path of that interpreter.
5. Perform an exact locked sync with editable installs disabled and with groups
   and extras named explicitly. Provisioning may use the network; execution may
   not depend on it.
6. Run an inspection helper with the staged interpreter. It reports the Python
   implementation, version, ABI/cache tag, platform, architecture, and exact
   installed distributions.
7. Reject dependencies or symlinks that escape the staging root. Hash the
   managed interpreter and standard-library closure plus every installed
   package file according to a versioned tree-digest specification.
8. Serialize the manifest canonically, compute the runtime identity, and verify
   the completed staging tree against that manifest.
9. Make the tree read-only and atomically rename it to a directory named by the
   runtime identity.

Concurrent provisioners may build the same declaration independently, but only
an atomic install can publish the final directory. A losing provisioner accepts
the winner only after verifying that the installed identity and contents match.
No reader observes a partially constructed runtime.

The implementation should use `dr-serialize` for canonical manifest bytes and
digest construction if its accepted contract supports the required stability.
Any missing general canonicalization or digest feature belongs in
`dr-serialize`, rather than a second serialization system in `dr-exec`.

## Manifest and in-memory types

The manifest crosses a persistence and trust boundary, so its components are
validated models. The loaded runtime is an internal immutable value object.
The exact field types and validators are implementation-plan work, but the
shape should remain close to this:

```python
class PythonInterpreterManifest(BaseModel):
    implementation: Literal["cpython"]
    version: PythonVersion
    abi_tag: str
    cache_tag: str
    platform: str
    architecture: str
    executable_relative_path: RelativeRuntimePath
    content_digest: Sha256Digest


class InstalledPackageManifest(BaseModel):
    normalized_name: PackageName
    version: PackageVersion
    content_digest: Sha256Digest


class PythonRuntimeContents(BaseModel):
    uv_version: UvVersion
    lockfile_digest: Sha256Digest
    dependency_groups: tuple[DependencyGroupName, ...]
    extras: tuple[ExtraName, ...]
    invocation_mode: PythonInvocationMode
    interpreter: PythonInterpreterManifest
    packages: tuple[InstalledPackageManifest, ...]


class PythonRuntimeManifest(BaseModel):
    schema_version: Literal[1]
    runtime_identity: PythonRuntimeIdentity
    contents: PythonRuntimeContents


@dataclass(frozen=True, slots=True)
class VerifiedPythonRuntime:
    root: Path
    interpreter: Path
    manifest: PythonRuntimeManifest
```

`PythonInvocationMode` and any other grouped string contracts are unique
`StrEnum` members. Persisted field names, enum values, schema version, and
identity payload keys are explicit wire-format contracts with golden tests;
they are never derived by iterating over model fields or enums.

The manifest uses relative paths so an intact runtime can move as one unit.
Absolute cache paths, creation timestamps, and hostnames are diagnostic data
only and do not participate in identity.

## Runtime identity

`runtime_identity` is the SHA-256 digest of a canonical, versioned identity
payload containing `schema_version` and `contents`. The identity field itself
is excluded from that payload, avoiding a self-reference.

The content digest specification must define, and golden tests must pin:

- path normalization and ordering;
- regular-file byte hashing;
- executable-bit handling;
- directory and empty-directory handling;
- symlink rejection or normalization;
- exclusion of the manifest from the tree it describes; and
- treatment of generated caches such as `__pycache__`.

Package names and versions alone are insufficient. The identity covers the
installed bytes, while the lockfile digest preserves the dependency-resolution
provenance that produced them.

## Loading and verification

Loading a runtime is a fail-closed operation:

1. Parse the manifest with unknown fields rejected.
2. Recompute and compare the runtime identity.
3. Resolve every relative path beneath the runtime root and reject escapes.
4. Compare the declared platform and architecture with the current host.
5. Recompute the versioned content digests.
6. Invoke the candidate interpreter with a fixed inspection program and minimal
   environment.
7. Compare the observed interpreter metadata and installed distributions with
   the manifest.
8. Return a `VerifiedPythonRuntime` only after every check succeeds.

Verification occurs before the runtime becomes available to the executor. The
executor may cache the verified reference within its process; it does not hash
the whole tree before every call. If stronger protection against mutation by
another process becomes necessary, that is a separate storage or operating-
system integrity design rather than an implicit executor promise.

## Execution integration

The executor constructs the child command from the verified absolute
interpreter path and the pinned `-I -c` invocation mode. It retains the v1
environment, working-directory, descriptor, budget, capture, teardown, and
recording contracts.

Every result and durable record carries `runtime_identity` and
`runtime_manifest_schema_version`. Diagnostics may additionally record the
resolved cache path, but that machine-local path is not provenance and does not
participate in comparison or identity.

Runtime declaration or verification failure is distinct from child spawn or
child execution failure. It occurs before spawn and never silently downgrades to
the isolated host-Python path.

## Verification plan

The implementation needs state-based tests covering:

- a golden canonical manifest and runtime identity;
- successful provisioning and execution with an empty package set;
- successful locked provisioning with a representative package;
- interpreter, standard-library, and package-file tampering;
- missing, extra, and version-mismatched distributions;
- malformed manifests, unknown fields, path traversal, and escaping symlinks;
- wrong platform or architecture;
- editable, local-path, and unpinned direct dependency rejection;
- exact `-I -c` child-observable semantics;
- absence of `uv` from the execution process tree;
- runtime identity persistence in successful and failed run records;
- concurrent provisioning of the same identity without partial publication;
  and
- cache relocation with identity preserved.

Tests synchronize concurrent provisioning through explicit barriers and
publication events. Timeouts are watchdogs only; elapsed time is never evidence
that publication or exclusion occurred.

## Recommended delivery slices

1. Define and golden-test the manifest, identity payload, tree digest, and
   verifier against a fixture runtime.
2. Add atomic provisioning of an exact managed CPython runtime with an empty
   package set.
3. Add exact locked package synchronization and installed-package verification.
4. Integrate the verified runtime with Python execution and durable records,
   making the verified runtime the sole path for callers that select this mode.
5. Add cache retention and garbage collection only after runtime correctness and
   provenance are stable.

The first three slices establish the hard boundary before execution depends on
it. The integration slice is a hard cutover for the new mode: no alternate
unverified construction path or compatibility shim remains.

## Decisions to settle during implementation planning

- the exact versioned tree-digest treatment of metadata, bytecode caches, file
  modes, and links;
- whether the first package-bearing runtime consumes a project `uv.lock` or a
  smaller runtime-specific lock artifact;
- the `dr-serialize` additions required for canonical manifest identity;
- the public provisioning command and cache-location configuration;
- cache retention, garbage collection, and coordination with active executors;
  and
- whether artifact export/import is needed beyond deterministic local
  reprovisioning on supported macOS hosts.

These choices refine the storage and delivery mechanics. They do not weaken the
core rule: execution starts only from bytes whose platform-specific closure has
been declared, content-identified, and verified.

## uv references

- [Python versions](https://docs.astral.sh/uv/concepts/python-versions/)
- [Project synchronization](https://docs.astral.sh/uv/concepts/projects/sync/)
- [`uv sync` command reference](https://docs.astral.sh/uv/reference/cli/#uv-sync)
