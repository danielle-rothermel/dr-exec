# Isolated-host runtime identity portability

Status: future plan; not part of v1.

V1 isolated-host runtime identity includes `resolved_executable`, the
normalized absolute path of the probed interpreter, alongside the
implementation, Python version, cache tag, and platform. Byte-identical
interpreters at different paths therefore compare as distinct runtimes:
rebuilding a virtual environment or moving to another machine fragments
runtime groupings even when the interpreter build is unchanged.

This is a deliberate bias toward false splits over false merges. Two runtimes
never share an identity unless every recorded distinction matches, and the
path is the only v1 evidence distinguishing builds that report identical
metadata. Grouping can always coarsen at read time by ignoring the field; a
distinction never recorded cannot be recovered.

## Remedies

- **Schema-version bump within isolated-host mode:** replace
  `resolved_executable` with a content digest of the resolved interpreter
  binary under a new identity payload schema version, moving the path to
  diagnostic record data. This restores cross-path comparability inside the
  existing mode at the cost of a probe-time file read plus symlink and
  framework-build resolution rules that must themselves be pinned.
- **[Verified uv-provisioned Python runtime](verified-python-runtime.md):**
  the strong form. Runtime identity covers interpreter, standard-library, and
  package bytes; absolute paths are diagnostic data only and never
  participate in identity.

Adopt a remedy when cross-machine or cross-rebuild runtime comparison becomes
a real workload; until then the limitation stands as documented v1 behavior.
