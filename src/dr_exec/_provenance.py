"""Executor source snapshot: package version and source-commit provenance.

Provenance is deliberately loose. It distinguishes builds during
experimentation; it never claims content verification of the installed
package or its dependency closure.

Provenance is read from embedded distribution metadata only. The executor
never inspects a repository at run time: a working tree that happens to
enclose the installed package is not evidence about dr-exec's own source,
and its unrelated modifications are not dr-exec's source state.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError, metadata, version
from typing import Literal
from uuid import uuid4

_PACKAGE_NAME = "dr-exec"
# The embedded source-commit metadata key, written at build time. Absent
# metadata means unknown provenance, never a probed substitute.
_SOURCE_COMMIT_METADATA_KEY = "Source-Commit"
_CLEAN = "clean"
_UNKNOWN = "unknown"
_LOWERCASE_HEXADECIMAL = frozenset("0123456789abcdef")
_GIT_OBJECT_ID_LENGTHS = frozenset({40, 64})

type SourceState = Literal["clean", "unknown"]


@dataclass(frozen=True, slots=True)
class ExecutorSourceSnapshot:
    """One resolved snapshot of the executor's own source provenance.

    ``source_commit`` is present only when a complete Git object ID was
    resolved. ``session_id`` distinguishes otherwise indistinguishable
    unknown source states, so unverified builds never compare equal; a
    clean snapshot carries no session identity.
    """

    package_version: str
    source_commit: str | None
    source_state: SourceState
    session_id: str | None


def _resolve_package_version() -> str:
    try:
        return version(_PACKAGE_NAME)
    except PackageNotFoundError:
        return _UNKNOWN


def _embedded_source_commit() -> str | None:
    """Return the build-time source commit, if the build embedded one.

    Anything other than a complete lowercase Git object ID is treated as
    absent, so malformed metadata degrades to unknown provenance rather
    than minting a false provenance claim.
    """
    try:
        embedded = metadata(_PACKAGE_NAME).get(_SOURCE_COMMIT_METADATA_KEY)
    except PackageNotFoundError:
        return None
    if embedded is None:
        return None
    commit = embedded.strip()
    if len(commit) not in _GIT_OBJECT_ID_LENGTHS or any(
        character not in _LOWERCASE_HEXADECIMAL for character in commit
    ):
        return None
    return commit


def _snapshot_source() -> ExecutorSourceSnapshot:
    """Resolve provenance from embedded metadata alone.

    An embedded commit is a clean build claim. Without one the state is
    unknown and a construction-session identity keeps otherwise
    indistinguishable builds from comparing equal.
    """
    commit = _embedded_source_commit()
    state: SourceState = _CLEAN if commit is not None else _UNKNOWN
    return ExecutorSourceSnapshot(
        package_version=_resolve_package_version(),
        source_commit=commit,
        source_state=state,
        session_id=None if state == _CLEAN else str(uuid4()),
    )


@cache
def _executor_source_snapshot() -> ExecutorSourceSnapshot:
    """Return this process's one executor source snapshot.

    Resolved once per process so every run in a session shares one
    session identity.
    """
    return _snapshot_source()


__all__ = ["ExecutorSourceSnapshot", "SourceState"]
