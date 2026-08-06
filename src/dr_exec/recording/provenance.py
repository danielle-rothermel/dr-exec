from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError, metadata, version
from typing import Literal
from uuid import uuid4

_PACKAGE_NAME = "dr-exec"
_SOURCE_COMMIT_METADATA_KEY = "Source-Commit"
_CLEAN = "clean"
_UNKNOWN = "unknown"
_LOWERCASE_HEXADECIMAL = frozenset("0123456789abcdef")
_GIT_OBJECT_ID_LENGTHS = frozenset({40, 64})

type SourceState = Literal["clean", "unknown"]


@dataclass(frozen=True, slots=True)
class ExecutorSourceSnapshot:
    """Build-label provenance for the executor package.

    An embedded commit labels the build source; it does not verify installed
    package contents or dependency closure.
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
    return _snapshot_source()


__all__ = ["ExecutorSourceSnapshot", "SourceState"]
