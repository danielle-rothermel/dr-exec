"""Executor source snapshot: package version and source-commit provenance.

Provenance is deliberately loose. It distinguishes builds during
experimentation; it never claims content verification of the installed
package or its dependency closure.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Literal
from uuid import uuid4

import dr_exec

_PACKAGE_NAME = "dr-exec"
_GIT_PROBE_TIMEOUT_SECONDS = 10.0
_CLEAN = "clean"
_DIRTY = "dirty"
_UNKNOWN = "unknown"

type SourceState = Literal["clean", "dirty", "unknown"]


@dataclass(frozen=True, slots=True)
class ExecutorSourceSnapshot:
    """One resolved snapshot of the executor's own source provenance.

    ``source_commit`` is present only when a complete Git object ID was
    resolved. ``session_id`` distinguishes otherwise indistinguishable
    dirty or unknown source states, so unverified builds never compare
    equal; a clean snapshot carries no session identity.
    """

    package_version: str
    source_commit: str | None
    source_state: SourceState
    session_id: str | None


def _git_output(argument: str, /, *, work_dir: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(work_dir), *argument.split(" ")],
            capture_output=True,
            check=False,
            text=True,
            timeout=_GIT_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def _resolve_package_version() -> str:
    try:
        return version(_PACKAGE_NAME)
    except PackageNotFoundError:
        return "unknown"


def _resolve_source_provenance(
    work_dir: Path,
    /,
) -> tuple[str | None, SourceState]:
    head = _git_output("rev-parse HEAD", work_dir=work_dir)
    if head is None:
        return None, _UNKNOWN
    commit = head.strip()
    status = _git_output("status --porcelain", work_dir=work_dir)
    if status is None:
        return commit, _UNKNOWN
    if status.strip():
        return commit, _DIRTY
    return commit, _CLEAN


def _snapshot_source(work_dir: Path, /) -> ExecutorSourceSnapshot:
    commit, state = _resolve_source_provenance(work_dir)
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
    session identity and pays the Git probe once.
    """
    return _snapshot_source(Path(dr_exec.__file__).parent)


__all__ = ["ExecutorSourceSnapshot", "SourceState"]
