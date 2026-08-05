"""Executor source snapshot and the provenance forms it can produce."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dr_exec._identity import (
    _build_executor_identity,
    _validate_executor_identity,
)
from dr_exec._provenance import (
    ExecutorSourceSnapshot,
    _executor_source_snapshot,
    _snapshot_source,
)


def _git(*arguments: str, work_dir: Path) -> None:
    subprocess.run(
        ["git", "-C", str(work_dir), *arguments],
        capture_output=True,
        check=True,
        timeout=30,
    )


@pytest.fixture
def git_work_dir(tmp_path: Path) -> Path:
    _git("init", "--quiet", work_dir=tmp_path)
    _git("config", "user.email", "test@example.invalid", work_dir=tmp_path)
    _git("config", "user.name", "Test", work_dir=tmp_path)
    (tmp_path / "tracked.txt").write_text("one\n", encoding="utf-8")
    _git("add", "tracked.txt", work_dir=tmp_path)
    _git("commit", "--quiet", "-m", "initial", work_dir=tmp_path)
    return tmp_path


def test_clean_worktree_yields_clean_provenance(git_work_dir: Path) -> None:
    snapshot = _snapshot_source(git_work_dir)
    assert snapshot.source_state == "clean"
    assert snapshot.source_commit is not None
    assert snapshot.session_id is None


def test_dirty_worktree_yields_a_session_identity(git_work_dir: Path) -> None:
    (git_work_dir / "tracked.txt").write_text("two\n", encoding="utf-8")
    snapshot = _snapshot_source(git_work_dir)
    assert snapshot.source_state == "dirty"
    assert snapshot.source_commit is not None
    assert snapshot.session_id is not None


def test_untracked_directory_yields_unknown_provenance(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot_source(tmp_path / "not-a-repository")
    assert snapshot.source_state == "unknown"
    assert snapshot.source_commit is None
    assert snapshot.session_id is not None


def test_dirty_snapshots_never_compare_equal(git_work_dir: Path) -> None:
    (git_work_dir / "tracked.txt").write_text("two\n", encoding="utf-8")
    first = _snapshot_source(git_work_dir)
    second = _snapshot_source(git_work_dir)
    assert first.source_commit == second.source_commit
    assert first != second


def test_the_process_snapshot_is_resolved_once() -> None:
    assert _executor_source_snapshot() is _executor_source_snapshot()


@pytest.mark.parametrize(
    "snapshot",
    [
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 40,
                source_state="clean",
                session_id=None,
            ),
            id="clean",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 64,
                source_state="dirty",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            id="dirty-sha256-object-id",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit=None,
                source_state="unknown",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f73",
            ),
            id="unknown",
        ),
    ],
)
def test_every_provenance_form_builds_a_valid_identity(
    snapshot: ExecutorSourceSnapshot,
) -> None:
    document = _build_executor_identity(snapshot)
    assert _validate_executor_identity(document) is document


@pytest.mark.parametrize(
    ("snapshot", "expected_message"),
    [
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit=None,
                source_state="clean",
                session_id=None,
            ),
            "clean source requires source_commit",
            id="clean-without-commit",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 40,
                source_state="clean",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            "clean source must not have session_id",
            id="clean-with-session",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 40,
                source_state="dirty",
                session_id=None,
            ),
            "requires session_id",
            id="dirty-without-session",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="abc",
                source_state="dirty",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            "complete lowercase Git object ID",
            id="abbreviated-commit",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="A" * 40,
                source_state="dirty",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            "complete lowercase Git object ID",
            id="uppercase-commit",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 40,
                source_state="dirty",
                session_id="not-a-uuid",
            ),
            "canonical UUID",
            id="non-uuid-session",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="",
                source_commit="0" * 40,
                source_state="clean",
                session_id=None,
            ),
            "at least 1 character",
            id="empty-package-version",
        ),
    ],
)
def test_incoherent_provenance_is_rejected(
    snapshot: ExecutorSourceSnapshot,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        _build_executor_identity(snapshot)
