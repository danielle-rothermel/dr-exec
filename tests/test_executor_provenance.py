"""Executor source snapshot and the provenance forms it can produce."""

from __future__ import annotations

import pytest

from dr_exec import _provenance
from dr_exec._identity import (
    _build_executor_identity,
    _validate_executor_identity,
)
from dr_exec._provenance import (
    ExecutorSourceSnapshot,
    _embedded_source_commit,
    _executor_source_snapshot,
    _snapshot_source,
)


class _Metadata:
    """The subset of distribution metadata provenance reads."""

    def __init__(self, source_commit: str | None) -> None:
        self._source_commit = source_commit

    def get(self, key: str, /) -> str | None:
        return self._source_commit if key == "Source-Commit" else None


def _embed(monkeypatch: pytest.MonkeyPatch, value: str | None, /) -> None:
    """Install the build-time metadata provenance is allowed to read."""
    monkeypatch.setattr(
        _provenance,
        "metadata",
        lambda _name: _Metadata(value),
    )


def test_an_embedded_commit_yields_clean_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _embed(monkeypatch, "a" * 40)
    snapshot = _snapshot_source()
    assert snapshot.source_state == "clean"
    assert snapshot.source_commit == "a" * 40
    assert snapshot.session_id is None


def test_an_embedded_sha256_commit_yields_clean_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _embed(monkeypatch, "b" * 64)
    assert _snapshot_source().source_commit == "b" * 64


def test_an_absent_commit_yields_unknown_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _embed(monkeypatch, None)
    snapshot = _snapshot_source()
    assert snapshot.source_state == "unknown"
    assert snapshot.source_commit is None
    assert snapshot.session_id is not None


@pytest.mark.parametrize(
    "embedded",
    ["abc", "A" * 40, "g" * 40, "", "0" * 41],
    ids=["abbreviated", "uppercase", "non-hex", "empty", "wrong-length"],
)
def test_a_malformed_embedded_commit_is_treated_as_absent(
    monkeypatch: pytest.MonkeyPatch,
    embedded: str,
) -> None:
    _embed(monkeypatch, embedded)
    assert _embedded_source_commit() is None
    assert _snapshot_source().source_state == "unknown"


def test_unknown_snapshots_never_compare_equal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _embed(monkeypatch, None)
    assert _snapshot_source() != _snapshot_source()


def test_provenance_spawns_no_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provenance reads embedded metadata and never probes a repository.

    An enclosing checkout is not evidence about dr-exec's own source, so
    resolving provenance must not shell out at all.
    """

    def _forbidden(*arguments: object, **keywords: object) -> object:
        raise AssertionError("provenance must not spawn a subprocess")

    monkeypatch.setattr("subprocess.run", _forbidden)
    monkeypatch.setattr("subprocess.Popen", _forbidden)
    _embed(monkeypatch, "c" * 40)
    assert _snapshot_source().source_commit == "c" * 40


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
                source_state="unknown",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            id="unknown-with-sha256-object-id",
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
                source_state="unknown",
                session_id=None,
            ),
            "requires session_id",
            id="unknown-without-session",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="abc",
                source_state="unknown",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            "complete lowercase Git object ID",
            id="abbreviated-commit",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="A" * 40,
                source_state="unknown",
                session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
            ),
            "complete lowercase Git object ID",
            id="uppercase-commit",
        ),
        pytest.param(
            ExecutorSourceSnapshot(
                package_version="0.1.0",
                source_commit="0" * 40,
                source_state="unknown",
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
