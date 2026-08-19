from __future__ import annotations

import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document

from dr_exec import (
    Budgets,
    DeclarationError,
    EnvGrant,
    ExecutionJob,
    ImportableEntryPoint,
    InProcessImportableJsonTarget,
    JobId,
    TrustedCommandTarget,
    WorkingDirectoryGrant,
    WorkingDirectoryGrantKind,
)
from dr_exec.declarations.validation import (
    resolve_working_directory_grant,
    validate_declaration,
    validate_working_directory_grant,
)


def test_caller_workspace_rejects_an_empty_relative_path() -> None:
    with pytest.raises(DeclarationError, match="absolute"):
        validate_working_directory_grant(WorkingDirectoryGrant.caller(""))


def test_caller_workspace_rejects_a_relative_path(
    tmp_path: Path,
) -> None:
    relative = tmp_path / "relative"
    relative.mkdir()

    with pytest.raises(DeclarationError, match="absolute"):
        validate_working_directory_grant(
            WorkingDirectoryGrant.caller(Path("relative"))
        )


def test_in_process_importable_json_jobs_reject_a_caller_workspace() -> None:
    workspace = Path(tempfile.mkdtemp(prefix="dr-exec-validation-"))
    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=InProcessImportableJsonTarget(
            entry_point=ImportableEntryPoint(
                module_name="support.in_process_entry_points",
                attribute_name="echo",
            ),
            request=build_identity_document(
                schema="dr_exec.test_request",
                schema_version=1,
                payload={"echo": "value"},
            ),
        ),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
        workspace=WorkingDirectoryGrant.caller(workspace),
    )

    with pytest.raises(DeclarationError, match="only scratch"):
        validate_declaration(job)


def test_resolve_working_directory_grant_canonicalizes_caller_paths(
    tmp_path: Path,
) -> None:
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    resolved = resolve_working_directory_grant(
        WorkingDirectoryGrant.caller(link)
    )

    assert resolved.kind is WorkingDirectoryGrantKind.CALLER
    assert resolved.path == target.resolve()


def test_caller_workspace_declaration_allows_a_nonexistent_path() -> None:
    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=TrustedCommandTarget(
            argv=("/usr/bin/true",),
            stdin=b"",
        ),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
        workspace=WorkingDirectoryGrant.caller(
            Path("/nonexistent/dr-exec-validation-workspace")
        ),
    )

    validate_declaration(job)


def test_resolve_working_directory_grant_rejects_a_nonexistent_path() -> None:
    with pytest.raises(DeclarationError, match="existing directory"):
        resolve_working_directory_grant(
            WorkingDirectoryGrant.caller(
                Path("/nonexistent/dr-exec-validation-workspace")
            )
        )


def test_caller_workspace_declaration_allows_parent_components(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    grant = WorkingDirectoryGrant.caller(jobs / ".." / "workspace")

    validate_working_directory_grant(grant)

    resolved = resolve_working_directory_grant(grant)

    assert resolved.path == workspace.resolve()
