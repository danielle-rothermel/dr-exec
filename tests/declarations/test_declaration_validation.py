from __future__ import annotations

from pathlib import Path

import pytest

from dr_exec import DeclarationError, WorkingDirectoryGrant
from dr_exec.declarations.validation import validate_working_directory_grant


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
