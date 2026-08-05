"""Shared fixtures for the dr-exec test suite."""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

import pytest
from dr_serialize import IdentityDocument, build_identity_document

from dr_exec import (
    AttemptId,
    ExecutionId,
    IsolatedHostPythonRuntime,
    JobId,
)

pytest_plugins = ("support.process",)

TEST_REQUEST_SCHEMA = "dr_exec.test_request"
JOB_UUID = UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70")
ATTEMPT_UUID = UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71")


@pytest.fixture(scope="session")
def host_runtime() -> IsolatedHostPythonRuntime:
    """One immutable probed runtime for tests of runtime consumers."""
    return IsolatedHostPythonRuntime(executable=Path(sys.executable))


@pytest.fixture
def execution_id() -> ExecutionId:
    return ExecutionId(
        job_id=JobId(JOB_UUID),
        attempt_id=AttemptId(ATTEMPT_UUID),
    )


@pytest.fixture
def request_document() -> IdentityDocument:
    return build_identity_document(
        schema=TEST_REQUEST_SCHEMA,
        schema_version=1,
        payload={"b": 2, "a": [1, {"z": None}]},
    )
