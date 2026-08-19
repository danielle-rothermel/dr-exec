from __future__ import annotations

import asyncio
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from support.executor import (
    fake_completion,
    job_for,
    python_target,
    trusted_python_target,
    trusted_target,
    untrusted_command_target,
)
from support.process import requires_posix

from dr_exec import (
    Budgets,
    CancelToken,
    CompletedExecution,
    CompleteRecordReceipt,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    FakeExecutor,
    FakeRecordReceipt,
    FiniteByteLimit,
    IsolatedHostPythonRuntime,
    ProcessExecutor,
    RecordReceiptKind,
    RecordState,
    TrustedCommandTarget,
    WorkingDirectoryGrant,
)

if TYPE_CHECKING:
    from dr_exec.capabilities.protocols import Executor


def build_process_executor(
    root: Path,
    runtime: IsolatedHostPythonRuntime,
    /,
) -> ProcessExecutor:
    records = root / "records"
    records.mkdir(exist_ok=True)
    return ProcessExecutor(
        runtime=runtime,
        run_store=DirectoryRunStore(root=records),
    )


def build_fake_executor(_root: Path, /) -> FakeExecutor:
    return FakeExecutor(
        responder=lambda _job, _cancellation: fake_completion()
    )


EXECUTOR_IMPLEMENTATIONS = [
    pytest.param(
        "process",
        marks=(
            requires_posix,
            pytest.mark.integration,
            pytest.mark.subprocess,
            pytest.mark.platform_posix,
        ),
        id="process",
    ),
    pytest.param("fake", id="fake"),
]


@pytest.fixture(params=EXECUTOR_IMPLEMENTATIONS)
def executor(request: pytest.FixtureRequest, tmp_path: Path) -> Executor:
    if request.param == "process":
        runtime = request.getfixturevalue("host_runtime")
        assert isinstance(runtime, IsolatedHostPythonRuntime)
        return build_process_executor(tmp_path, runtime)
    return build_fake_executor(tmp_path)


def clean_exit_command() -> tuple[str, ...]:
    return (sys.executable, "-I", "-c", "pass")


def relative_executable_without_granted_path() -> ExecutionJob:
    return job_for(trusted_target(("dr-exec-test-relative",)))


def untrusted_relative_executable_without_granted_path() -> ExecutionJob:
    return job_for(untrusted_command_target(("dr-exec-test-relative",)))


def relative_executable_with_separator() -> ExecutionJob:
    return job_for(
        trusted_target(("bin/dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def untrusted_relative_executable_with_separator() -> ExecutionJob:
    return job_for(
        untrusted_command_target(("bin/dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def relative_granted_path_entry() -> ExecutionJob:
    return job_for(
        trusted_target(("dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "bin"}),
    )


def empty_granted_path_entry() -> ExecutionJob:
    return job_for(
        trusted_target(("dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:"}),
    )


def input_exceeding_its_declared_budget() -> ExecutionJob:
    return job_for(
        TrustedCommandTarget(
            argv=clean_exit_command(), stdin=b"more than four bytes"
        ),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


def python_request_exceeding_its_declared_budget() -> ExecutionJob:
    return job_for(
        python_target("a-request-longer-than-four-bytes"),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


def trusted_python_request_exceeding_its_declared_budget() -> ExecutionJob:
    return job_for(
        trusted_python_target("a-request-longer-than-four-bytes"),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


INVALID_DECLARATIONS = [
    pytest.param(
        relative_executable_without_granted_path, id="relative-no-path"
    ),
    pytest.param(
        untrusted_relative_executable_without_granted_path,
        id="untrusted-relative-no-path",
    ),
    pytest.param(
        relative_executable_with_separator,
        id="relative-with-separator",
    ),
    pytest.param(
        untrusted_relative_executable_with_separator,
        id="untrusted-relative-with-separator",
    ),
    pytest.param(
        relative_granted_path_entry, id="relative-granted-path-entry"
    ),
    pytest.param(empty_granted_path_entry, id="empty-granted-path-entry"),
    pytest.param(input_exceeding_its_declared_budget, id="input-over-budget"),
    pytest.param(
        python_request_exceeding_its_declared_budget,
        id="untrusted-python-request-over-budget",
    ),
    pytest.param(
        trusted_python_request_exceeding_its_declared_budget,
        id="trusted-python-request-over-budget",
    ),
]


@pytest.mark.parametrize("declaration", INVALID_DECLARATIONS)
def test_every_executor_rejects_the_same_invalid_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    with pytest.raises(DeclarationError):
        executor.run_blocking(declaration())


def valid_absolute_command() -> ExecutionJob:
    return job_for(trusted_target(("/usr/bin/true",)))


def valid_relative_command_with_granted_path() -> ExecutionJob:
    return job_for(
        trusted_target(("true",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def valid_input_within_its_budget() -> ExecutionJob:
    return job_for(
        trusted_target(("/usr/bin/true",)),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4096)),
    )


def valid_untrusted_command() -> ExecutionJob:
    return job_for(untrusted_command_target(clean_exit_command()))


def valid_python_target() -> ExecutionJob:
    return job_for(python_target("conformance"))


def valid_caller_workspace() -> ExecutionJob:
    workspace = Path(tempfile.mkdtemp(prefix="dr-exec-conformance-"))
    return job_for(
        trusted_target(("/usr/bin/true",)),
        workspace=WorkingDirectoryGrant.caller(workspace),
    )


VALID_DECLARATIONS = [
    pytest.param(valid_absolute_command, id="absolute-command"),
    pytest.param(
        valid_relative_command_with_granted_path, id="relative-granted-path"
    ),
    pytest.param(valid_input_within_its_budget, id="input-within-budget"),
    pytest.param(valid_untrusted_command, id="untrusted-command"),
    pytest.param(valid_python_target, id="untrusted-python"),
    pytest.param(valid_caller_workspace, id="caller-workspace"),
]


@pytest.mark.parametrize("declaration", VALID_DECLARATIONS)
def test_every_executor_accepts_the_same_supported_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    executor.run_blocking(declaration())


def test_each_executor_enforces_its_own_receipt_kind(
    executor: Executor,
) -> None:
    receipt = executor.run_blocking(valid_absolute_command()).record_receipt

    if isinstance(executor, FakeExecutor):
        assert isinstance(receipt, FakeRecordReceipt)
        assert receipt.kind is RecordReceiptKind.NOT_APPLICABLE
    else:
        assert isinstance(receipt, CompleteRecordReceipt)
        assert receipt.kind is RecordReceiptKind.COMPLETE
        assert isinstance(executor, ProcessExecutor)
        assert executor.run_store.load(receipt.reference).state is (
            RecordState.FINALIZED
        )


@pytest.mark.parametrize("declaration", VALID_DECLARATIONS)
def test_awaitable_run_matches_run_blocking_for_valid_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    job = declaration()

    async def collect() -> tuple[CompletedExecution, object]:
        offloaded, tick = await asyncio.gather(
            executor.run(job),
            asyncio.sleep(0),
        )
        return offloaded, tick

    offloaded, tick = asyncio.run(collect())
    assert tick is None
    blocking = executor.run_blocking(declaration())
    assert offloaded.result.outcome == blocking.result.outcome
    assert type(offloaded.record_receipt) is type(blocking.record_receipt)


def test_awaitable_run_delegates_to_the_same_blocking_path_as_run_blocking() -> (
    None
):
    shared = fake_completion()
    executor = FakeExecutor(responder=lambda _job, _cancellation: shared)
    job = valid_absolute_command()

    async def collect() -> CompletedExecution:
        return await executor.run(job)

    assert asyncio.run(collect()) is shared
    assert executor.run_blocking(job) is shared


def test_awaitable_run_forwards_cancellation_like_run_blocking(
    executor: Executor,
) -> None:
    token = CancelToken()
    token.cancel()
    job = valid_absolute_command()

    async def collect() -> CompletedExecution:
        return await executor.run(job, cancellation=token)

    offloaded = asyncio.run(collect())
    blocking = executor.run_blocking(
        valid_absolute_command(), cancellation=token
    )
    assert offloaded.result.outcome == blocking.result.outcome
    assert type(offloaded.record_receipt) is type(blocking.record_receipt)
