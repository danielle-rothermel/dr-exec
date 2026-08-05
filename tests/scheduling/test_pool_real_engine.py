"""A small end-to-end pass over the real engine through the real pool.

`test_execution_pool.py` qualifies scheduling behavior against a gated
fake, which is the right substrate for questions about ordering and
admission. It cannot answer one question: whether the scheduler and the
production engine actually compose -- whether real children spawn, real
records land, and genuine child processes can overlap under scheduling.

So this file is deliberately small and deliberately real. It runs actual
macOS processes through `ProcessExecutor.run_many` and
`ProcessExecutor.open_pool`, over a real `DirectoryRunStore`, and checks
the properties that only real execution can establish: one durable record
per job, real outcomes, and controlled overlap of genuine children.

Everything here is darwin-marked. The containment and process-group
semantics the engine rests on are macOS's, and the engine refuses any
other platform at the declaration boundary, so these cases are
local-qualification-only by construction rather than by preference.

Synchronization is on terminal outcomes and durable state -- completions
delivered, records on disk -- never on elapsed time. The watchdog is a
watchdog.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from support.executor import job_for, python_target, trusted_target

from dr_exec import (
    CompletedExecution,
    CompleteRecordReceipt,
    DirectoryRunStore,
    ExecutionJob,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExitedOutcome,
    FixedPoolCapacity,
    IsolatedHostPythonRuntime,
    JobId,
    ProcessExecutor,
)

WATCHDOG_SECONDS = 120.0

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.subprocess,
    pytest.mark.platform_macos,
]


@pytest.fixture(autouse=True)
def watchdog() -> Iterator[object]:
    """Fail a wedged case instead of letting it hang the whole suite."""
    timer = threading.Timer(
        WATCHDOG_SECONDS, lambda: os.kill(os.getpid(), signal.SIGALRM)
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: the case did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def executor(
    tmp_path: Path, host_runtime: IsolatedHostPythonRuntime
) -> ProcessExecutor:
    records = tmp_path / "records"
    records.mkdir()
    return ProcessExecutor(
        runtime=host_runtime,
        run_store=DirectoryRunStore(root=records),
    )


def clean_exit_job() -> ExecutionJob:
    """A real child that exits zero without producing output."""
    return job_for(trusted_target((sys.executable, "-I", "-c", "pass")))


def failing_job() -> ExecutionJob:
    """A real child that exits non-zero. Its failure is completion data."""
    return job_for(
        trusted_target((sys.executable, "-I", "-c", "raise SystemExit(3)"))
    )


# --- Finite batch over real children -------------------------------------


@requires_macos
def test_a_real_batch_completes_every_job_and_records_each_one(
    executor: ProcessExecutor,
) -> None:
    """Real children, real outcomes, one durable record per job.

    This is the composition claim: the scheduler admits and delivers, and
    each delivery names a record that is actually on disk. Checking the
    record directories is what distinguishes real execution from a
    scheduler that merely produced values.
    """
    batch = [clean_exit_job() for _ in range(4)]

    completed = list(
        executor.run_many(
            batch,
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=3)
            ),
        )
    )

    assert {one.result.execution_id.job_id for one in completed} == {
        job.job_id for job in batch
    }
    assert all(
        one.result.outcome == ExitedOutcome(exit_code=0) for one in completed
    )
    for one in completed:
        receipt = one.record_receipt
        assert isinstance(receipt, CompleteRecordReceipt)
        assert receipt.record_dir.is_dir()

    directories = {
        one.record_receipt.record_dir  # ty: ignore[unresolved-attribute]
        for one in completed
    }
    assert len(directories) == len(batch)


@requires_macos
def test_a_failing_real_job_is_completion_data_and_the_batch_continues(
    executor: ProcessExecutor,
) -> None:
    """A per-job failure does not stop the stream.

    A child exiting non-zero is an outcome, not a scheduler problem, so
    the batch it belongs to must still deliver every other completion.
    """
    good = [clean_exit_job() for _ in range(2)]
    bad = failing_job()

    completed = list(
        executor.run_many(
            [*good, bad],
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=2)
            ),
        )
    )

    outcomes = {
        one.result.execution_id.job_id: one.result.outcome for one in completed
    }
    assert set(outcomes) == {job.job_id for job in [*good, bad]}
    assert outcomes[bad.job_id] == ExitedOutcome(exit_code=3)
    for job in good:
        assert outcomes[job.job_id] == ExitedOutcome(exit_code=0)


# --- Streaming pool over real children -----------------------------------


@requires_macos
def test_a_real_pool_streams_completions_with_their_caller_context(
    executor: ProcessExecutor,
) -> None:
    """The async surface composes with the engine, context intact.

    Contexts are the caller's own objects; here each job carries its own
    identity as context, and every completion must arrive holding exactly
    the context submitted with that job -- through a real spawn, a real
    record, and a real teardown.
    """
    batch = [clean_exit_job() for _ in range(5)]
    pool = executor.open_pool(
        config=ExecutionPoolConfig(
            capacity=FixedPoolCapacity(max_active_jobs=2)
        )
    )

    async def source() -> AsyncIterator[ExecutionSubmission[JobId]]:
        for job in batch:
            yield ExecutionSubmission(job=job, context=job.job_id)

    async def collect() -> list[tuple[JobId, JobId]]:
        async with pool:
            return [
                (
                    completion.completed_execution.result.execution_id.job_id,
                    completion.context,
                )
                async for completion in pool.run_stream(source())
            ]

    paired = asyncio.run(collect())

    assert len(paired) == len(batch)
    assert all(delivered == context for delivered, context in paired)


@requires_macos
def test_a_real_python_target_runs_through_the_pool(
    executor: ProcessExecutor,
) -> None:
    """The protocol-bearing target works under scheduling too.

    The Python target is the one kind with a protected protocol channel
    and a runtime-owned invocation, so running it through the pool is
    what shows the scheduler does not disturb the engine's descriptor and
    protocol handling.
    """
    batch = [job_for(python_target("pooled"))]

    completed = list(
        executor.run_many(
            batch,
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=2)
            ),
        )
    )

    assert len(completed) == len(batch)
    for one in completed:
        assert one.result.outcome == ExitedOutcome(exit_code=0)
        assert len(one.result.protocol_outputs) == 1


@requires_macos
def test_a_real_pool_holds_multiple_children_in_flight_together(
    executor: ProcessExecutor,
) -> None:
    """The real engine reaches configured overlap under parent-held gates.

    The deterministic fake-backed scheduler cases own the capacity bound.
    This integration case owns the remaining composition claim: two real
    children both announce that they are alive before either is released.
    A serial implementation cannot reach the second announcement.
    """
    slots = 2
    completed: list[CompletedExecution] = []
    failure: BaseException | None = None

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    host, port = server.getsockname()
    batch = [_parent_gated_job(host, port) for _ in range(3)]
    server.listen(len(batch))

    def run_batch() -> None:
        nonlocal failure
        try:
            completed.extend(
                executor.run_many(
                    batch,
                    config=ExecutionPoolConfig(
                        capacity=FixedPoolCapacity(max_active_jobs=slots)
                    ),
                )
            )
        except BaseException as raised:  # noqa: BLE001
            failure = raised

    driver = threading.Thread(target=run_batch)
    driver.start()
    connections: list[socket.socket] = []
    try:
        for _ in range(slots):
            connection, _ = server.accept()
            assert connection.recv(1) == b"A"
            connections.append(connection)

        for connection in connections:
            connection.sendall(b"R")
            connection.close()
        connections.clear()

        tail, _ = server.accept()
        assert tail.recv(1) == b"A"
        tail.sendall(b"R")
        tail.close()
    finally:
        for connection in connections:
            connection.close()
        server.close()

    driver.join(WATCHDOG_SECONDS)
    assert not driver.is_alive(), "watchdog fired joining the real batch"
    assert failure is None
    assert len(completed) == len(batch)


def _parent_gated_job(host: str, port: int, /) -> ExecutionJob:
    """A child that announces entry and waits for its parent's release."""
    source = (
        "import socket\n"
        "gate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        f"gate.connect(({host!r}, {port}))\n"
        "gate.sendall(b'A')\n"
        "assert gate.recv(1) == b'R'\n"
        "gate.close()\n"
    )
    return job_for(trusted_target((sys.executable, "-I", "-c", source)))
