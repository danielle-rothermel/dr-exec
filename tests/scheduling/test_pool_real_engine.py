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
    return job_for(trusted_target((sys.executable, "-I", "-c", "pass")))


def failing_job() -> ExecutionJob:
    return job_for(
        trusted_target((sys.executable, "-I", "-c", "raise SystemExit(3)"))
    )


@requires_macos
def test_a_real_batch_completes_every_job_and_records_each_one(
    executor: ProcessExecutor,
) -> None:
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
        assert executor.run_store.load(
            receipt.reference
        ).declaration.execution_id == (one.result.execution_id)

    references = {
        one.record_receipt.reference  # ty: ignore[unresolved-attribute]
        for one in completed
    }
    assert len(references) == len(batch)


@requires_macos
def test_a_failing_real_job_is_completion_data_and_the_batch_continues(
    executor: ProcessExecutor,
) -> None:
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


@requires_macos
def test_a_real_pool_streams_completions_with_their_caller_context(
    executor: ProcessExecutor,
) -> None:
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
    source = (
        "import socket\n"
        "gate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        f"gate.connect(({host!r}, {port}))\n"
        "gate.sendall(b'A')\n"
        "assert gate.recv(1) == b'R'\n"
        "gate.close()\n"
    )
    return job_for(trusted_target((sys.executable, "-I", "-c", source)))
