from __future__ import annotations

import asyncio

from support.executor import fake_completion, job_for, trusted_target

from dr_exec import FakeExecutor
from dr_exec.scheduling.offload import (
    offload_blocking,
    offload_blocking_daemon,
    offload_run_blocking,
)


def test_offload_blocking_does_not_block_the_event_loop() -> None:
    async def collect() -> tuple[int, object]:
        offloaded, tick = await asyncio.gather(
            offload_blocking(_return_forty_two),
            asyncio.sleep(0),
        )
        return offloaded, tick

    offloaded, tick = asyncio.run(collect())

    assert tick is None
    assert offloaded == 42


def test_offload_blocking_delegates_to_the_call() -> None:
    seen: list[str] = []

    def record(value: str, /) -> str:
        seen.append(value)
        return value

    async def collect() -> str:
        return await offload_blocking(record, "done")

    assert asyncio.run(collect()) == "done"
    assert seen == ["done"]


def test_offload_run_blocking_does_not_block_the_event_loop() -> None:
    shared = fake_completion()
    executor = FakeExecutor(responder=lambda _job, _cancellation: shared)
    job = job_for(trusted_target(("/usr/bin/true",)))

    async def collect() -> tuple[object, object]:
        offloaded, tick = await asyncio.gather(
            offload_run_blocking(executor, job),
            asyncio.sleep(0),
        )
        return offloaded, tick

    offloaded, tick = asyncio.run(collect())

    assert tick is None
    assert offloaded is shared


def test_offload_run_blocking_delegates_to_run_blocking() -> None:
    shared = fake_completion()
    executor = FakeExecutor(responder=lambda _job, _cancellation: shared)
    job = job_for(trusted_target(("/usr/bin/true",)))

    async def collect() -> object:
        return await offload_run_blocking(executor, job)

    assert asyncio.run(collect()) is shared


def test_offload_blocking_daemon_does_not_block_the_event_loop() -> None:
    async def collect() -> tuple[int, object]:
        offloaded, tick = await asyncio.gather(
            offload_blocking_daemon(_return_forty_two),
            asyncio.sleep(0),
        )
        return offloaded, tick

    offloaded, tick = asyncio.run(collect())

    assert tick is None
    assert offloaded == 42


def test_offload_blocking_daemon_delegates_to_the_call() -> None:
    seen: list[str] = []

    def record(value: str, /) -> str:
        seen.append(value)
        return value

    async def collect() -> str:
        return await offload_blocking_daemon(record, "done")

    assert asyncio.run(collect()) == "done"
    assert seen == ["done"]


def _return_forty_two() -> int:
    return 42
