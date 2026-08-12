from __future__ import annotations

import asyncio
from collections.abc import Callable
from threading import Thread

from dr_exec.capabilities.protocols import Executor
from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import ExecutionJob
from dr_exec.recording.models import CompletedExecution


async def offload_blocking[T](
    call: Callable[..., T],
    /,
    *args: object,
    **kwargs: object,
) -> T:
    """Offload one blocking call without blocking the event loop."""

    return await asyncio.to_thread(call, *args, **kwargs)


async def offload_blocking_daemon[T](
    call: Callable[..., T],
    /,
    *args: object,
    **kwargs: object,
) -> T:
    """Offload one blocking call on a daemon thread."""

    loop = asyncio.get_running_loop()
    future: asyncio.Future[T] = loop.create_future()

    def worker() -> None:
        try:
            result = call(*args, **kwargs)
        except BaseException as error:  # noqa: BLE001 - forward to awaiter
            loop.call_soon_threadsafe(_deliver_exception, future, error)
        else:
            loop.call_soon_threadsafe(_deliver_result, future, result)

    Thread(target=worker, daemon=True).start()
    return await future


def _deliver_result[T](future: asyncio.Future[T], result: T, /) -> None:
    if not future.done():
        future.set_result(result)


def _deliver_exception[T](
    future: asyncio.Future[T], error: BaseException, /
) -> None:
    if not future.done():
        future.set_exception(error)


async def offload_run_blocking(
    executor: Executor,
    job: ExecutionJob,
    /,
    *,
    cancellation: CancelToken | None = None,
) -> CompletedExecution:
    """Offload one blocking executor run without blocking the event loop."""

    return await offload_blocking(
        executor.run_blocking, job, cancellation=cancellation
    )


__all__ = [
    "offload_blocking",
    "offload_blocking_daemon",
    "offload_run_blocking",
]
