from __future__ import annotations

import asyncio
from collections.abc import Callable

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


__all__ = ["offload_run_blocking"]
