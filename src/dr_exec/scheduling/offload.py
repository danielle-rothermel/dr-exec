from __future__ import annotations

import asyncio

from dr_exec.capabilities.protocols import Executor
from dr_exec.core.cancel import CancelToken
from dr_exec.declarations.models import ExecutionJob
from dr_exec.recording.models import CompletedExecution


async def offload_run_blocking(
    executor: Executor,
    job: ExecutionJob,
    /,
    *,
    cancellation: CancelToken | None = None,
) -> CompletedExecution:
    """Offload one blocking executor call without blocking the event loop."""

    return await asyncio.to_thread(
        executor.run_blocking, job, cancellation=cancellation
    )


__all__ = ["offload_run_blocking"]
