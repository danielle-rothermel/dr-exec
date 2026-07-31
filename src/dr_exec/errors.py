"""The two exception paths. Everything else about a run is data.

Outcomes are values: budget violations, signal deaths, and absent programs
all arrive as a :class:`~dr_exec.record.RunResult`. Exceptions are reserved
for the two cases where no run result exists — the caller's declaration was
never runnable, or the executor's own machinery broke.
"""

from __future__ import annotations


class DrExecError(Exception):
    """Base for the executor's two exception paths."""


class DeclarationError(DrExecError):
    """A pre-spawn caller error: the declaration was never runnable.

    Raised before any child exists — invalid argv, oversized source or
    input, a relative program with no granted ``PATH``, an overlay
    exclusion present in the parent environment.
    """


class ExecutorFailure(DrExecError):
    """The executor's own machinery broke; no run result exists.

    The only exception path after a successful spawn: a process group that
    outlived the termination self-budget, or IPC threads that would not
    join. Payload misbehavior is never an executor failure.
    """
