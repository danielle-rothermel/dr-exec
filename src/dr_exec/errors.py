"""The two exception paths. Everything else about a run is data.

Outcomes are values: budget violations, signal deaths, and absent programs
all arrive as a :class:`~dr_exec.record.RunResult`. Exceptions are reserved
for the two cases where no run result exists — the caller's declaration was
never runnable, or the executor's own machinery broke.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dr_exec.batch import ItemResult
    from dr_exec.record import RunResult


class DrExecError(Exception):
    """Base for the executor's two exception paths."""


class DeclarationError(DrExecError):
    """A pre-spawn caller error: the declaration was never runnable.

    Raised before any child exists, and raised by the declaration types
    themselves as well as by the entry points — a budget that is not a
    positive number is as unrunnable as an oversized source, so both arrive
    as the same type. Callers catch one exception hierarchy for every way a
    call can be wrong before it starts: invalid argv, oversized source or
    input, a malformed budget or grant, a relative program with no granted
    ``PATH``, an overlay exclusion present in the parent environment.
    """


class ExecutorFailure(DrExecError):
    """The executor's own machinery broke; no run result exists.

    The only exception path after a successful spawn: a process group that
    outlived the termination self-budget, or IPC threads that would not
    join. Payload misbehavior is never an executor failure.
    """


class ProtocolFailure(ExecutorFailure):
    """A batch transcript could not be accounted for: the driver broke.

    The driver is the executor's agent inside the child, so a transcript
    fault is executor-attributed even though it happened across the process
    boundary. Results validated before the fault ride along on ``results``:
    a result once delivered is never lost, and the ``run`` the child produced
    stays available for the consumer's own diagnosis.
    """

    def __init__(
        self,
        message: str,
        *,
        results: tuple[ItemResult, ...],
        run: RunResult,
    ) -> None:
        super().__init__(message)
        self.results = results
        self.run = run
