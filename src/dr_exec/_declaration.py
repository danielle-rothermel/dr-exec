"""The declaration rules every executor applies, production or fake.

These are the checks that read only the declaration itself: the bytes the
child would receive against the declared input budget, and whether argv[0]
has a defensible meaning under the declared environment grant. They depend
on no host, no runtime, and no process, so one implementation serves both
`ProcessExecutor` and `FakeExecutor` and the two cannot drift into
accepting different declarations.

Host support is deliberately *not* here. Refusing an unsupported platform
is a statement about where a containment claim holds, not about whether a
declaration is well-formed, and only the production path makes that claim.

The Python target's transport bytes are the canonical request document,
which the declaration already carries in full: the runtime chooses the
interpreter invocation but never the request, so the input length measured
here is the same length the engine measures after preparation.
"""

from __future__ import annotations

import os
from pathlib import Path

from dr_exec._protocol import request_transport_bytes
from dr_exec.declare import (
    EnvGrant,
    ExecutionJob,
    FiniteByteLimit,
    TrustedCommandTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.errors import DeclarationError


def granted_environment(grant: EnvGrant, /) -> dict[str, str]:
    """Materialize the exact environment the child receives.

    Values were snapshotted when the grant was constructed, so nothing
    here consults the parent's live environment: the grant is the whole
    inherited state.
    """
    return {variable.name: variable.value for variable in grant.variables}


def declared_input_bytes(job: ExecutionJob, /) -> bytes:
    """Return the exact bytes the declaration would send on child stdin."""
    match job.target:
        case TrustedCommandTarget() | UntrustedCommandTarget():
            return job.target.stdin
        case UntrustedPythonTarget():
            return request_transport_bytes(job.target.request)


def validate_input_budget(job: ExecutionJob, stdin_bytes: bytes, /) -> None:
    """Compare declared input length with the budget before any spawn.

    Input bounds are the one workload axis checked before a child exists,
    so an over-budget input never costs a spawn.
    """
    budget = job.budgets.input_bytes
    limit = budget.max_bytes if isinstance(budget, FiniteByteLimit) else None
    if limit is not None and len(stdin_bytes) > limit:
        raise DeclarationError(
            f"declared input of {len(stdin_bytes)} bytes exceeds the "
            f"{limit}-byte input budget"
        )


def validate_command_resolvability(
    argv: tuple[str, ...],
    environment: dict[str, str],
    /,
) -> None:
    """Refuse an argv[0] the granted environment gives no meaning.

    Absent a granted ``PATH``, only an absolute executable resolves; a
    relative executable with no granted ``PATH`` has no defensible meaning
    and is a declaration error rather than a spawn attempt that would
    consult the parent's ambient search path.

    A granted ``PATH`` resolves only through absolute entries, because the
    child changes to its scratch directory before ``exec``: a relative hit
    would name nothing the search found, and reading it against the
    parent's location is the ambient cwd this package never consults. An
    empty entry is the same case spelled shorter, since it means the
    current directory.

    A name that resolves nowhere is not refused here: production leaves
    that to the spawn, which reports absence as an outcome rather than
    raising.
    """
    name = argv[0]
    if Path(name).is_absolute():
        return
    granted_path = environment.get("PATH")
    if granted_path is None:
        raise DeclarationError(
            "a relative executable requires a granted PATH: " + name
        )
    for entry in granted_path.split(os.pathsep):
        if not Path(entry).is_absolute():
            raise DeclarationError(
                "a granted PATH resolves only through absolute entries: "
                + entry
            )


def validate_declaration(job: ExecutionJob, /) -> None:
    """Apply every host-independent declaration rule, in engine order."""
    validate_input_budget(job, declared_input_bytes(job))
    match job.target:
        case TrustedCommandTarget() | UntrustedCommandTarget():
            validate_command_resolvability(
                job.target.argv, granted_environment(job.env)
            )
        case UntrustedPythonTarget():
            # The runtime owns this invocation and resolved its absolute
            # executable at construction, so there is no caller-declared
            # argv[0] to resolve.
            return


__all__ = [
    "declared_input_bytes",
    "granted_environment",
    "validate_command_resolvability",
    "validate_declaration",
    "validate_input_budget",
]
