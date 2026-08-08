from __future__ import annotations

import os
from pathlib import Path

from dr_exec.core.errors import DeclarationError
from dr_exec.declarations.models import (
    EnvGrant,
    ExecutionJob,
    FiniteByteLimit,
    TrustedCommandTarget,
    TrustedPythonTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.declarations.transport import request_transport_bytes


def granted_environment(grant: EnvGrant, /) -> dict[str, str]:
    return {variable.name: variable.value for variable in grant.variables}


def declared_input_bytes(job: ExecutionJob, /) -> bytes:
    match job.target:
        case TrustedCommandTarget() | UntrustedCommandTarget():
            return job.target.stdin
        case TrustedPythonTarget() | UntrustedPythonTarget():
            return request_transport_bytes(job.target.request)


def validate_input_budget(job: ExecutionJob, stdin_bytes: bytes, /) -> None:
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
    name = argv[0]
    if Path(name).is_absolute():
        return
    separators = (os.sep,) if os.altsep is None else (os.sep, os.altsep)
    if any(separator in name for separator in separators):
        raise DeclarationError(
            "a relative executable must be a separator-free PATH name: " + name
        )
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
    validate_input_budget(job, declared_input_bytes(job))
    match job.target:
        case TrustedCommandTarget() | UntrustedCommandTarget():
            validate_command_resolvability(
                job.target.argv, granted_environment(job.env)
            )
        case TrustedPythonTarget() | UntrustedPythonTarget():
            return


__all__ = [
    "declared_input_bytes",
    "granted_environment",
    "validate_command_resolvability",
    "validate_declaration",
    "validate_input_budget",
]
