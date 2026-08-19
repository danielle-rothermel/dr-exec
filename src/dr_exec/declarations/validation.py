from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

from dr_exec.core.errors import DeclarationError
from dr_exec.core.kinds import EnvGrantKind
from dr_exec.declarations.models import (
    EnvGrant,
    ExecutionJob,
    FiniteOutput,
    InProcessImportableJsonTarget,
    TrustedCommandTarget,
    TrustedPythonTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
    WorkingDirectoryGrant,
    WorkingDirectoryGrantKind,
)
from dr_exec.declarations.transport import request_transport_bytes


def granted_environment(grant: EnvGrant, /) -> dict[str, str]:
    return {variable.name: variable.value for variable in grant.variables}


def _declared_input_bytes(job: ExecutionJob, /) -> bytes:
    match job.target:
        case TrustedCommandTarget() | UntrustedCommandTarget():
            return job.target.stdin
        case (
            TrustedPythonTarget()
            | UntrustedPythonTarget()
            | InProcessImportableJsonTarget()
        ):
            return request_transport_bytes(job.target.request)


def validate_input_budget(job: ExecutionJob, stdin_bytes: bytes, /) -> None:
    limit = job.budgets.input_bytes.limit
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


def absolute_posix_path_shape_error(path: Path | str, /) -> str | None:
    """Return an error when a path is not a canonical absolute POSIX spelling."""

    posix = path.as_posix() if isinstance(path, Path) else path
    if not posix:
        return "caller working-directory paths must be absolute"
    pure = PurePosixPath(posix)
    if not pure.is_absolute():
        return "caller working-directory paths must be absolute: " + posix
    if any(part in {".", ".."} for part in pure.parts):
        return "caller working-directory paths must be canonical: " + posix
    return None


def validate_working_directory_grant(grant: WorkingDirectoryGrant, /) -> None:
    match grant.kind:
        case WorkingDirectoryGrantKind.SCRATCH:
            return
        case WorkingDirectoryGrantKind.CALLER:
            path = grant.path
            if path is None:
                raise DeclarationError(
                    "caller working-directory grants require a path"
                )
            shape_error = absolute_posix_path_shape_error(path)
            if shape_error is not None:
                raise DeclarationError(shape_error)


def resolve_working_directory_grant(
    grant: WorkingDirectoryGrant,
    /,
) -> WorkingDirectoryGrant:
    """Return a grant whose caller path is resolved once for the attempt."""

    validate_working_directory_grant(grant)
    match grant.kind:
        case WorkingDirectoryGrantKind.SCRATCH:
            return grant
        case WorkingDirectoryGrantKind.CALLER:
            assert grant.path is not None
            resolved = grant.path.resolve()
            if not resolved.is_dir():
                raise DeclarationError(
                    "caller working-directory paths must name an existing "
                    "directory: " + resolved.as_posix()
                )
            return WorkingDirectoryGrant.caller(resolved)


def validate_declaration(job: ExecutionJob, /) -> None:
    validate_input_budget(job, _declared_input_bytes(job))
    validate_working_directory_grant(job.workspace)
    match job.target:
        case InProcessImportableJsonTarget():
            if job.workspace.kind is not WorkingDirectoryGrantKind.SCRATCH:
                raise DeclarationError(
                    "in-process importable JSON jobs accept only scratch "
                    "working-directory grants"
                )
            if job.env.kind is not EnvGrantKind.NONE:
                raise DeclarationError(
                    "in-process importable JSON jobs accept no environment "
                    "grant"
                )
            if isinstance(job.budgets.payload_output, FiniteOutput):
                raise DeclarationError(
                    "in-process importable JSON jobs accept no finite "
                    "payload_output budget"
                )
            return
        case TrustedCommandTarget() | UntrustedCommandTarget():
            validate_command_resolvability(
                job.target.argv, granted_environment(job.env)
            )
        case TrustedPythonTarget() | UntrustedPythonTarget():
            return


__all__ = [
    "absolute_posix_path_shape_error",
    "granted_environment",
    "resolve_working_directory_grant",
    "validate_command_resolvability",
    "validate_declaration",
    "validate_input_budget",
    "validate_working_directory_grant",
]
