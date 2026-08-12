from dr_exec.core.kinds import ExecutorFailureCode


class DeclarationError(ValueError):
    """A pre-spawn execution declaration is invalid."""


class ExecutorFailure(RuntimeError):
    """Executor machinery could not produce a trustworthy result."""

    def __init__(self, message: str, *, code: ExecutorFailureCode) -> None:
        self.code = code
        super().__init__(message)


class RecordLoadError(ValueError):
    """A persisted run record is malformed or inconsistent."""


__all__ = ["DeclarationError", "ExecutorFailure", "RecordLoadError"]
