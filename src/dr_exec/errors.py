class DeclarationError(ValueError):
    """A pre-spawn execution declaration is invalid."""


class ExecutorFailure(RuntimeError):
    """Executor machinery could not produce a trustworthy result."""


class RecordLoadError(ValueError):
    """A persisted run record is malformed or internally inconsistent."""


__all__ = ["DeclarationError", "ExecutorFailure", "RecordLoadError"]
