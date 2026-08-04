class DeclarationError(ValueError):
    """A pre-spawn execution declaration is invalid."""


class ExecutorFailure(RuntimeError):
    """Executor machinery could not produce a trustworthy result."""


__all__ = ["DeclarationError", "ExecutorFailure"]
