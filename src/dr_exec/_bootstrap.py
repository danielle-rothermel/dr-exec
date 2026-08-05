"""Fixed isolated invocation shape for untrusted Python targets.

The interpreter is always invoked as ``<executable> -I -c <source>``. The
consumer's ``driver_source`` never reaches argv, a shell, or an import
path: the library-owned wrapper carries it as one embedded string literal
bound to a fixed name.

This module owns the invocation shape and the data embedding. The wrapper
body that opens the protected descriptor, decodes the request, resolves
the driver entrypoint, and writes protocol frames is owned by the
protected protocol.
"""

from __future__ import annotations

# Child-observable literals. The invocation shape, the entrypoint name,
# and the embedded-source binding are pinned; changing any of them is a
# standing-contract revision, not an implementation detail.
ISOLATED_INVOCATION_ARGUMENTS = ("-I", "-c")
DRIVER_ENTRYPOINT_NAME = "dr_exec_main"
DRIVER_SOURCE_BINDING = "DR_EXEC_DRIVER_SOURCE"


def driver_wrapper_source(driver_source: str, /) -> str:
    """Return the library-owned wrapper source embedding ``driver_source``.

    ``driver_source`` is embedded through ``repr``, so arbitrary consumer
    text -- quotes, backslashes, and newlines included -- stays one inert
    string literal in the wrapper rather than executable wrapper syntax.
    """
    if "\0" in driver_source:
        raise ValueError("driver_source must not contain NUL")
    return f"{DRIVER_SOURCE_BINDING} = {driver_source!r}\n"


__all__ = [
    "DRIVER_ENTRYPOINT_NAME",
    "DRIVER_SOURCE_BINDING",
    "ISOLATED_INVOCATION_ARGUMENTS",
    "driver_wrapper_source",
]
