"""Fixed isolated invocation shape and the library-owned child wrapper.

The interpreter is always invoked as ``<executable> -I -c <source>``. The
consumer's ``driver_source`` never reaches argv, a shell, or an import
path: the library-owned wrapper carries it as one embedded string literal
bound to a fixed name.

The wrapper runs inside an isolated host interpreter that is not required
to have dr-exec importable, so its body is stdlib-only and reproduces the
pinned canonical JSON profile directly. It opens the protected descriptor
before any domain code, reads the request through EOF, validates it,
resolves ``dr_exec_main``, and writes LF-terminated canonical frames.

Failure ownership is split at the wrapper boundary. A missing or
non-callable entrypoint, a source-load failure, and a callback failure are
payload-owned: the wrapper stops without a completion frame, so the parent
observes an incomplete stream with every previously accepted output
preserved. A protected-writer failure is executor-owned machinery failure.
"""

from __future__ import annotations

# Child-observable literals. The invocation shape, the entrypoint name,
# the embedded-source binding, and the protected descriptor number are
# pinned; changing any of them is a standing-contract revision, not an
# implementation detail.
ISOLATED_INVOCATION_ARGUMENTS = ("-I", "-c")
DRIVER_ENTRYPOINT_NAME = "dr_exec_main"
DRIVER_SOURCE_BINDING = "DR_EXEC_DRIVER_SOURCE"
PROTOCOL_DESCRIPTOR = 3

# The wrapper body, stdlib-only and self-contained. It reads its own
# module globals and never inspects argv or the environment. The pinned
# child-observable literals are not spelled here: they are rendered from
# the module constants above, which are their single source.
_WRAPPER_BODY = '''
import hashlib as _dr_exec_hashlib
import json as _dr_exec_json
import os as _dr_exec_os
import sys as _dr_exec_sys

_DR_EXEC_IDENTITY_FIELDS = {"schema", "schema_version", "payload"}


def _dr_exec_canonical(value):
    """Render the pinned canonical JSON profile as exact UTF-8 bytes."""
    return _dr_exec_json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _dr_exec_strict(value, path):
    """Accept only strict JSON values; reject anything else by path."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("non-finite number at " + path)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _dr_exec_strict(item, path + "[" + str(index) + "]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("non-string key at " + path)
            _dr_exec_strict(item, path + "." + key)
        return
    raise ValueError("unsupported value at " + path)


def _dr_exec_identity(document, origin):
    """Validate the exact three-field Identity Document shape."""
    if not isinstance(document, dict):
        raise ValueError(origin + " must be an object")
    if set(document) != _DR_EXEC_IDENTITY_FIELDS:
        raise ValueError(origin + " must have exactly the identity fields")
    if not isinstance(document["schema"], str):
        raise ValueError(origin + " schema must be a string")
    version = document["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise ValueError(origin + " schema_version must be an integer")
    _dr_exec_strict(document["payload"], origin + ".payload")
    return document


def _dr_exec_read_request():
    """Read stdin through EOF and validate the canonical request."""
    data = _dr_exec_sys.stdin.buffer.read()
    try:
        decoded = _dr_exec_json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError("request is not strict JSON") from error
    document = _dr_exec_identity(decoded, "request")
    if _dr_exec_canonical(document) != data:
        raise ValueError("request bytes are not canonical JSON bytes")
    return document, data


class _DrExecProtocolWriter:
    """The protected fd 3 writer, owned by the library, not the payload.

    The descriptor is duplicated and the original closed at construction,
    so the handle survives domain code that replaces language-level
    stdout or stderr, and the payload cannot reach the protected stream
    through the well-known descriptor number. Every frame is flushed
    before its call returns, so a later payload crash cannot lose an
    output the parent already accepted.
    """

    def __init__(self, descriptor):
        self._stream = _dr_exec_os.fdopen(_dr_exec_os.dup(descriptor), "wb")
        _dr_exec_os.close(descriptor)
        self._sequence = 0
        self._closed = False

    def prelude(self, request_bytes):
        digest = _dr_exec_hashlib.sha256(request_bytes).hexdigest()
        self._frame({
            "version": 1,
            "kind": "prelude",
            "request_id_sha256": digest,
        })

    def emit(self, document):
        validated = _dr_exec_identity(document, "output document")
        self._frame({
            "version": 1,
            "kind": "output",
            "sequence": self._sequence,
            "document": {
                "schema": validated["schema"],
                "schema_version": validated["schema_version"],
                "payload": validated["payload"],
            },
        })
        self._sequence += 1

    def complete(self):
        self._frame({
            "version": 1,
            "kind": "complete",
            "output_count": self._sequence,
        })
        self._closed = True
        self._stream.close()

    def _frame(self, frame):
        if self._closed:
            raise ValueError("the protected stream is already complete")
        self._stream.write(_dr_exec_canonical(frame) + b"\\n")
        self._stream.flush()


def _dr_exec_bootstrap():
    writer = _DrExecProtocolWriter(_DR_EXEC_PROTOCOL_DESCRIPTOR)
    request, request_bytes = _dr_exec_read_request()
    writer.prelude(request_bytes)
    namespace = {"__name__": "dr_exec_driver"}
    exec(DR_EXEC_DRIVER_SOURCE, namespace)
    entrypoint = namespace.get(_DR_EXEC_ENTRYPOINT_NAME)
    if not callable(entrypoint):
        raise ValueError(
            "driver_source must define a callable " + _DR_EXEC_ENTRYPOINT_NAME
        )
    entrypoint(request, writer.emit)
    writer.complete()


_dr_exec_bootstrap()
'''


def driver_wrapper_source(driver_source: str, /) -> str:
    """Return the library-owned wrapper source embedding ``driver_source``.

    ``driver_source`` is embedded through ``repr``, so arbitrary consumer
    text -- quotes, backslashes, and newlines included -- stays one inert
    string literal in the wrapper rather than executable wrapper syntax.
    The pinned child-observable literals are rendered from the module
    constants, so the child can only ever observe their one spelling.
    """
    if "\0" in driver_source:
        raise ValueError("driver_source must not contain NUL")
    return "\n".join(
        (
            f"{DRIVER_SOURCE_BINDING} = {driver_source!r}",
            f"_DR_EXEC_PROTOCOL_DESCRIPTOR = {PROTOCOL_DESCRIPTOR}",
            f"_DR_EXEC_ENTRYPOINT_NAME = {DRIVER_ENTRYPOINT_NAME!r}",
            _WRAPPER_BODY,
        )
    )


__all__ = [
    "DRIVER_ENTRYPOINT_NAME",
    "DRIVER_SOURCE_BINDING",
    "ISOLATED_INVOCATION_ARGUMENTS",
    "PROTOCOL_DESCRIPTOR",
    "driver_wrapper_source",
]
