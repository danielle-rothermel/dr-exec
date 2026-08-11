"""Worker-side loop for the worker-pool importable JSON executor.

This module is the entry point of every spawned worker process. It imports the
declared entry-point module once at startup, then serves one request at a time
over the two pipes the parent handed it.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from collections.abc import Callable
from typing import IO, Final, cast

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    StrictJsonError,
    build_identity_document,
    canonical_identity_json_bytes,
    validate_identity_document,
    validate_strict_json,
)

# Persisted-format contract: the worker exchanges the same fixed envelope
# literals as the in-process and process executors.
ENVELOPE_SCHEMA: Final = "dr_exec.importable_json"
ENVELOPE_SCHEMA_VERSION: Final = 1

FRAME_TERMINATOR: Final = b"\n"

# Wire values for the one status field of a worker result frame. Never derived
# from Python identifiers.
STATUS_KEY: Final = "status"
STATUS_OK: Final = "ok"
STATUS_PAYLOAD_RAISED: Final = "payload_raised"
STATUS_PAYLOAD_RESULT_INVALID: Final = "payload_result_invalid"
STATUS_EXECUTOR_REJECTED: Final = "executor_rejected"
DETAIL_KEY: Final = "detail"
RESULT_KEY: Final = "result"

READY_FRAME: Final = b"ready" + FRAME_TERMINATOR

_STARTUP_IMPORT_FAILED_EXIT_CODE: Final = 3


def main() -> None:
    """Serve requests until the parent closes the request pipe."""

    module_name = sys.argv[1]
    attribute_name = sys.argv[2]
    request_fd = int(sys.argv[3])
    result_fd = int(sys.argv[4])

    requests = os.fdopen(request_fd, "rb", buffering=0)
    results = os.fdopen(result_fd, "wb", buffering=0)
    try:
        entry_point = _resolve_entry_point(module_name, attribute_name)
    except BaseException:  # noqa: BLE001 - the parent sees a closed pipe
        results.close()
        sys.exit(_STARTUP_IMPORT_FAILED_EXIT_CODE)
    _write_frame(results, READY_FRAME)
    _serve(entry_point, requests=requests, results=results)


def _resolve_entry_point(
    module_name: str, attribute_name: str, /
) -> Callable[[Jsonable], object]:
    module = importlib.import_module(module_name)
    entry_point = getattr(module, attribute_name)
    if not callable(entry_point):
        raise TypeError("the imported module attribute is not callable")
    return cast("Callable[[Jsonable], object]", entry_point)


def _serve(
    entry_point: Callable[[Jsonable], object],
    /,
    *,
    requests: IO[bytes],
    results: IO[bytes],
) -> None:
    while True:
        frame = _read_frame(requests)
        if frame is None:
            return
        _write_frame(results, _handle(entry_point, frame))


def _handle(
    entry_point: Callable[[Jsonable], object], frame: bytes, /
) -> bytes:
    try:
        request = _decode_request(frame)
    except Exception as error:  # noqa: BLE001 - reported to the parent
        return _status_frame(STATUS_EXECUTOR_REJECTED, str(error))
    try:
        payload = validate_strict_json(request.payload)
    except StrictJsonError as error:
        return _status_frame(STATUS_EXECUTOR_REJECTED, str(error))
    try:
        returned = entry_point(payload)
    except Exception:  # noqa: BLE001 - a payload failure is result data
        return _status_frame(
            STATUS_PAYLOAD_RAISED,
            "the importable JSON entry point raised",
        )
    try:
        result = validate_strict_json(returned)
    except StrictJsonError as error:
        return _status_frame(STATUS_PAYLOAD_RESULT_INVALID, str(error))
    return _result_frame(result)


def _decode_request(frame: bytes, /) -> IdentityDocument:
    document = validate_identity_document(json.loads(frame.decode("utf-8")))
    if (
        document.schema != ENVELOPE_SCHEMA
        or document.schema_version != ENVELOPE_SCHEMA_VERSION
    ):
        raise ValueError("request does not use the importable JSON envelope")
    return document


def _result_frame(result: Jsonable, /) -> bytes:
    envelope = build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        payload={STATUS_KEY: STATUS_OK, RESULT_KEY: result},
    )
    return canonical_identity_json_bytes(envelope) + FRAME_TERMINATOR


def _status_frame(status: str, detail: str, /) -> bytes:
    envelope = build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        payload={STATUS_KEY: status, DETAIL_KEY: detail},
    )
    return canonical_identity_json_bytes(envelope) + FRAME_TERMINATOR


def _read_frame(stream: IO[bytes], /) -> bytes | None:
    """Read one newline-terminated frame, streaming rather than bounding it."""

    chunks: list[bytes] = []
    while True:
        chunk = stream.readline()
        if not chunk:
            return None if not chunks else b"".join(chunks)
        chunks.append(chunk)
        if chunk.endswith(FRAME_TERMINATOR):
            return b"".join(chunks)


def _write_frame(stream: IO[bytes], frame: bytes, /) -> None:
    stream.write(frame)
    stream.flush()


if __name__ == "__main__":
    main()
