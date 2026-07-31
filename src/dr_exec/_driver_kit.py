"""The executor's agent inside a batch child. Stdlib only, never imported.

This module's *source text* is composed with the caller's driver body and
delivered to a hermetic interpreter as one argv-carried program, so it runs
under ``-I`` with no packages beyond the standard library and no ``dr_exec``
on ``sys.path``. Importing it in the parent would be meaningless: the names
here exist to be read as text by :mod:`dr_exec.batch`.

The body the composed program expects supplies ``run_item(item_id, payload)``
returning a JSON-able mapping. Everything else — protocol handle capture,
prelude emission, per-item failure capture, terminal line — is the kit's.
"""

from __future__ import annotations

KIT_SOURCE = """\
import json as _json
import os as _os
import sys as _sys
import traceback as _traceback

_PROTOCOL_HANDLE = _os.fdopen(_os.dup(1), "w", encoding="utf-8", newline="\\n")
_sys.stdout = _sys.stderr
_sys.__stdout__ = _sys.stderr

_PRELUDE = _json.loads(_KIT_PRELUDE_JSON)
_ITEMS = _json.loads(_KIT_ITEMS_JSON)


def _kit_clip(text, limit):
    text = str(text)
    if len(text) > limit:
        return text[:limit] + _KIT_CLIP_MARKER
    return text


def _kit_emit(line):
    _PROTOCOL_HANDLE.write(_json.dumps(line, separators=(",", ":")))
    _PROTOCOL_HANDLE.write("\\n")
    _PROTOCOL_HANDLE.flush()


def _kit_result_line(item_id, payload):
    return {
        _KIT_KEY_KIND: _KIT_KIND_RESULT,
        _KIT_KEY_ITEM_ID: item_id,
        _KIT_KEY_PAYLOAD: payload,
    }


def _kit_error_payload(text):
    return {_KIT_KEY_ERROR: _kit_clip(text, _KIT_RESULT_BOUND)}


_kit_emit(_PRELUDE)

try:
    _KIT_LOAD_ERROR = None
    _kit_run_item = None
    exec(_KIT_BODY_SOURCE, globals())
    _kit_run_item = globals().get(_KIT_BODY_HOOK_NAME)
    if _kit_run_item is None:
        raise NameError(
            "driver body defines no " + _KIT_BODY_HOOK_NAME + " hook"
        )
except BaseException:
    _KIT_LOAD_ERROR = _traceback.format_exc()

if _KIT_LOAD_ERROR is not None:
    for _kit_item in _ITEMS:
        _kit_emit(
            _kit_result_line(
                _kit_item[_KIT_KEY_ITEM_ID], _kit_error_payload(_KIT_LOAD_ERROR)
            )
        )
else:
    for _kit_item in _ITEMS:
        _kit_item_id = _kit_item[_KIT_KEY_ITEM_ID]
        try:
            _kit_payload = _kit_run_item(_kit_item_id, _kit_item[_KIT_KEY_PAYLOAD])
            _kit_line = _kit_result_line(_kit_item_id, _kit_payload)
            _kit_rendered = _json.dumps(_kit_line, separators=(",", ":"))
        except BaseException:
            _kit_line = _kit_result_line(
                _kit_item_id, _kit_error_payload(_traceback.format_exc())
            )
            _kit_rendered = _json.dumps(_kit_line, separators=(",", ":"))
        if len(_kit_rendered.encode("utf-8")) > _KIT_RESULT_BOUND:
            _kit_line = _kit_result_line(
                _kit_item_id,
                _kit_error_payload(
                    "result of "
                    + str(len(_kit_rendered.encode("utf-8")))
                    + " bytes exceeds the "
                    + str(_KIT_RESULT_BOUND)
                    + "-byte per-item protocol result budget"
                ),
            )
        _kit_emit(_kit_line)

_kit_emit({_KIT_KEY_KIND: _KIT_KIND_COMPLETE, _KIT_KEY_RESULTS_EMITTED: len(_ITEMS)})
raise SystemExit(0)
"""
"""The kit body, parameterized by the ``_KIT_*`` names the preamble binds.

The private protocol handle is captured from ``os.dup(1)`` *before*
``sys.stdout`` is reassigned, so payload prints land on the payload stream
and can never interleave with protocol lines. The fd-level hole — a payload
writing to fd 1 directly — is a declared limit of the containment profile,
not something the kit can close.

Every line is flushed as it is produced: a result once emitted survives any
later death of the child.
"""
