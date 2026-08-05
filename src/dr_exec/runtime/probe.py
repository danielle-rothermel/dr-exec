from __future__ import annotations

import json
import subprocess
from pathlib import Path

PROBE_SOURCE = (
    "import json,platform,sys;"
    "print(json.dumps({"
    '"implementation":sys.implementation.name,'
    '"python_version":platform.python_version(),'
    '"cache_tag":sys.implementation.cache_tag,'
    '"platform":sys.platform'
    "}))"
)
PROBE_ARGUMENTS = ("-I", "-c", PROBE_SOURCE)
PROBE_FACT_KEYS = frozenset(
    {"implementation", "python_version", "cache_tag", "platform"}
)


class InterpreterProbeError(RuntimeError):
    """The fixed interpreter probe did not report usable runtime facts."""


def probe_interpreter(executable: Path, /) -> dict[str, str]:
    argv = (str(executable), *PROBE_ARGUMENTS)
    try:
        completed = subprocess.run(
            argv,
            capture_output=True,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise InterpreterProbeError(
            f"interpreter probe failed to run: {executable}"
        ) from error
    if completed.returncode != 0:
        raise InterpreterProbeError(
            f"interpreter probe exited {completed.returncode}: {executable}"
        )
    return _parse_probe_output(completed.stdout, executable=executable)


def _parse_probe_output(
    stdout: bytes,
    /,
    *,
    executable: Path,
) -> dict[str, str]:
    try:
        facts = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InterpreterProbeError(
            f"interpreter probe output is not JSON: {executable}"
        ) from error
    if not isinstance(facts, dict) or set(facts) != PROBE_FACT_KEYS:
        raise InterpreterProbeError(
            f"interpreter probe reported unexpected keys: {executable}"
        )
    if any(
        not isinstance(value, str) or not value for value in facts.values()
    ):
        raise InterpreterProbeError(
            f"interpreter probe reported an unusable fact: {executable}"
        )
    return facts


__all__ = ["InterpreterProbeError", "probe_interpreter"]
