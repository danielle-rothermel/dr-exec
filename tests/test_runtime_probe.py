"""Isolated-host runtime: the fixed `-I` probe, prepare, and describe.

Probe tests spawn real interpreters. They synchronize on the probe's
terminal outcome -- a returned record or a raised error -- never on
elapsed time.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from dr_exec import (
    ContainmentProfile,
    IsolatedHostPythonRuntime,
    RuntimeKind,
    UntrustedPythonTarget,
)
from dr_exec._bootstrap import (
    DRIVER_SOURCE_BINDING,
    ISOLATED_INVOCATION_ARGUMENTS,
    driver_wrapper_source,
)
from dr_exec._identity import ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA
from dr_exec._probe import (
    PROBE_ARGUMENTS,
    PROBE_FACT_KEYS,
    InterpreterProbeError,
    probe_interpreter,
)

DRIVER_SOURCE = "def dr_exec_main(request, emit):\n    return None\n"


def _write_executable(path: Path, source: str) -> Path:
    path.write_text(source, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.fixture
def host_runtime() -> IsolatedHostPythonRuntime:
    return IsolatedHostPythonRuntime(Path(sys.executable))


def test_probe_arguments_are_the_fixed_isolated_invocation() -> None:
    assert PROBE_ARGUMENTS[:2] == ("-I", "-c")
    assert ISOLATED_INVOCATION_ARGUMENTS == ("-I", "-c")


def test_probe_reports_exactly_the_runtime_identity_facts() -> None:
    facts = probe_interpreter(Path(sys.executable))
    assert set(facts) == PROBE_FACT_KEYS
    assert facts["implementation"] == sys.implementation.name
    assert facts["cache_tag"] == sys.implementation.cache_tag
    assert facts["platform"] == sys.platform


def test_probe_runs_isolated_from_ambient_python_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`-I` must defeat a sitecustomize planted on PYTHONPATH."""
    (tmp_path / "sitecustomize.py").write_text(
        "raise SystemExit(9)\n", encoding="utf-8"
    )
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    non_isolated = subprocess.run(
        [sys.executable, "-c", PROBE_ARGUMENTS[2]],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert non_isolated.returncode != 0
    assert probe_interpreter(Path(sys.executable))["implementation"] == (
        sys.implementation.name
    )


def test_describe_returns_the_record_retained_at_construction(
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    first = host_runtime.describe()
    assert host_runtime.describe() is first
    assert first.kind is RuntimeKind.ISOLATED_HOST_PYTHON
    assert first.resolved_executable.is_absolute()
    assert first.id_doc.schema == ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA
    assert first.id_doc.schema_version == 1


def test_runtime_identity_binds_the_resolved_executable_path(
    tmp_path: Path,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    """Equal builds at different paths are deliberately distinct runtimes."""
    aliased = tmp_path / "aliased-python"
    aliased.symlink_to(Path(sys.executable).resolve())
    assert IsolatedHostPythonRuntime(aliased).describe() == (
        host_runtime.describe()
    )
    forwarding = _write_executable(
        tmp_path / "forwarding-python",
        f'#!/bin/sh\nexec "{Path(sys.executable).resolve()}" "$@"\n',
    )
    forwarded_record = IsolatedHostPythonRuntime(forwarding).describe()
    assert forwarded_record.resolved_executable == forwarding.resolve()
    assert forwarded_record != host_runtime.describe()


def test_prepare_builds_the_fixed_isolated_command(
    host_runtime: IsolatedHostPythonRuntime,
    request_document: object,
) -> None:
    target = UntrustedPythonTarget(
        driver_source=DRIVER_SOURCE,
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    prepared = host_runtime.prepare(target)
    assert prepared.argv[0] == host_runtime.executable.as_posix()
    assert prepared.argv[1:3] == ISOLATED_INVOCATION_ARGUMENTS
    assert len(prepared.argv) == 4
    assert prepared.request == target.request
    assert prepared.runtime_record == host_runtime.describe()


def test_prepare_embeds_driver_source_as_inert_data(
    host_runtime: IsolatedHostPythonRuntime,
    request_document: object,
) -> None:
    """Quotes, backslashes, and newlines never become wrapper syntax."""
    hostile = "'\"\\\n" + "import os\nos._exit(3)\n"
    target = UntrustedPythonTarget(
        driver_source=hostile,
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    wrapper = host_runtime.prepare(target).argv[3]
    namespace: dict[str, object] = {}
    exec(wrapper, namespace)  # noqa: S102 - the wrapper is library-owned
    assert namespace[DRIVER_SOURCE_BINDING] == hostile


def test_driver_wrapper_rejects_nul_bearing_source() -> None:
    with pytest.raises(ValueError, match="NUL"):
        driver_wrapper_source("x\0y")


def test_construction_rejects_a_missing_executable(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unable to resolve"):
        IsolatedHostPythonRuntime(tmp_path / "absent")


def test_construction_rejects_a_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="regular file"):
        IsolatedHostPythonRuntime(tmp_path)


@pytest.mark.skipif(os.name != "posix", reason="POSIX execute bit")
def test_construction_rejects_a_non_executable_file(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="not executable"):
        IsolatedHostPythonRuntime(plain)


def test_construction_rejects_a_failing_probe(tmp_path: Path) -> None:
    failing = _write_executable(tmp_path / "failing", "#!/bin/sh\nexit 7\n")
    with pytest.raises(InterpreterProbeError, match="exited 7"):
        IsolatedHostPythonRuntime(failing)


def test_construction_rejects_non_json_probe_output(tmp_path: Path) -> None:
    chatty = _write_executable(
        tmp_path / "chatty", "#!/bin/sh\necho not json\n"
    )
    with pytest.raises(InterpreterProbeError, match="not JSON"):
        IsolatedHostPythonRuntime(chatty)


def test_construction_rejects_unexpected_probe_keys(tmp_path: Path) -> None:
    wrong = _write_executable(
        tmp_path / "wrong", '#!/bin/sh\necho \'{"implementation":"x"}\'\n'
    )
    with pytest.raises(InterpreterProbeError, match="unexpected keys"):
        IsolatedHostPythonRuntime(wrong)


def test_construction_rejects_an_empty_probe_fact(tmp_path: Path) -> None:
    empty_fact = _write_executable(
        tmp_path / "empty-fact",
        '#!/bin/sh\necho \'{"implementation":"","python_version":"1",'
        '"cache_tag":"1","platform":"1"}\'\n',
    )
    with pytest.raises(InterpreterProbeError, match="unusable fact"):
        IsolatedHostPythonRuntime(empty_fact)
