from __future__ import annotations

import os
import platform
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from dr_serialize import IdentityDocument, build_identity_document
from pydantic import ValidationError

from dr_exec import (
    ContainmentProfile,
    IsolatedHostPythonRuntime,
    RuntimeKind,
    RuntimeRecord,
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.runtime import host
from dr_exec.runtime.bootstrap import (
    DRIVER_SOURCE_BINDING,
    ISOLATED_INVOCATION_ARGUMENTS,
)
from dr_exec.runtime.identity import ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA
from dr_exec.runtime.probe import (
    PROBE_ARGUMENTS,
    PROBE_FACT_KEYS,
    InterpreterProbeError,
    _parse_probe_output,
    probe_interpreter,
)

pytestmark = [pytest.mark.integration, pytest.mark.subprocess]

DRIVER_SOURCE = "def dr_exec_main(request, emit):\n    return None\n"


def _write_executable(path: Path, source: str) -> Path:
    path.write_text(source, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.fixture(scope="module")
def host_runtime() -> IsolatedHostPythonRuntime:
    return IsolatedHostPythonRuntime(Path(sys.executable))


def test_probe_arguments_are_the_fixed_isolated_invocation() -> None:
    assert PROBE_ARGUMENTS[:2] == ("-I", "-c")
    assert ISOLATED_INVOCATION_ARGUMENTS == ("-I", "-c")


def test_probe_reports_exactly_the_runtime_identity_facts() -> None:
    facts = probe_interpreter(Path(sys.executable))
    assert set(facts) == PROBE_FACT_KEYS
    assert facts["implementation"] == sys.implementation.name
    assert facts["python_version"] == platform.python_version()
    assert facts["cache_tag"] == sys.implementation.cache_tag
    assert facts["platform"] == sys.platform


def test_probe_runs_isolated_from_ambient_python_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_runtime_identity_maps_each_fact_from_the_probed_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    facts = {
        "implementation": "implementation-sentinel",
        "python_version": "python-version-sentinel",
        "cache_tag": "cache-tag-sentinel",
        "platform": "platform-sentinel",
    }
    monkeypatch.setattr(host, "probe_interpreter", lambda _: facts)

    runtime = IsolatedHostPythonRuntime(Path(sys.executable))
    record = runtime.describe()
    resolved = Path(sys.executable).resolve()

    assert record.kind is RuntimeKind.ISOLATED_HOST_PYTHON
    assert record.resolved_executable == resolved
    assert record.id_doc.payload == {
        "kind": "isolated_host_python",
        "resolved_executable": resolved.as_posix(),
        "implementation": "implementation-sentinel",
        "python_version": "python-version-sentinel",
        "cache_tag": "cache-tag-sentinel",
        "platform": "platform-sentinel",
    }


def test_runtime_record_rejects_a_resolved_path_identity_mismatch() -> None:
    id_doc = build_identity_document(
        schema="dr_exec.isolated_host_python_runtime",
        schema_version=1,
        payload={
            "kind": "isolated_host_python",
            "resolved_executable": "/opt/py/bin/python3.13",
            "implementation": "cpython",
            "python_version": "3.13.2",
            "cache_tag": "cpython-313",
            "platform": "darwin",
        },
    )

    with pytest.raises(ValidationError, match="does not match identity"):
        RuntimeRecord(
            kind=RuntimeKind.ISOLATED_HOST_PYTHON,
            resolved_executable=Path("/opt/py/bin/python3.12"),
            id_doc=id_doc,
        )


def test_runtime_identity_binds_the_resolved_executable_path(
    tmp_path: Path,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
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
    request_document: IdentityDocument,
) -> None:
    targets = (
        TrustedPythonTarget(
            driver_source=DRIVER_SOURCE,
            request=request_document,
        ),
        UntrustedPythonTarget(
            driver_source=DRIVER_SOURCE,
            request=request_document,
            containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
        ),
    )

    prepared = tuple(host_runtime.prepare(target) for target in targets)

    assert prepared[0] == prepared[1]
    assert prepared[0].argv[0] == host_runtime.executable.as_posix()
    assert prepared[0].argv[1:3] == ISOLATED_INVOCATION_ARGUMENTS
    assert len(prepared[0].argv) == 4
    assert prepared[0].runtime_record == host_runtime.describe()


def test_prepare_canonicalizes_and_hashes_the_request_once(
    host_runtime: IsolatedHostPythonRuntime,
    request_document: IdentityDocument,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transported = b'{"canonical":"request"}'
    calls: list[IdentityDocument] = []

    def capture(request: IdentityDocument) -> bytes:
        calls.append(request)
        return transported

    monkeypatch.setattr(host, "request_transport_bytes", capture)
    prepared = host_runtime.prepare(
        TrustedPythonTarget(
            driver_source=DRIVER_SOURCE,
            request=request_document,
        )
    )

    assert calls == [request_document]
    assert prepared.request_bytes == transported
    assert prepared.request_id_sha256 == (
        "1d82e4d717aa597e04f89d4b684cc3ca57ef2d80ba82cfa95576b60b41ad7eda"
    )


def test_prepare_and_repeated_describe_do_not_reprobe(
    host_runtime: IsolatedHostPythonRuntime,
    request_document: IdentityDocument,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    def _forbidden(_: Path) -> dict[str, str]:
        raise AssertionError("prepare and describe must not re-probe")

    target = UntrustedPythonTarget(
        driver_source=DRIVER_SOURCE,
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    monkeypatch.setattr(host, "probe_interpreter", _forbidden)

    assert host_runtime.prepare(target).runtime_record == (
        host_runtime.describe()
    )
    assert host_runtime.describe() is host_runtime.describe()


def test_prepare_embeds_driver_source_as_inert_data(
    host_runtime: IsolatedHostPythonRuntime,
    request_document: IdentityDocument,
) -> None:
    hostile = "'\"\\\n" + "import os\nos._exit(3)\n"
    target = UntrustedPythonTarget(
        driver_source=hostile,
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    wrapper = host_runtime.prepare(target).argv[3]
    binding, _, body = wrapper.partition("\n")
    namespace: dict[str, object] = {}
    exec(binding, namespace)  # noqa: S102 - the binding is under test
    assert namespace[DRIVER_SOURCE_BINDING] == hostile
    assert "os._exit(3)" not in body


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


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        pytest.param(b"not json", "not JSON", id="malformed-json"),
        pytest.param(b"\xff", "not JSON", id="invalid-utf8"),
        pytest.param(b"[]", "unexpected keys", id="non-object"),
        pytest.param(
            b'{"implementation":"x"}',
            "unexpected keys",
            id="missing-keys",
        ),
        pytest.param(
            b'{"cache_tag":"x","extra":"x","implementation":"x",'
            b'"platform":"x","python_version":"x"}',
            "unexpected keys",
            id="extra-key",
        ),
        pytest.param(
            b'{"cache_tag":"x","implementation":1,"platform":"x",'
            b'"python_version":"x"}',
            "unusable fact",
            id="non-string-fact",
        ),
        pytest.param(
            b'{"cache_tag":"x","implementation":"","platform":"x",'
            b'"python_version":"x"}',
            "unusable fact",
            id="empty-fact",
        ),
    ],
)
def test_probe_rejects_invalid_reported_facts(
    stdout: bytes,
    message: str,
) -> None:
    with pytest.raises(InterpreterProbeError, match=message):
        _parse_probe_output(stdout, executable=Path(sys.executable))
