from __future__ import annotations

import tarfile
import tomllib
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path
from sys import argv

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _project_version() -> str:
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as pyproject_file:
        project = tomllib.load(pyproject_file)["project"]
    version = project.get("version")
    if not isinstance(version, str):
        raise TypeError("pyproject project.version must be a string")
    return version


def _only_artifact(directory: Path, pattern: str) -> Path:
    matches = tuple(directory.glob(pattern))
    if len(matches) != 1:
        raise ValueError(
            f"expected one {pattern!r} artifact in {directory}, "
            f"found {len(matches)}"
        )
    return matches[0]


def validate_wheel(wheel_path: Path, *, version: str) -> None:
    distribution = f"dr_exec-{version}"
    metadata_path = f"{distribution}.dist-info/METADATA"
    license_path = f"{distribution}.dist-info/licenses/LICENSE"

    with zipfile.ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        required_names = {
            "dr_exec/py.typed",
            metadata_path,
            license_path,
        }
        missing_names = required_names - names
        if missing_names:
            raise ValueError(
                f"{wheel_path}: missing files: {sorted(missing_names)!r}"
            )
        if any("/__pycache__/" in name or "/tests/" in name for name in names):
            raise ValueError(f"{wheel_path}: contains development-only files")

        metadata = BytesParser(policy=policy.default).parsebytes(
            wheel.read(metadata_path)
        )
        if metadata["Version"] != version:
            raise ValueError(f"{wheel_path}: metadata version does not match")
        if metadata["License-Expression"] != "MIT":
            raise ValueError(f"{wheel_path}: missing MIT license expression")
        if "LICENSE" not in (metadata.get_all("License-File") or []):
            raise ValueError(f"{wheel_path}: missing License-File metadata")


def validate_sdist(sdist_path: Path, *, version: str) -> None:
    prefix = f"dr_exec-{version}/"
    with tarfile.open(sdist_path, mode="r:gz") as source_distribution:
        names = set(source_distribution.getnames())
    if f"{prefix}LICENSE" not in names:
        raise ValueError(f"{sdist_path}: missing LICENSE")
    if f"{prefix}src/dr_exec/py.typed" not in names:
        raise ValueError(f"{sdist_path}: missing py.typed")
    if any(
        "/__pycache__/" in name or f"{prefix}tests/" in name for name in names
    ):
        raise ValueError(f"{sdist_path}: contains development-only files")


def main() -> None:
    if len(argv) != 2:
        raise SystemExit("usage: check_distribution.py ARTIFACT_DIRECTORY")

    artifact_directory = Path(argv[1]).resolve(strict=True)
    if not artifact_directory.is_dir():
        raise ValueError(f"not an artifact directory: {artifact_directory}")

    version = _project_version()
    wheel_path = _only_artifact(artifact_directory, "*.whl")
    sdist_path = _only_artifact(artifact_directory, "*.tar.gz")
    validate_wheel(wheel_path, version=version)
    validate_sdist(sdist_path, version=version)
    print(f"Validated wheel and source distribution for dr-exec {version}.")


if __name__ == "__main__":
    main()
