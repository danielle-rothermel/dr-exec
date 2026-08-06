#!/usr/bin/env bash

set -euo pipefail

script_directory="$({
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
    pwd -P
})"
repository_root="$({
    cd -- "${script_directory}/.."
    pwd -P
})"

temporary_root=""
temporary_parent=""

cleanup() {
    local cleanup_target="${temporary_root:-}"

    if [[ -z "${cleanup_target}" ]]; then
        return 0
    fi
    if [[ ! -d "${cleanup_target}" || -L "${cleanup_target}" ]]; then
        printf 'Refusing to clean invalid temporary directory: %s\n' \
            "${cleanup_target}" >&2
        return 1
    fi
    if [[ "$(dirname -- "${cleanup_target}")" != "${temporary_parent}" ]]; then
        printf 'Refusing to clean temporary directory outside its parent: %s\n' \
            "${cleanup_target}" >&2
        return 1
    fi
    if [[ "$(basename -- "${cleanup_target}")" != dr-exec-pre-check.* ]]; then
        printf 'Refusing to clean unexpected temporary directory: %s\n' \
            "${cleanup_target}" >&2
        return 1
    fi

    rm -rf -- "${cleanup_target:?}"
}

on_exit() {
    local exit_status=$?

    trap - EXIT
    if ! cleanup && [[ "${exit_status}" -eq 0 ]]; then
        exit_status=1
    fi
    exit "${exit_status}"
}

trap on_exit EXIT

cd -- "${repository_root}"

if [[ "$#" -gt 1 ]]; then
    printf 'Usage: %s [dist]\n' "$0" >&2
    exit 2
fi
publish_artifacts="${1:-}"
if [[ -n "${publish_artifacts}" && "${publish_artifacts}" != "dist" ]]; then
    printf 'Usage: %s [dist]\n' "$0" >&2
    exit 2
fi

uv sync --locked --no-sources
uv run ruff format --check .
uv run ruff check .
uv run ty check
uvx tombi@1.2.5 lint --offline .defs/terms.toml
uv run python scripts/check_defs.py
uv run pytest -q

temporary_base="${TMPDIR:-/tmp}"
temporary_base="$({
    cd -- "${temporary_base}"
    pwd -P
})"
temporary_parent="${temporary_base}"
temporary_root="$(mktemp -d "${temporary_base}/dr-exec-pre-check.XXXXXXXX")"
temporary_root="$({
    cd -- "${temporary_root}"
    pwd -P
})"

if [[ "$(dirname -- "${temporary_root}")" != "${temporary_parent}" \
    || "$(basename -- "${temporary_root}")" != dr-exec-pre-check.* ]]; then
    printf 'mktemp returned an unexpected directory: %s\n' \
        "${temporary_root}" >&2
    exit 1
fi

requirements_file="${temporary_root}/requirements.txt"
wheel_environment="${temporary_root}/wheel-environment"
if [[ -z "${publish_artifacts}" ]]; then
    artifact_directory="${temporary_root}/dist"
else
    artifact_directory="${repository_root}/dist"
    if [[ -e "${artifact_directory}" || -L "${artifact_directory}" ]]; then
        printf 'Refusing to overwrite existing artifact path: %s\n' \
            "${artifact_directory}" >&2
        exit 1
    fi
fi
mkdir -- "${artifact_directory}"
uv build --out-dir "${artifact_directory}"
uv run python scripts/check_distribution.py "${artifact_directory}"

uv export --quiet --locked --no-sources --no-dev --no-emit-project \
    --output-file "${requirements_file}"

project_python="$(uv run --no-sync python -I -c 'import sys; print(sys.executable)')"
if [[ ! -x "${project_python}" ]]; then
    printf 'Project interpreter is not executable: %s\n' \
        "${project_python}" >&2
    exit 1
fi

uv venv --python "${project_python}" "${wheel_environment}"
wheel_python="${wheel_environment}/bin/python"
if [[ ! -x "${wheel_python}" ]]; then
    printf 'Wheel-test interpreter is not executable: %s\n' \
        "${wheel_python}" >&2
    exit 1
fi

shopt -s nullglob
wheels=("${artifact_directory}"/*.whl)
shopt -u nullglob
if [[ "${#wheels[@]}" -ne 1 ]]; then
    printf 'Expected one wheel; found %d.\n' "${#wheels[@]}" >&2
    exit 1
fi

uv pip install --python "${wheel_python}" \
    --requirement "${requirements_file}"
uv pip install --python "${wheel_python}" --no-deps "${wheels[0]}"

(
    cd -- "${wheel_environment}"
    "${wheel_python}" -I \
        "${repository_root}/scripts/check_built_install.py" \
        "${repository_root}"
)
