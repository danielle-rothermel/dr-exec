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

uv sync --locked
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

artifact_directory="${temporary_root}/dist"
mkdir -- "${artifact_directory}"
uv build --out-dir "${artifact_directory}"
uv run python scripts/check_distribution.py "${artifact_directory}"
