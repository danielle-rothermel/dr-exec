#!/usr/bin/env bash

set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
CACHE_DIR="${REPO_ROOT}/.cache/pre-check"

cd "${REPO_ROOT}"

mkdir -p "${CACHE_DIR}"

contains_path() {
    local candidate="$1"
    shift

    local path
    for path in "$@"; do
        if [[ "${path}" == "${candidate}" ]]; then
            return 0
        fi
    done

    return 1
}

# Bash 3.2 treats an empty array expansion as unset under `set -u`.
fixer_paths=("")
if ! git diff --cached --name-only --diff-filter=ACMR -z \
    >"${CACHE_DIR}/staged-paths.bin"; then
    printf 'Failed to inspect staged paths.\n'
    exit 1
fi
while IFS= read -r -d '' path; do
    case "${path}" in
        src/*.py | src/*.pyi | scripts/*.py | scripts/*.pyi)
            fixer_paths+=("${path}")
            ;;
    esac
done <"${CACHE_DIR}/staged-paths.bin"

unstaged_paths=("")
if ! git diff --name-only -z >"${CACHE_DIR}/unstaged-paths.bin"; then
    printf 'Failed to inspect unstaged paths.\n'
    exit 1
fi
while IFS= read -r -d '' path; do
    unstaged_paths+=("${path}")
done <"${CACHE_DIR}/unstaged-paths.bin"

for path in "${fixer_paths[@]:1}"; do
    if contains_path "${path}" "${unstaged_paths[@]}"; then
        printf 'Cannot safely autofix a partially staged file:\n'
        printf '  %s\n' "${path}"
        printf 'Stage or restore its remaining changes, then retry.\n'
        exit 1
    fi
done

run_silent() {
    local name="$1"
    local output_file="$2"
    shift 2

    printf '  %s\n' "${name}"
    "$@" >"${output_file}" 2>&1
}

run_report() {
    local name="$1"
    local output_file="$2"
    shift 2

    printf '\n==> %s\n' "${name}"
    "$@" 2>&1 | tee "${output_file}"
    return "${PIPESTATUS[0]}"
}

autofix_status=0

run_autofix() {
    local name="$1"
    local output_file="$2"
    shift 2

    run_silent "${name}" "${output_file}" "$@"
    local command_status=$?

    if [[ "${command_status}" -gt 1 ]]; then
        printf '\nAutofix command failed; see:\n'
        printf '  %s\n' "${output_file}"
        autofix_status=1
    fi
}

stage_autofixes() {
    local path
    local status=0

    for path in "${fixer_paths[@]:1}"; do
        git add -- "${path}" || status=1
    done

    return "${status}"
}

printf 'Running quiet autofixes...\n'

if [[ "${#fixer_paths[@]}" -gt 1 ]]; then
    run_autofix "ruff check --fix" "${CACHE_DIR}/ruff-check-fix.txt" \
        uv run ruff check --fix "${fixer_paths[@]:1}"
    run_autofix "ty check --fix" "${CACHE_DIR}/ty-check-fix.txt" \
        uv run ty check --fix "${fixer_paths[@]:1}"
    run_autofix "ruff format" "${CACHE_DIR}/ruff-format.txt" \
        uv run ruff format "${fixer_paths[@]:1}"
else
    printf '  no staged Python files\n'
fi

if ! stage_autofixes; then
    printf '\nFailed to stage autofixes.\n'
    exit 1
fi

printf '\nRunning final checks...\n'

status="${autofix_status}"

if [[ "${#fixer_paths[@]}" -gt 1 ]]; then
    run_report "ruff check" "${CACHE_DIR}/ruff-check.txt" \
        uv run ruff check "${fixer_paths[@]:1}" || status=1
    run_report "ruff format --check" "${CACHE_DIR}/ruff-format-check.txt" \
        uv run ruff format --check "${fixer_paths[@]:1}" || status=1
    run_report "ty check" "${CACHE_DIR}/ty-check.txt" \
        uv run ty check "${fixer_paths[@]:1}" || status=1

    printf '\nCheck output files:\n'
    printf '  %s\n' "${CACHE_DIR}/ruff-check.txt"
    printf '  %s\n' "${CACHE_DIR}/ruff-format-check.txt"
    printf '  %s\n' "${CACHE_DIR}/ty-check.txt"
else
    printf '  no staged Python files\n'
fi

if [[ "${status}" -ne 0 ]]; then
    printf '\nFix all reported issues, then rerun:\n'
    printf '  scripts/pre-check.sh\n'
fi

exit "${status}"
