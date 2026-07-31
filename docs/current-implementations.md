# Current implementations

Catalog of existing process-execution implementations across the fleet: one
section per repo, one subsection per implementation. Companion to
`current-uses.md` — that doc records the needs dr-exec must serve; this one
records the mechanisms that exist today (anatomy, notable properties,
weaknesses) so design can mine prior art deliberately, including from
implementations that are clearly bad. Repos ordered by most recent PR
activity.

## dr-code

## whetstone-ai

## whetstone-envs

## fchord

## dotfiles

## whetstone-viewer

## genfxn

## dr-platform

## dr-graph

## dr-providers

## dr-subs

## dr-diagram

## unitbench

## nlae

## dr-llm

## nl_latents

## diff-walkthrough

## dr-cognee

## code-eval

### Never-raises subprocess runner

`src/code_eval/subprocess_runner.py` — `SubprocessRunner`, a frozen pydantic
model wrapping `subprocess.run`. Notable properties: `shutil.which`
pre-resolution surfacing a missing tool as `tool_found=False` data rather
than an exception; a `SubprocessResult(ok, timed_out, tool_found,
duration_s, ...)` envelope that never raises. Weaknesses: no environment
control, no process group handling. Consumers drive ruff/ty over generated
source (stdin or tmpfile). Adjacent in-process pieces:
`validators/compile_check.py` (`compile()` only, never executed) and
`validators/import_resolve.py` (`find_spec`, which executes parent packages
of dotted names).

Budget axes — wall-clock only:

- Wall-clock — per-instance `timeout_s` (default 5 s); on expiry, partial
  stdout/stderr are recovered from `TimeoutExpired`.
- Output and input — unbounded.

## symphony-lite

## dr-notion

## dr-dspy

## llmflow

### Bash tool executor (TypeScript)

`packages/runtime/src/tool-executor.ts` — model-issued commands via
`bash -lc` with `cwd`, SIGTERM → 1 s → SIGKILL (leader only). Notable
property: `BASH_TOOL_CONTRACT` (`packages/core/src/tools.ts:84-130`) — tool
schema, approval requirement, timeout defaults/ceilings, and output limit
co-located in one frozen contract object. Weaknesses: login-shell
semantics; leader-only kills.

Budget axes — wall-clock and output, both pinned in the contract object:

- Wall-clock — per-call timeout with a configured ceiling, defaults in
  `BASH_TOOL_CONTRACT`.
- Output — per-stream caps via `cappedAppend`; truncation is silent, and
  the full concatenation is materialized before slicing.
- Termination — 1 s wait between SIGTERM and SIGKILL (leader only).

### Codex CLI transport (TypeScript)

`packages/providers/src/codex.ts` — `spawn("codex", ["exec", "--json", ...,
"--sandbox", "read-only", ..., "-"])` with the model-authored prompt on
stdin; AbortSignal cancellation. Weaknesses: SIGTERM only, leader only.

Budget axes — wall-clock only:

- Wall-clock — `CODEX_COMMAND_TIMEOUT_MS` environment variable.
- Output — unbounded stdout accumulation.
- Input — prompt written to stdin, unbounded.

## codearc

## dr-queues

### Detached stage-worker spawning

`src/dr_queues/runtime/lifecycle.py:126` (`start_stage_workers`) —
`Popen(cmd, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL,
start_new_session=True)`; command prefix resolved via `shutil.which` for an
installed entry point with `sys.executable -m` fallback; grace-period sleep
then `poll()` as a startup liveness check. A second variant at
`src/dr_queues/pipeline/runner.py:296` spawns one worker per pipeline stage
with plain `Popen` and inherited stdio. Weaknesses: no health monitoring
past startup, no shutdown/reap path in the spawn module. Tests fake by
monkeypatching `Popen`.

Budget axes — none: workers run with no execution timeout (the startup
grace-period is a liveness check, not a deadline), and output is discarded
in the detached variant rather than bounded.

## marimo_utils

## nl-code

## dr_exp

## deconCNN

## dr_gen

## dr-docker

### Docker CLI subprocess adapter

`src/dr_docker/subprocess_adapter.py` (362 lines) —
`SubprocessDockerAdapter` builds `docker run` argv from a
`DockerRuntimeRequest`: security profile (`--network=none`, `--read-only`,
`--cap-drop`, `--security-opt=no-new-privileges`), tmpfs and bind mounts,
env injection, workdir. Notable properties: selectors-based concurrent
stdout/stderr reader with explicit truncation markers; bytes stdin; result
envelope `DockerRuntimeResult(ok, error=ErrorEnvelope(code, retriable))`;
container cleanup via cidfile in `finally` (`docker rm -f`, CID validated
against `[0-9a-f]{64}`). Weaknesses: no `start_new_session`/`killpg` (bare
`proc.kill()` on the docker CLI client), no TERM→KILL escalation, writer
thread joined without timeout, no argv validation, no plain-argv
(non-Docker) entry point. `cleanup.py:21` is a best-effort `docker rm -f`
with output discarded and no timeout.

Budget axes — the fleet's widest set, mostly containment-backed:

- Wall-clock — `timeout_seconds` on the docker CLI client;
  `TimeoutExpired` → kill.
- CPU time — cpu ulimit, derived from `timeout_seconds`.
- Memory — `--memory` (plus `--cpus` rate limiting).
- Processes — `--pids-limit` and the nproc ulimit.
- File size / open files — fsize and nofile ulimits.
- Output — per-stream byte caps with explicit truncation markers.
- Input — bytes stdin, unbounded.
- Termination — unbounded: `proc.wait()` after kill has no timeout.

## dr-util

## parse_claude

## utils

### Slurm snapshot workflow

`slurm_tools/` — cluster runs execute from immutable workspace snapshots:
`workspace.py` computes a git-described snapshot name and exclusion list
(`git describe`/`status`/`ls-files` via `Popen(stdout=PIPE)`, streamed
line-by-line), `squashfs.py` packs it with `mksquashfs` via `check_call`,
`job.py` symlinks images into job dirs (`ln -sf`), submission is
`sbatch --chdir <jobdir>` via `check_call`, and
`scripts/run_from_snapshot.py` unpacks with `unsquashfs` then `check_call`s
an arbitrary argv inside the unpacked workspace. Notable property: the
snapshot-then-execute pattern gives cluster jobs a hermetic-by-construction
working tree. Weaknesses: fixed local binary paths. Near-duplicate copies
live in `projects/graphex/scripts/slurmutils/` (and historically in maqa).

Budget axes — none: no timeouts anywhere; output is uncaptured
(inherited), never bounded.

### Job babysitting and coreutils-by-shell

`scripts/job_management/hanabi_wandb_eval.py` and neighbors — long-running
babysitter loops that enumerate run state with `sh.ls`, manage checkpoints
with `sh.cp`/`sh.rm`/`sh.mv`/`sh.touch`, poll `squeue` via `check_output`,
`scancel` stale jobs, and resubmit via `sbatch` or generated bash scripts.
`redis_logger` starts a long-lived `redis-server` child via fire-and-forget
`Popen` with no handle management.

Budget axes — none: polling loops and coreutils calls run without timeouts
or output bounds, captured or inherited.

## scripts

### StarCraft-era cluster job management

2018, predecessor of the utils/dr_exp lineage. A shared
`get_bash_output()` stencil (`check_output` on a two-arg argv, captured,
split on newlines) underpins: `job_management/kickoff_rl_runs.py`
(`squeue` polling with a fixed format string; slurm submission whose stdout
is parsed for a "Job directory:" line), `monitoring/rl_run_tracking.py`
(squeue parsing plus gnuplot rendering of run metrics), and gnuplot
invocations for plots. Uniform shape: argv lists, no shell strings. Valued
as one of the cleanest expressions of the poll–submit–monitor cluster loop
despite its age.

Budget axes — none: everything captured with no timeouts and no output
bounds.
