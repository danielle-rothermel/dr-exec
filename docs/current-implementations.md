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
duration_s, ...)` envelope that never raises; partial stdout/stderr
recovered from `TimeoutExpired`; per-instance timeout (default 5 s).
Weaknesses: no output or input bounds, no environment control, no process
group handling. Consumers drive ruff/ty over generated source (stdin or
tmpfile). Adjacent in-process pieces: `validators/compile_check.py`
(`compile()` only, never executed) and `validators/import_resolve.py`
(`find_spec`, which executes parent packages of dotted names).

## symphony-lite

## dr-notion

## dr-dspy

## llmflow

### Bash tool executor (TypeScript)

`packages/runtime/src/tool-executor.ts` — model-issued commands via
`bash -lc` with `cwd`, timeout with a configured ceiling, SIGTERM → 1 s →
SIGKILL (leader only), per-stream output caps. Notable property:
`BASH_TOOL_CONTRACT` (`packages/core/src/tools.ts:84-130`) — tool schema,
approval requirement, timeout defaults/ceilings, and output limit co-located
in one frozen contract object. Weaknesses: `cappedAppend` materializes the
full concatenation before slicing and truncates silently; login-shell
semantics; leader-only kills.

### Codex CLI transport (TypeScript)

`packages/providers/src/codex.ts` — `spawn("codex", ["exec", "--json", ...,
"--sandbox", "read-only", ..., "-"])` with the model-authored prompt on
stdin; env-var timeout; AbortSignal cancellation. Weaknesses: unbounded
stdout accumulation; SIGTERM only, leader only.

## codearc

## dr-queues

### Detached stage-worker spawning

`src/dr_queues/runtime/lifecycle.py:126` (`start_stage_workers`) —
`Popen(cmd, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL,
start_new_session=True)`; command prefix resolved via `shutil.which` for an
installed entry point with `sys.executable -m` fallback; grace-period sleep
then `poll()` as a startup liveness check. A second variant at
`src/dr_queues/pipeline/runner.py:296` spawns one worker per pipeline stage
with plain `Popen` and inherited stdio. Weaknesses: no execution timeout, no
health monitoring past startup, no shutdown/reap path in the spawn module;
output discarded entirely in the detached variant. Tests fake by
monkeypatching `Popen`.

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
`--cap-drop`, `--security-opt=no-new-privileges`), resource limits
(`--memory`, `--cpus`, `--pids-limit`, ulimits for cpu/fsize/nofile/nproc,
with the cpu ulimit derived from `timeout_seconds`), tmpfs and bind mounts,
env injection, workdir. Notable properties: selectors-based concurrent
stdout/stderr reader with per-stream byte caps and explicit truncation
markers; bytes stdin; result envelope `DockerRuntimeResult(ok,
error=ErrorEnvelope(code, retriable))`; container cleanup via cidfile in
`finally` (`docker rm -f`, CID validated against `[0-9a-f]{64}`).
Weaknesses: no `start_new_session`/`killpg` (bare `proc.kill()` on the
docker CLI client), no TERM→KILL escalation, unbounded `proc.wait()` after
kill, writer thread joined without timeout, no argv validation, no
plain-argv (non-Docker) entry point. `cleanup.py:21` is a best-effort
`docker rm -f` with output discarded and no timeout.

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
working tree. Weaknesses: no timeouts anywhere, uncaptured output,
fixed local binary paths. Near-duplicate copies live in
`projects/graphex/scripts/slurmutils/` (and historically in maqa).

### Job babysitting and coreutils-by-shell

`scripts/job_management/hanabi_wandb_eval.py` and neighbors — long-running
babysitter loops that enumerate run state with `sh.ls`, manage checkpoints
with `sh.cp`/`sh.rm`/`sh.mv`/`sh.touch`, poll `squeue` via `check_output`,
`scancel` stale jobs, and resubmit via `sbatch` or generated bash scripts —
all captured-or-inherited, no timeouts. `redis_logger` starts a long-lived
`redis-server` child via fire-and-forget `Popen` with no handle management.

## scripts

### StarCraft-era cluster job management

2018, predecessor of the utils/dr_exp lineage. A shared
`get_bash_output()` stencil (`check_output` on a two-arg argv, captured,
split on newlines) underpins: `job_management/kickoff_rl_runs.py`
(`squeue` polling with a fixed format string; slurm submission whose stdout
is parsed for a "Job directory:" line), `monitoring/rl_run_tracking.py`
(squeue parsing plus gnuplot rendering of run metrics), and gnuplot
invocations for plots. Uniform shape: everything captured, argv lists, no
shell strings, no timeouts. Valued as one of the cleanest expressions of
the poll–submit–monitor cluster loop despite its age.
