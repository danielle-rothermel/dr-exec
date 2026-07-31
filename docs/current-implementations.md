# Current implementations

Catalog of existing process-execution implementations across the fleet: one
section per repo, one subsection per implementation. Companion to
`current-uses.md` — that doc records the needs dr-exec must serve; this one
records the mechanisms that exist today (anatomy, notable properties,
weaknesses) so design can mine prior art deliberately, including from
implementations that are clearly bad. Repos ordered by most recent PR
activity.

## dr-code

### Bounded subprocess primitive

`src/dr_code/execution/subprocess.py` (345 lines, branch
`rebuild/01-subprocess-execution`) — the reference call-scoped runner:
`run_subprocess` (argv) and `run_python_subprocess`
(`sys.executable -I -c <source>` with the minimal replacement environment
`{"OPENBLAS_NUM_THREADS": "1"}`). Anatomy: argv validation (nonempty string
sequence, no NULs, never a shell); environment inherited (`None`) or full
validated replacement; `Popen` with `start_new_session=True` plus three
daemon IPC threads (stdin writer, two bounded stream readers sharing a
lock and overflow event); a monotonic-deadline poll loop; on every exit
path `os.killpg(SIGKILL)` with a completion-race retry after reaping the
leader. Typed errors: `SubprocessError` → `Timeout` / `OutputLimit` /
`Infrastructure` (→ `Start`); nonzero exit never raises — the raw
returncode (including negative signal values) returns in a frozen
`SubprocessCompletedProcess(returncode, stdout, stderr)`; text-only, UTF-8
decode with `errors="replace"`. Injection seam: `PythonSubprocessRunner`
Protocol — with two near-identical ~25-line test doubles at
`tests/metrics/helpers.py:153` and
`tests/humaneval/test_humaneval_primitives.py:167` (unbounded, no group
kill). Notable properties: the only fleet implementation combining session
isolation, whole-group kill, and a bounded termination wait; the
completion-race retry on group signaling. Weaknesses: no cwd, env overlay,
bytes mode, per-stream caps, truncate mode, streaming or passthrough,
optional stdin, or absence pre-check (a missing program surfaces as
`SubprocessStartError`).

Budget axes — wall-clock, output, input, and termination; all except
wall-clock are module constants rather than caller-declared:

- Wall-clock — required, finite, positive `timeout_seconds` per call (the
  one caller-declared budget); enforced by a 10 ms poll loop; expiry →
  `SubprocessTimeoutError`.
- Output — 1 MiB shared across stdout and stderr; overflow aborts the run →
  `SubprocessOutputLimitError`; no truncate mode.
- Input — 4 MiB, validated before spawn; oversized input is rejected
  without spawning.
- Termination — 5 s bounded wait for group death and 1 s bounded IPC-thread
  joins; exceeding either → `SubprocessInfrastructureError`.

Observability — none: no narration of any lifecycle step, no durable
record; the run result is in-memory only and dies with the caller. Channel
separation is trivially safe (the executor emits nothing). A run that hangs
inside its deadline is indistinguishable from one making progress.

### HumanEval batch protocol machinery

`src/dr_code/humaneval/batch_runner.py` plus `batch_runner_script.py` — the
adapter/driver pair over the primitive. Parent side: builds a JSON payload
(candidate code, support code, checks) for stdin and interprets completion
through the protocol: `CANDIDATE_KILL_RETURNCODES` ({-SIGKILL, -SIGSEGV}) →
candidate-attributed error; timeout → every case TIMEOUT; output limit →
every case ERROR; other nonzero exit, JSON parse failure, shape violation,
or unknown/duplicate case IDs → `EvaluationHarnessError` carrying partial
results. Child side: shipped as resource text and never imported; reads one
JSON value from stdin; reassigns `sys.stdout` (and `__stdout__`) to stderr
while keeping a private handle for protocol output, so candidate prints
cannot corrupt the protocol; per-case `exec` with clipped tracebacks;
load-phase failure emits error-for-every-case and exits 0. Notable
properties: exit-code-based payload-versus-executor attribution;
partial-results-through-exceptions; stdout-protocol protection.

Budget axes — inherits the primitive's budgets wholesale; adds one protocol
budget:

- Result fields — per-field traceback clipping at 8000 chars
  (`FIELD_LIMIT`) in the child.

Observability — no narration and no durable record, but two strong partial
properties: channel separation is the design's centerpiece (the child
reassigns `sys.stdout` to stderr so candidate prints cannot corrupt
protocol output), and per-case `elapsed_seconds` measurements plus
partial-results-through-exceptions preserve what happened in-memory even on
failure. Nothing survives the calling process.

## whetstone-ai

### Codex exec transports

Two sibling shapes for running `codex exec` with model-authored prompts.
`src/whetstone/optimization/codex_runner.py:256` — codex wired to an MCP
server spawned as `sys.executable -m whetstone.optimization.mcp_server`
(which imports its evaluator via `importlib.import_module` on the
`WS_MCP_EVALUATOR` env var): `stdin=DEVNULL`, environment `{**os.environ}`
plus MCP config vars (blanket inherit with overlay), `shutil.which`
pre-check, `check=False` with nonzero → `OpaqueStepError`.
`src/whetstone/optimization/codex_proposer.py:146` — `codex exec
--skip-git-repo-check -s read-only --output-last-message <tmpfile>` with
`cwd=`: the result is read from the temp file rather than stdout (stdout is
a JSONL event stream too noisy to parse), and `TimeoutExpired` is caught
into a typed `CodexInvocation(text="", returncode=-1, timed_out=True)`
rather than raised. Notable properties: the file-delivered result as an
early spill-to-disk instance; the proposer's never-raises timeout envelope;
sandbox posture pinned in argv (`-s read-only`). Testing seam: no
injection — tests fake the codex CLI itself with `#!/bin/sh` stub scripts
on PATH, including a `sleep 5` stub to force the timeout path
(`tests/optimization/test_codex_proposer.py`).

Budget axes — wall-clock only, leader-only enforcement:

- Wall-clock — runner 600 s; proposer per-call timeout (caught into the
  typed result); live smoke test 30 s. `subprocess.run`'s timeout kills
  only the direct child.
- Output — unbounded capture; the runner slices the JSONL stream to its
  last 2000 chars after completion (post-hoc selection, not a cap); the
  proposer's file delivery is disk-backed and unbounded.
- Termination — none: no process-group handling, no escalation.

Observability — the child's own JSONL event stream is a live progress
narration that the runner captures but never consumes during the run: it is
inspected only post-hoc, sliced to the last 2000 chars, so in-flight
progress is effectively discarded. No executor narration, no durable
record (the proposer's result tmpfile is transient).

### HumanEval oracle driver (broken)

`src/whetstone/envs/ed1m_oracle.py` and `envs/ed1_scoring.py` — a driver
program (`_DRIVER_SOURCE`, :46-63) that reads a JSON request from stdin,
`exec`s a model-produced reconstruction, and writes single-line JSON to
stdout, intended to run under dr-code's bounded primitive. Currently
nonfunctional twice over: both files import
`dr_code.humaneval.subprocess_runner`, a module path that no longer exists
(moved to `dr_code.execution.subprocess`), `dr_code` is not installed in
the repo's venv, and the call site passes `input_json=` where the current
signature takes `input_text=`. Notable as prior art for the batch-driver
protocol shape, and as the fleet's clearest example of cross-repo drift
against an unpinned execution dependency.

Budget axes — none of its own: intended to inherit the dr-code primitive's
budgets wholesale; the driver protocol itself imposes no field or result
bounds.

Observability — none: the single-line JSON response is the only artifact,
and the breakage itself demonstrates the cost — nothing recorded the drift
until the call site failed.

### Docker availability probe

`src/whetstone/runner/execution_mode.py:106` — `docker info` in bytes mode,
`shutil.which` pre-check, returncode collapsed to a boolean. Budget axes:
wall-clock 10 s; nothing else applicable. Observability: the boolean
collapse discards all diagnostic detail — a missing daemon, a permission
error, and a hung docker all report identically.

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

Observability — no narration and no durable record, but the result
envelope is diagnosis-friendly: `tool_found`, `timed_out`, `duration_s`,
and timeout-recovered partial output all arrive as data, so a failed run
explains itself in-memory. Nothing persists.

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

Observability — the silent `cappedAppend` truncation is the section's
anti-pattern: information loss with no marker, so a capped transcript is
indistinguishable from a complete one. No executor narration or durable
record in the executor itself; approval-before-execution surfaces each
command to the operator ahead of time, which is a form of pre-execution
visibility.

### Codex CLI transport (TypeScript)

`packages/providers/src/codex.ts` — `spawn("codex", ["exec", "--json", ...,
"--sandbox", "read-only", ..., "-"])` with the model-authored prompt on
stdin; AbortSignal cancellation. Weaknesses: SIGTERM only, leader only.

Budget axes — wall-clock only:

- Wall-clock — `CODEX_COMMAND_TIMEOUT_MS` environment variable.
- Output — unbounded stdout accumulation.
- Input — prompt written to stdin, unbounded.

Observability — `--json` gives a structured event stream, but it is
accumulated rather than surfaced live; no narration, no durable record.

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

Observability — the anti-observable extreme: the detached variant routes
all three stdio streams to DEVNULL, so a worker's entire life is invisible
by construction; the startup `poll()` is the only progress signal that will
ever exist. No narration, no record, nothing to consult when a worker
dies.

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

Observability — the strongest marked-loss story in the fleet: truncation
carries explicit markers stating how many bytes were dropped and the cap in
force, so a capped stream is never mistaken for a complete one. The
cidfile is a durable artifact, but only for cleanup — no run record. A
module logger exists; lifecycle narration is minimal.

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

Observability — inherited stdio makes live progress fully visible on the
operator's terminal (stdio passthrough doing narration duty), and the
snapshot name embeds `git describe` output — a durable provenance record
of exactly what workspace state each cluster job ran from. Nothing records
the execution itself.

### Job babysitting and coreutils-by-shell

`scripts/job_management/hanabi_wandb_eval.py` and neighbors — long-running
babysitter loops that enumerate run state with `sh.ls`, manage checkpoints
with `sh.cp`/`sh.rm`/`sh.mv`/`sh.touch`, poll `squeue` via `check_output`,
`scancel` stale jobs, and resubmit via `sbatch` or generated bash scripts.
`redis_logger` starts a long-lived `redis-server` child via fire-and-forget
`Popen` with no handle management.

Budget axes — none: polling loops and coreutils calls run without timeouts
or output bounds, captured or inherited.

Observability — the babysitter loops are themselves observers (their whole
job is polling run state), but their own actions — checkpoint deletions,
scancels, resubmissions — leave no record beyond their side effects, and
the fire-and-forget `redis-server` has no narration, handle, or log
destination at all.

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

Observability — the repo's purpose is observability of cluster jobs
(squeue polling, gnuplot renderings of run metrics), and submission parses
and reports the job directory from slurm's stdout — a small durable
pointer to where results will land. The execution mechanism itself adds no
narration or record.
