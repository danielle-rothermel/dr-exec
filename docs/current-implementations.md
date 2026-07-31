# Current implementations

Catalog of existing process-execution implementations across the fleet: one
section per repo, one subsection per implementation. Companion to
`current-uses.md` — that doc records the needs dr-exec must serve; this one
records the mechanisms that exist today (anatomy, notable properties,
weaknesses) so design can mine prior art deliberately, including from
implementations that are clearly bad. Every filled section pulls out four
categories for cross-implementation comparison — Budget axes,
Observability, Lifecycle, Attribution — plus two only where they exist:
Testing seam and Amortization. Repos ordered by most recent PR activity.

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
lock and overflow event); a monotonic-deadline poll loop; text-only, UTF-8
decode with `errors="replace"`; results in a frozen
`SubprocessCompletedProcess(returncode, stdout, stderr)`. Weaknesses: no cwd, env overlay,
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

Lifecycle — the fleet's reference: fresh session per spawn
(`start_new_session=True`); on every exit path `os.killpg(SIGKILL)` with a
completion-race retry after reaping the leader; 5 s bounded termination
wait and 1 s bounded IPC-thread joins. The only fleet implementation
combining session isolation, whole-group kill, and a bounded wait —
no-survivors compliant.

Attribution — typed tree `SubprocessError` → `Timeout` / `OutputLimit` /
`Infrastructure` (→ `Start`): bounds and infrastructure raise; outcomes
are data — nonzero exit never raises, and the raw returncode including
negative signal values is returned for the caller to interpret.

Testing seam — `PythonSubprocessRunner` Protocol, with two near-identical
~25-line doubles at `tests/metrics/helpers.py:153` and
`tests/humaneval/test_humaneval_primitives.py:167` (unbounded, no group
kill): the double-divergence problem a first-class fake story must
prevent.

### HumanEval batch protocol machinery

`src/dr_code/humaneval/batch_runner.py` plus `batch_runner_script.py` — the
adapter/driver pair over the primitive. Parent side: builds a JSON payload
(candidate code, support code, checks) for stdin and validates the
per-case results against expected case IDs. Child side: shipped as
resource text and never imported; reads one
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
failure. Nothing survives the calling process — and the child emits all
results at exit, so a late-item crash loses the batch: noncompliant with
the results-leave-the-child-incrementally behavior.

Lifecycle — delegated wholesale to the primitive.

Attribution — the fleet's most developed: `CANDIDATE_KILL_RETURNCODES`
({-SIGKILL, -SIGSEGV}) → candidate-attributed error; timeout → every case
TIMEOUT; output limit → every case ERROR; other nonzero exit, JSON parse
failure, shape violation, or unknown/duplicate case IDs →
`EvaluationHarnessError` carrying partial results. The working prototype
of the driver trust split and item accounting.

Amortization — one child per (candidate, task), shared across that task's
cases: an existing declared sharing boundary, with interpreter startup
amortized over the case dimension only.

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
sandbox profile pinned in argv (`-s read-only`).

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

Lifecycle — none beyond `subprocess.run` defaults: leader-only timeout
kill, no group, no escalation, no reap concern.

Attribution — split between the two shapes: the runner maps nonzero exit
to `OpaqueStepError` (exception-as-outcome); the proposer maps timeout to
a typed never-raises `CodexInvocation(timed_out=True)`; both pre-check
absence with `shutil.which`.

Testing seam — no injection: tests fake the codex CLI itself with
`#!/bin/sh` stub scripts on PATH, including a `sleep 5` stub to force the
timeout path (`tests/optimization/test_codex_proposer.py`) — a third fake
pattern alongside Protocol injection and monkeypatching.

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

Lifecycle and attribution — moot while broken; the intended design
inherits both from the dr-code primitive.

### Docker availability probe

`src/whetstone/runner/execution_mode.py:106` — `docker info` in bytes mode,
`shutil.which` pre-check, returncode collapsed to a boolean. Budget axes:
wall-clock 10 s; nothing else applicable. Observability: the boolean
collapse discards all diagnostic detail — a missing daemon, a permission
error, and a hung docker all report identically. Lifecycle:
`subprocess.run` defaults. Attribution: the boolean collapse, nothing
finer.

## whetstone-envs

## fchord

## dotfiles

## whetstone-viewer

## genfxn

### safe_exec worker containment

`src/genfxn/core/safe_exec.py` (881 lines) — model-generated Python
executed in `multiprocessing` workers (forkserver/spawn; `fork` via env
override), self-described as "not a security sandbox." Containment layers:
`resource.setrlimit(RLIMIT_AS)` (default 256 MB); AST pre-validation
blocking `__import__`/`eval`/`exec`/`compile`/`open`/`getattr`, dunder
attributes, `Import`, `ClassDef`, `Global`, and top-level non-function
statements; allowlisted builtins; structured value return through an
allowlisted type graph (nesting depth 32) with results pickled before
`queue.put` so serialization failures surface synchronously; an explicit
`trust_untrusted_code` gate raising `SafeExecTrustRequiredError`; a
persistent reusable worker with its own startup timeout; spawn-bootstrap
diagnostics; six-type error taxonomy. Weaknesses: no stdin path (inputs
travel as pickled args, unbounded). The AST layer is
shape-policing rather than reach-restriction: it forbids what generated
code may *look like* (no classes, no globals), a posture the target
principles reject for research code.

Budget axes:

- Memory — `RLIMIT_AS` 256 MB default: a task-scale interior default of
  exactly the kind the budgets principle prohibits.
- Result size — 1 MB pickled-result bound (protocol budget).
- Wall-clock — per-execution timeout plus a separate worker-startup
  timeout (the fleet's one existing startup self-budget).
- Termination — SIGTERM→SIGKILL with 0.2 s joins; `killpg` only when group
  leadership is confirmed.
- Output — none: child stdout/stderr go to inherited fds, uncaptured and
  unbounded.

Observability — no capture of worker output at all (accidental passthrough
to the parent's fds, not a chosen delivery mode); spawn-bootstrap
diagnostics give good startup-failure attribution; nothing durable.

Lifecycle — TERM→KILL escalation with 0.2 s joins, but degradable:
`_set_process_group` swallows `os.setsid()` failures so group kill
silently falls back to leader-only, and `_terminate_process_tree` swallows
signaling failures — escalation that cannot prove it happened.
`atexit`/`__del__` cleanup for persistent workers.

Attribution — six-type error taxonomy; the `trust_untrusted_code` gate
fails as its own error class; spawn-bootstrap diagnostics attribute
startup failures precisely. No exit-code attribution of payload kills.

Amortization — `_PersistentWorker`/`_IsolatedFunction` reuse one
containment-bearing worker across calls, with a separate startup budget:
the fleet's clearest existing warm-worker amortization.

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

Lifecycle — `subprocess.run` defaults: leader-only timeout kill, no group
handling.

Attribution — the never-raises envelope is the attribution story: absence
(`tool_found`), timeout (`timed_out`), and exit status all data, nothing
exceptional. No payload-vs-executor split beyond that.

Testing seam — constructed `SubprocessRunner` instances passed to
consumers: configuration-as-injection, no monkeypatching.

## symphony-lite

## dr-notion

## dr-dspy

### Deno capability-sandboxed Python interpreter

`dspy/primitives/python_interpreter.py` (vendored) — the fleet's one
non-container sandbox: a persistent Deno process running Pyodide (Python
compiled to WASM), driven over JSON-line RPC on stdin/stdout. Containment
is Deno's deny-by-default capability model, granted per axis at
construction: `--allow-read=<paths>`, `--allow-write=<paths>`,
`--allow-env=<vars>`, `--allow-net=<hosts>` — each an explicit opt-in
list, nothing ambient; no `--allow-run`, so subprocess spawning inside the
sandbox simply does not exist. Paths are symlink-canonicalized so grants
match what Deno's permission check compares against (denoland/deno#9607).
Lifecycle: `_ensure_deno_process` restarts the child whenever `poll()`
shows it dead; on crash, exit code and stderr are read for diagnosis;
`deno info --json` discovers the cache dir up front. Notable properties:
per-axis capability grants that mirror the inherited-state-by-explicit-
grant vocabulary almost exactly; containment with no kernel/container
dependency; restart-on-death supervision of a sandboxed worker.

Budget axes — none: RPC reads are blocking `readline()` with no timeout, a
hung sandbox hangs the caller; no memory, CPU, or output bounds on either
side of the WASM boundary.

Observability — crash diagnosis only (exit code plus stderr on failure);
no narration, no durable record; silent restart of a dead sandbox is
exactly the silent-replacement pattern the supervised behaviors forbid.

Lifecycle — persistent child with restart-on-death (`_ensure_deno_process`
polls before each use); no kill path, no escalation, no reap of the
replaced process.

Attribution — crash surfaces exit code plus stderr; in-sandbox permission
denials arrive as Deno errors through the RPC channel — reach violations
attributed by the mechanism itself.

Amortization — the design's purpose: Deno+Pyodide startup (seconds) paid
once, executions (milliseconds) amortized across the persistent sandbox.

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

Lifecycle — SIGTERM → 1 s → SIGKILL, leader only: escalation present,
group absent.

Attribution — output and exit code returned to the model as data; timeout
distinguishable via the kill path; no finer taxonomy.

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

Lifecycle — SIGTERM only, leader only: no escalation, no group.

Attribution — timeout and AbortSignal cancellation distinguishable from
exit; otherwise raw.

## codearc

### Git fixture and query helpers

~35 sites across `tests/test_cli.py`, `tests/test_miner.py`, and
`scripts/demo_miner.py` — git invoked with `cwd=`, `check=True`, captured,
to build test fixture repos and query them; plus a `run_git` helper
(`scripts/local_loc_stats_t1.py:64`) using `git -C`, and a
`git config --global` read (`:74`) with `check=False` where empty stdout is
interpreted as "unset" — returncode-and-empty-output as data, a small
exit-policy example. Production code executes nothing: mining goes through
pydriller and libcst. Budget axes — none: no timeouts, no output bounds.
Observability — none; nothing beyond `check=True` exceptions. Lifecycle —
`subprocess.run` defaults. Attribution — raise-on-nonzero for fixtures;
the config read's empty-stdout-as-unset is caller-side exit policy in
miniature.

### Secret retrieval via gh

`scripts/github_loc_stats_t1.py:125` — `gh auth token` with
`capture_output=True`, `check=True`: a credential deliberately transported
through captured stdout. Budget axes — none. Observability — evidence for
the faithful-record posture: output that is itself sensitive is a domain
fact only the caller can know, so redaction is caller-side and
post-capture; the executor records verbatim. (llmflow's in-flight
`redactToken` is the fleet's one executor-side redaction instance — the
approach dr-exec deliberately does not take.) Lifecycle and attribution —
`check=True` defaults, nothing finer.

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

Lifecycle — spawn-only: `start_new_session` detaches the worker and then
no kill, escalation, or reap path exists anywhere in the module — workers
are orphaned by design the moment the spawner exits.

Attribution — none: exit is never observed, so nothing is ever attributed.

Testing seam — monkeypatched `Popen` in `tests/test_lifecycle.py`.

## marimo_utils

## nl-code

### Container-gated execution worker

`src/nl_code/code_execution/worker.py` — `exec` of model-generated code
behind `_require_docker_execution`, which raises unless an env var is set
*and* `_is_running_in_container()` verifies the claim: the origin of the
containment-is-verified behavior. In-container defenses: `RLIMIT_CPU`;
per-item `signal.setitimer(ITIMER_REAL)` + SIGALRM deadlines; bounded
stdin and bounded stdout capture via `dr_docker.workers.json_stdio`
(`read_stdin_bounded`, `BoundedTextCapture`); an AST denylist (Async
nodes, dunder access) and a `__builtins__` shallow-copy namespace — the
same shape-policing caveat as genfxn's. Caller side
(`code_execution/runner.py`) delegates spawning to
`dr_docker.SubprocessDockerAdapter` (see dr-docker) and translates its
error envelope into `CodeExecutionInfrastructureError`.

Budget axes — layered across three parties:

- Worker: CPU (`RLIMIT_CPU`) and per-item wall-clock
  (`setitimer`/SIGALRM).
- Caller: stdin cap 50 MiB; per-stream stdout/stderr caps 1 MiB default,
  env-overridable.
- Container: the full dr-docker set (memory, pids, fsize, nofile) by
  delegation.

Observability — error envelopes carry structured codes with a `retriable`
flag (failure as diagnosable data); otherwise no narration or durable
record at this layer.

Lifecycle — delegated to the dr-docker adapter (leader-only, see there);
inside the worker, per-item SIGALRM timers are the only lifecycle control.

Attribution — three-way: the container gate refuses with its own error
before execution; AST rejection is a pre-execution payload verdict; the
adapter's error envelope translates to
`CodeExecutionInfrastructureError` — executor-side by construction.

Amortization — one container per batch with per-item timers inside it: a
declared sharing boundary (container at the batch dimension, deadline at
the item dimension).

## dr_exp

### Slurm worker fleet launcher

`src/dr_exp/worker/launcher.py` — the fleet's most complete supervised
long-lived implementation. `WorkerLauncher` spawns N workers per GPU (its
own CLI: `sys.executable -m dr_exp.cli.main ... worker --worker-id ...`),
each with an environment overlay (`os.environ.copy()` plus a per-worker
`CUDA_VISIBLE_DEVICES`), stdout+stderr into a per-worker log file, and
`preexec_fn=os.setsid` for group management. Supervision loop: `poll()`
health check every 5 s; restart-on-failure as a constructor-declared
policy, gated on pending work, with restart counts tracked and each restart
logged; a launcher lifetime cap of 47 h defaulted deliberately inside the
48 h SLURM limit; a control-file command channel (`stop_<jobid>` /
`finish_current_<jobid>`) checked each tick; signal handlers routing
SIGTERM/SIGINT into the same shutdown path. Shutdown is group-targeted with
escalation: SIGTERM via `killpg` to every worker, a fixed 5 s grace sleep,
SIGKILL to survivors. GPU discovery parses `CUDA_VISIBLE_DEVICES` with
`nvidia-smi --list-gpus` fallback (full binary path, captured, `check=True`,
no timeout). Weaknesses: restart identity reconstructed by string-parsing
worker IDs; the grace period is a fixed sleep, not a bounded wait, and
kills are never followed by a reap; no per-job deadline at this layer.

Budget axes:

- Launcher lifetime — 47 h against SLURM's 48 h: a contract budget derived
  from a real downstream constraint, the taxonomy's clearest existing
  example.
- Termination — 5 s TERM→KILL escalation, group-targeted (executor
  self-budget); no post-kill reap wait.
- Output — per-worker logs spill to disk, unbounded: the permissive
  disk-backed posture the budgets principle prefers.
- Per-worker/per-job wall-clock — none at this layer.

Observability — the fleet's richest: narration to console and a launcher
log file simultaneously; a per-worker durable log each; a status JSON
heartbeat rewritten every 60 s (workers, restart counts, job-state tallies,
runtime); error aggregation sweeping worker logs for
error/exception/traceback markers every 10 min into `errors.log`; every
spawn, restart, kill, and signal logged. One durable-record flaw: the
status file is deleted on clean exit, so the final heartbeat survives only
crashes.

Lifecycle — `setsid` process groups per worker; shutdown is group SIGTERM
→ fixed 5 s sleep → group SIGKILL for survivors, with external signals
routed into the same path; drain exists as a first-class mode
(`finish_current`). Flaws: the grace period is a sleep, not a bounded
wait, and kills are never followed by a reap.

Attribution — worker exits recorded as `exited(returncode)` status
strings; restart accounting per slot; error attribution is log-derived
(the periodic error-marker grep) rather than protocol-level.

Amortization — workers pull jobs from a shared queue, so process startup
amortizes across all jobs a worker lives through: amortization at the
worker dimension.

Testing seam — `subprocess.run`/`Popen` mocked in
`tests/integration/test_worker_launcher.py`.

### CLI self-invocation job submission

`scripts/submission/*.py` (~a dozen sites) — job submission drives the
repo's own `dr_exp` CLI via `subprocess.run(capture_output=True)`, parsing
stdout for lines like "Job submitted with ID:". Self-invocation as an API:
the process boundary substitutes for a library call, with string-parsing
where a return value should be. No timeouts anywhere; mixed `check=`
postures. Budget axes — none. Observability — captured output is consumed
only for the parsed line; no narration or record beyond each script's own
prints. Lifecycle — `subprocess.run` defaults. Attribution — success
determined by string-matching stdout; mixed `check=` postures.

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

Lifecycle — split-brain: container-side teardown is the strength (cidfile
tracked, `docker rm -f` in `finally`, CID validated) while client-side is
weak (bare `proc.kill()` leader-only, no escalation, unbounded wait,
writer thread joined without timeout).

Attribution — `ErrorEnvelope(code, retriable)`: structured failure codes
with retriability as data, the fleet's only retriability-aware taxonomy.

Amortization — none: a cold container per call, seconds of setup however
small the payload — the shape that motivated the amortization principle.

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
the execution itself. Lifecycle — `check_call` defaults; nothing outlives
the call except the slurm job itself. Attribution —
`CalledProcessError` or nothing.

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
destination at all. Lifecycle — none: the redis child has no handle, no
kill path, no reap; everything else is `check_output` defaults.
Attribution — none; side effects are the only record.

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
narration or record. Lifecycle — `check_output` defaults throughout.
Attribution — raise on nonzero plus parsed stdout; uniform.
