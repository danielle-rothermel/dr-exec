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

### Symlink-farm workspaces for a vendored generator

`src/whetstone_envs/c18/upstream.py:169` — `[sys.executable,
"run_experiment.py", ...]` drives a vendored generator whose output
filename is fixed, so each call gets a `TemporaryDirectory` populated with
symlinks into the read-only vendored tree (:143-146) as its `cwd`:
parallel calls cannot collide, and the shared tree stays untouched. The
result is read from a file the child wrote (:183-195) — file-delivered,
not captured. Test-side patterns worth noting: `patch --batch -p1` applied
against pinned SHA256s (content-pinned mutation of the vendored tree),
full env replacement with a constructed `PATH`, and `PYTHONHASHSEED`
pinning for determinism.

Budget axes — wall-clock per call; output captured unbounded (the result
travels by file, disk-backed and unbounded).

Observability — none live; on failure `UpstreamError` carries
`stderr[-500:]` — the recurring tail-slice pattern: a post-hoc excerpt
standing in for a record nothing kept in full.

Lifecycle — `subprocess.run` defaults, leader-only.

Attribution — `check=False` with nonzero mapped to a dedicated
`UpstreamError` carrying the stderr tail: a typed executor-side verdict,
no finer split.

Amortization — a fresh symlink farm per call: cheap (symlinks, not
copies), a deliberate trade of per-call setup for parallel safety.

## fchord

### Timeout-bearing git helper with persisted failure records

`src/fchord/github_pull.py:186` (`_git`) — captured, text, `check=True`,
and — rare among the fleet's git helpers — a timeout: `_GIT_TIMEOUT =
300.0` (:36), per *invocation*, and `_sparse_shallow_clone` makes three
(clone, sparse-checkout, rev-parse), so the effective clone bound is up to
900 s. Repo scoping via `git -C` argv; the checkout lives in a
`TemporaryDirectory` discarded on every path. Shell scripts: three are
pure `exec` process replacement; `parse-codex-session.sh` does
mktemp + `trap rm EXIT` + atomic `mv` + trap disarm — a complete
atomic-write-with-cleanup idiom in shell.

Budget axes — wall-clock only (module constant, per invocation not per
operation); no output bound.

Observability — the fleet's clearest failure-as-persisted-record: clone
failure writes `pull.json` with a `FailedPage(url, error)` entry via
atomic write and returns it — the failed attempt is durably recorded, not
raised (docstring states the contract). Success records provenance: the
resolved commit SHA from `rev-parse` is stored in the pull record.

Lifecycle — `subprocess.run` defaults, leader-only; cleanup is
filesystem-only.

Attribution — better than most: `CalledProcessError` → last *line* of
stderr (exit-code fallback when stderr is empty) vs `TimeoutExpired` → a
distinct "git timed out after Ns" message, both landing in the persisted
record; `OSError`/git-absent propagates uncaught.

Testing seam — none: tests clone for real over `file://` fixtures with an
unbounded `check=True` fixture helper — the spawn path is exercised live.

## dotfiles

### Git sync helpers

`scripts/sync_skills.py` — `run_git`/`run_git_bytes:261,273`
(`git -c core.quotepath=off`, explicit PIPE capture, `check=True`, no
timeout parameter and no seam to add one) — including *network* calls
(`clone --depth 1`, `fetch --depth 1` mid-merge) with inherited stdin and
no `GIT_TERMINAL_PROMPT=0`: a credential prompt hangs a merge forever.
Three returncode disciplines coexist: raw `CalledProcessError`
(`check=True`); returncode-as-boolean with stderr→DEVNULL
(`commit_available:436`, `git_tree_exists:746` — missing repo, corrupt
object, and absent commit all collapse to `False`); and
`merge_file_bytes:963` reinterpreting *every* nonzero as "conflict" —
though `git merge-file` returns −1/128 on genuine error, with stderr
discarded, so errors masquerade as conflicts. `git archive` output is
materialized wholly in memory into a tarfile. Amortization — none: a
three-way merge of an N-file skill spawns ~3N git processes
(`cat-file blob` per file per side). Testing seam — none: tests spawn
the real script via `uv run` (no timeout), with testability coming from
env-var path injection, not a runner seam. Note: the audit's
`skill_dispatch.py` agent-CLI spawn (`claude -p`, timeout 120) no longer
exists — the file was cut to a pure-filesystem checker; dotfiles now has
no agent-CLI execution.

### pi extensions (TypeScript)

`zsh-user-bash.ts:31` — every model-issued command becomes
`exec <zsh> -fc <command>` passed as a *string* to pi's bash-based exec:
double-shelled by construction (bash parses the outer, zsh the inner),
with single-quote `shellQuote` correct for one level; bounding and kill
delegated entirely to pi. `copy-all.ts` — `spawn("pbcopy")` with stderr
accumulated into the rejection message and spawn-error distinguished
from exit failure, but no stdin error handler (EPIPE → unhandled) and no
timeout. `diff.ts` — `pi.exec` with a bare-literal 5000 ms timeout
repeated per site, called up to four times per flow with no aggregate
budget.

## whetstone-viewer

### Cross-repo hydration via sibling venvs

`src/whetstone_viewer/etl/hydration/runner.py:111` (`_run_dump`) —
`["uv", "run", "--frozen", "--project", <repo>, "python", "-", *args]`
with a first-party script piped to `python -` on stdin: the script
executes inside a *sibling repo's pinned venv* — a working declared-runtime
instance, with the interpreter and package set specified as "that
project's frozen environment" rather than inherited from the host.
The dump programs live as module string constants (`dump_scripts.py`)
specifically because they import packages that exist only in the child's
venv and must never be importable in the parent — dependency isolation
across sibling repos as the stdin-payload rationale. Purity is a stated
constraint throughout: `--frozen` so source-repo lockfiles are never
mutated, stdin delivery so nothing is written into the source tree,
`--isolated` ruff for byte-stability across machines.

Budget axes — wall-clock 300 s per dump, 30 s per git call, 60 s per ruff
call — all module constants; output captured unbounded.

Observability — provenance-grade: each source repo's commit and dirtiness
land in the build summary, as does the resolved ruff version; hydration
failure degrades *with a record* (`hydration_note = "hydration FAILED…"`)
rather than aborting the build. One conflation: a failed `git status`
and a clean tree both yield `dirty=False`.

Lifecycle — `subprocess.run` defaults; the `uv run` → `python` two-level
tree makes the leader-only timeout kill a real survivor risk.

Attribution — four-way in `_run_dump`, finer than one error type
suggests: launch failure, nonzero exit (`stderr[-2000:]`), non-JSON
stdout (`stderr[-1000:]`), and JSON-of-wrong-shape each get distinct
messages inside `HydrationError`. Child-side partial failure is data
(per-env `{"error": …}` entries in the JSON); parent-side failure is the
exception — a working driver-trust-split instance.

Testing seam — two: `RuffFormatter.resolve(ruff_cmd=…)` is an argv-prefix
injection seam (default `["uv","run","--frozen","ruff"]`), and hydration
tests inject constructed `HydrationResult`s above the boundary — the
spawn path is deliberately untested in CI.

Amortization — `RuffFormatter` resolves `ruff --version` once per build
and reuses it for every task (the version doubles as provenance); each
format call still pays a fresh spawn plus a per-call `TemporaryDirectory`
whose fixed inner filename (`canonical.py`) is collision-free precisely
because the directory is per-call.

### Failure-degrades-to-None helpers

`:153` (`_git`): `git -C` rev-parse/status with timeout 30, all failures
→ `None`; `etl/hydration/task_intrinsic.py:270`: `ruff format --isolated
<tmpfile>` read back from the mutated file (artifact-path delivery),
timeout 60, any failure → `None`. Two different judgments apply: the git
helper's collapse loses attribution outright, but the ruff `None` is a
*documented* null-vs-false discipline — "a missing measurement stays null
instead of being coerced to 'unchanged'" — degradation that preserves
data honesty even while discarding failure detail.
`web/scripts/gen-api.mjs:26` — `execFileSync` with `stdio: "inherit"`,
no timeout, throws on nonzero; the child delivers its result via `--out
<file>` (artifact path), and a `try/finally rmSync` cleans the scratch
dir on every path.

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

### Spawn-context crash-recovery harness

`tests/test_dbos_recovery_boundary.py` — `multiprocessing` spawn-context
workers used to prove DBOS replay across a genuine process death:
`os._exit(86)` as an uncatchable crash with a sentinel exit code the
parent asserts exactly. Two-tier deadline with a documented rationale: the
child polls its own 10 s deadline, the parent joins at 20 s — the margin
exists "so `terminate()` cannot race in and destroy the worker's
diagnostic traceback exactly as it reports the timeout." Escalation is
one step (terminate, then *unbounded* join). Concurrency safety by
namespace: every shared identifier (pipeline, queue, app name/version)
is uuid-suffixed per run — collision avoidance against a shared Postgres
rather than via temp dirs. Attribution: exit-code sentinel plus four
independent durable DB signals cross-checked after the crash.

## dr-graph

### Import-hygiene probe

`tests/test_imports.py:31` — `sys.executable -c` *without* `-I`: the probe
deliberately measures the ambient environment (PYTHONPATH, venv, user
site) rather than an isolated one. Attribution travels through the exit
path with no protocol parsing: the child raises
`SystemExit(",".join(loaded))` — a non-integer SystemExit prints the
offending module names to stderr and exits 1; the parent asserts
returncode 0 with stderr in the failure message. Captured, `check=False`,
no timeout; the repo's only exec site.

## dr-providers

### Import-hygiene probes and script exec_module

`tests/test_public_api.py:125,133` — `sys.executable -c` with
`check=True` and *no capture*: the child asserts in-process, so the
failure detail lands only on inherited stderr (visible in pytest's
capture, absent from the `CalledProcessError`). The second probe's
payload is generated by joining imports over a `PURE_MODULES` data
list — program-from-data, not a literal.
`tests/test_audit_ground_truth.py:10-17` loads a checked-in script via
`spec_from_file_location` + `exec_module` at module import time, through
*relative* paths — cwd-dependent, and a script failure is a pytest
collection error, not a test failure. No timeouts anywhere.

## dr-subs

### SSH remote worker invocation

`src/dr_subs/machines.py:121-198` (`_run_remote_worker`) — the fleet's one
remote execution path: `Popen(("ssh", *_SSH_OPTIONS, source_id,
*_REMOTE_WORKER_COMMAND))` with the peer drawn from a closed enum (:51), a
JSON request on stdin, and the response read via `selectors` + `os.read`.
Companion probes: an ssh reachability check (DEVNULL, timeout 8,
returncode inspected) before expensive work, and a `scutil` hostname query
whose failure degrades gracefully to `"local"`. Design flaw worth
preserving as a lesson: the request is written to stdin in full before the
read loop starts (:149), so a request larger than the pipe buffer can
never complete — the write-before-read deadlock in production form.

Budget axes — both caller-declared per call, the fleet's best
parameterization: wall-clock (default 3600 s) and a response cap
(64 MiB default, per-call override) — a contract budget on the response;
input unbounded.

Observability — stderr uninherited by the protocol: it passes through to
the operator's terminal (accidental stdio passthrough for diagnostics); no
narration, no record.

Lifecycle — `_stop_process`: SIGTERM → 1 s wait → SIGKILL, single process
only; no session, no group — the ssh client dies, whatever it spawned
remotely is out of reach entirely.

Attribution — the fleet's only transport-aware taxonomy:
`RemoteScanError(code)` distinguishes transport failures (`ssh_timeout`,
`ssh_unavailable`) from remote-worker failures (`peer_worker_unavailable`,
`peer_worker_timeout`, `peer_response_limit`, `peer_protocol_error`) —
attribution across a machine boundary, plus a typed UTF-8 decode error at
the response edge.

## dr-diagram

### Headless Chrome CDP scripts

Four viz scripts spawn Chrome with `--remote-debugging-port=0`, scrape
the DevTools `ws://` URL from stderr — matching each chunk
*independently*, so a URL split across two data events is never matched
(a real race, backstopped only by a 15 s wait) — then drive a
hand-rolled ~15-line CDP client (five copies, one hand-framing raw
WebSocket bytes; none has a response timeout, so a lost CDP reply hangs
forever). Cleanup is called only from try/catch bodies — no signal or
exit handlers, so Ctrl-C orphans the Chrome tree and its temp profile;
kills are leader-only `SIGKILL` with no wait, racing profile `rmSync`
against Chrome's own shutdown writes (race swallowed by a bare catch).
`reflexion-workbench/scripts/smoke.mjs` is a different shape
(`--dump-dom` + virtual-time budget, pid-suffixed profile dir *inside
the repo tree*), and its attribution is inverted: exit and error both
resolve with accumulated stdout — spawn failure, timeout-kill, and
success are indistinguishable except by downstream content checks.
`phase7/skill-audit/shots/cap.mjs` is scratch code: fixed port 9333
(concurrent runs collide), *no* `--user-data-dir` (writes the real
Chrome profile), no kill at all, and statically-broken imports. Five
`sh -c 'command -v'` probes with two different return contracts.

### Shell wrappers: Perl deadline and codex batch

`phase6/…/pi-image-run.sh` — the Perl deadline faithfully reconstructs
shell-convention exit codes (128+N for signal deaths) on the *normal*
path, but the timeout path exits 124 from inside the SIGALRM handler, so
a deadline kill can never report the child's actual signal; TERM → 5 s →
KILL on a single pid, no setsid — pi's tool subprocesses survive.
`eval "$(mise env)" || true` silently proceeds under a wrong toolchain.
`phase7/…/run-codex.sh` — `env -i` keeping only HOME/PATH/TMPDIR with
`ANTHROPIC_*` re-added as *empty strings* (present-but-empty, not
unset); prompt via stdin herestring, result via `-o <file>`, full trace
spooled per invocation (the repo's one durable record); but
`set -uo pipefail` without `-e` means per-document failures print and
the loop continues — the script always exits 0 with no status
aggregation.

## unitbench

### Sibling-repo codegen scripts

`scripts/gen-api.mjs` — one `run()` helper serving two delivery modes
(captured by default, `stdio:'inherit'` at the second call site), with a
live bug on the inherit path: the error message calls `.trim()` on a
null `stderr`, so a failing `openapi-typescript` crashes the reporter
with a TypeError instead of reporting; missing-binary (`status null`)
hits the same crash, and `result.error` is never consulted. Sibling
repos are reached via `uv --directory <path>` with env-overridable dirs
whose relative defaults resolve against `process.cwd()` — running from a
subdirectory silently targets the wrong path. `gen-graph-schema.mjs` —
no error handling at all, Node's default 1 MiB `maxBuffer`, raw stdout
into `JSON.parse` (any uv warning corrupts it), then an unguarded
hand-patch of the generated schema: a cross-repo contract enforced by
inline mutation.

### Vercel install wrapper

`scripts/vercel-install.mjs` — the fleet's cleanest JS injection seam:
environment, spawn function, and output sinks all defaulted parameters
with typed JSDoc contracts, fully drivable without a real process.
Redaction is thoughtful but bounded: the token is redacted in raw *and*
percent-encoded forms, but per-chunk (a token split across stream chunks
leaks), and it still lives in the child's env — visible to every
descendant of `pnpm install`; the parent-env delete prevents
re-forwarding, not exposure. Attribution: exit code preserved in the
message, but the spawn-error path discards the underlying error for a
fixed string (ENOENT and EACCES indistinguishable). No timeout.

## nlae

### Two-transport fetcher with filesystem reconciliation

`src/nlae/arxiv_library/fetch.py` — a two-backend design the audit
flattened: `Transport.AUTO` picks gcloud iff `shutil.which` finds it,
else a pure-Python httpx path that spawns nothing — availability-driven
backend selection where the *in-process fallback is the better-bounded
path* (60 s timeout + retries vs gcloud's no timeout, no capture,
inherited stdio). Attribution is by reconciliation, not exit code: after
the copy, the destination directory is re-listed and diffed against the
manifest — partial success degrades to a misses list, the child's word
is never trusted. The `.part` + atomic-rename convention (httpx path
only) makes interrupted downloads invisible to the held-file census.
Testing seam — the `runner=subprocess.run` identity-comparison pattern
at two levels: production and tests take structurally different paths
(the which-preflight only runs when the runner is the real one), and the
fakes pin `subprocess.run`'s exact kwarg names — adding `timeout=` to
production would break every test: a seam that actively resists adding a
budget.

## dr-llm

### Headless agent CLI transports

`llm/providers/transports/headless_base.py` — `subprocess.run` captured
text with a per-provider-config timeout (180 s default, 600 s for
claude/codex) — config-level, never per-call. The "shell denylist" is a
basename check on argv[0] only (`env bash -c …` passes); the codex
`--sandbox read-only` is a replaceable default, not a pin. Environment:
overlay `{**os.environ, **overrides}`; the API key is injected under two
names and only when present — a missing key silently falls through to
ambient auth. `required_executables`/`required_env_vars` are computed
declaratively at config time but never actually checked before spawn.

Budget axes — wall-clock only (config field); output capture unbounded.

Observability — a durable JSONL transcript sink (lock-serialized,
never-fails-the-call) with three-layer bounding: 512-char sanitize →
key-name redaction → 10 MiB event envelope. Under *default* config the
sanitize layer replaces payloads with `"<omitted>"` — which also empties
the exception message, so the production failure message contains no
stderr at all (the `[:800]` clip is dead code), and timeout events
discard `TimeoutExpired`'s recovered partial output. `latency_ms` is a
first-class response field. Argv is logged verbatim (not in the redaction
key set); env is never logged.

Lifecycle — no `start_new_session` anywhere in the repo; the leader-only
timeout kill reliably orphans agent-CLI grandchildren.

Attribution — nonzero exit → `HeadlessExecutionError`; codex adds an
in-band channel: JSONL `type=="error"` events raise even on exit 0
(exit-0-but-failed handled), and unparseable child stdout is *retained*
as `non_json_stdout_lines` data rather than dropped.

Testing seam — monkeypatched `subprocess.run` whose fake captures argv,
input, and env but silently drops `timeout` — no test can assert the
budget is passed.

### Docker invocation and streaming psql

`project/docker_runner.py` — captured bytes, no timeout; `docker_error`
classifies by stderr *substring* into five typed errors plus a fallback —
returncode never inspected, locale/version-fragile — and call sites layer
further ad-hoc string checks on top ("already running" → success). The
taxonomy drives retry-vs-abort in the readiness loop: retriability
derived from substrings. Tool-missing is handled three different ways in
one repo, with no `shutil.which` preflight anywhere.
`docker_lifecycle.wait_docker_ready`'s `timeout_seconds` is an *iteration
count* (each iteration spawns an untimed `docker exec`), while its
sibling `wait_dsn_ready` is a true monotonic deadline — same parameter
name, different semantics. `docker_psql.py` — bidirectional binary
streaming with a daemon stderr thread; the success-path `wait()` and
thread joins are unbounded; kill exists only on the exception path,
leader-only; psql's stdout goes to DEVNULL (restore diagnostics are
stderr-only). Genuine bright spot: `docker_swap_in_db`'s compensating
cleanup — a uuid-named temp database dropped on any exception, with
cleanup failure narrated via `exception.add_note`. `validate_pg_identifier`
is the repo's one injection defense, aimed at SQL identifier
interpolation, not argv. `postgres_sync.py:747` — psql restore with an
open-file stdin, bytes, no timeout; the pgpass tempfile is chmod-0600
*after* creation (a brief default-mode window). Tests include an explicit
deadlock-avoidance case (stderr drained before `wait`) and a fake built
by calling `Popen.__new__` to skip initialization.

## nl_latents

### Containment tripwire and provider worker scripts

`evaluation_runner.py:184-213` — the fleet's one caller-side containment
verification: snapshot `git status --short`, run a one-item smoke
evaluation through the docker path, re-snapshot, and raise if the
working tree changed ("use docker isolation before running decoder
evaluation") — a pre-flight proof that containment actually holds,
using the filesystem as the witness. `docker_env={"MPLBACKEND": "Agg"}`
is an explicit env overlay into the sandbox. Tests use two unusual
oracles: `bash -c 'source <config> && printf "$VAR"'` to assert a shell
config's exported default, and substring asserts on script *text*.
`scripts/shared_provider_worker_runner.sh` — a bash process-group
manager: pids array + background jobs + bare `wait`, cleanup is `kill`
(TERM only, no groups, no escalation, trap wired by the sourcing
caller); per-provider worker counts and retry policies as env-defaults
(rate-limit-derived concurrency ceilings); `: "${VAR:?}"` fail-fast on
required config; and the fleet's highest-fidelity command narration —
`printf ' %q'` logging each launched command shell-quoted and
re-runnable.

## dr-cognee

### Vendored mirror copy

`src/dr_cognee/vendored/github_docs_mirror.py` — verified byte-identical
to dr-notion's file except a one-line vendoring header ("keep edits
upstream"); no drift, and actively imported (committed `__pycache__`).
Every dr-notion finding applies at a +1 line offset.

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

### Detached agent-run supervisor

`src/symphony_lite/claude_runner.py:50` — agent runs spawned detached:
`Popen(["claude", "-p", prompt, ..., "--output-format", "stream-json"],
cwd=workspace, stdout=<open file>, stderr=STDOUT, stdin=DEVNULL,
start_new_session=True)`, pid recorded in SQLite, runs lasting minutes to
hours. The closest existing thing to ownership-survives-the-owner: the
CLI exits and later invocations kill or inspect runs via the stored pid —
but with the identity hole open (liveness is `os.kill(pid, 0)`, kill is
`os.killpg(run.pid, SIGTERM)` with no pid-reuse guard: a recycled pid can
be probed or signaled as if it were the run). `cwd` is load-bearing
(claude session IDs are cwd-scoped). Precondition check worth noting:
`assert_subscription_auth_env()` asserts `ANTHROPIC_API_KEY` is *absent*
before spawn — a negative environment assertion.

Budget axes — none: no timeout (unbounded by design), transcript file
grows without limit (a deliberate unbounded-accumulation grant).

Observability — the transcript is spooled (stream-json to a file, live
and durable): genuinely good delivery. The SQLite pid registry is a
durable ownership record. No narration of lifecycle events; exit is never
observed (no wait, no reap — death is discovered by a failed liveness
probe).

Lifecycle — group-targeted SIGTERM, but nothing else: no escalation, no
wait, no reap, no exit observation ever; the runs are permanent zombies
until the pid table is manually reconciled.

Attribution — none: exit status is never collected, so nothing is ever
attributed.

### Codex app-server JSON-RPC singleton

`src/symphony_lite/codex_appserver.py:67` — a long-lived singleton child
(`codex app-server`, stdin=PIPE, stdout=PIPE, stderr=append-mode log
file, `start_new_session=True`) speaking bidirectional JSON-RPC: a reader
thread demultiplexes responses by id into per-request queues; a write
lock serializes stdin. Cancellation is protocol-level: `kill_codex_run`
sends a `turn/interrupt` RPC rather than a signal — in-flight work
cancelled without touching the process. Flaws: `_read_loop` iterates
stdout with no size bound; `_pending` is mutated under two different
locks; the stdin write is unguarded against `BrokenPipeError`; a crashed
child is never reaped or restarted; `stop()` is leader-only
`terminate()`.

Budget axes — per-request wall-clock 60 s (interactions budgeted while
the child is not — the UC6 shape); no output bound on the RPC stream.

Observability — stderr spooled to an append-mode log (durable); no
narration; a dead child is discovered as a broken pipe on next use.

Lifecycle — leader-only terminate, no escalation, no reap, no restart.

Attribution — per-request timeout distinguishable; channel failures
(dead server, broken pipe) surface as raw exceptions, unattributed.

Also noteworthy, one line each: `cli.py:165` replaces the CLI process
entirely via `os.execvp` (exec-replacement, outside dr-exec's spawn
model); git helpers at `workspace.py` use `git -C` with timeouts and
returncode-as-predicate.

## dr-notion

### GitHub docs mirror git operations

`src/dr_notion/github_docs_mirror.py` — a sparse blob-filtered clone
(`--depth 1 --filter=blob:none --sparse` + `sparse-checkout set
--no-cone` with computed patterns): bandwidth bounded *by protocol*,
expressed in argv — the fleet's only resource bounding done by asking for
less rather than killing. The clone is uncaptured with inherited stdin
and no timeout: a credential prompt on a private URL blocks forever.
`run_git` (cwd=checkout, captured, `check=True`) propagates raw
`CalledProcessError` — git's stderr rides the exception object but is
absent from `str(exc)`, so failures surface as "exit status 128" with
the actual git error invisible. Warm reuse via `ensure_checkout`
(fetch+detach vs clone paths to the same post-state) guarded by an
origin-URL match check; the checkout path is fixed and unlocked —
concurrent mirrors of one repo race on the same tree. Env fully
inherited: no `GIT_TERMINAL_PROMPT=0`, no `git -c` hardening.

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

### SGLang server launcher (vendored)

`dspy/clients/lm_local.py:71` — `Popen(["python","-m",
"sglang.launch_server", …])` with stdout=PIPE/stderr=STDOUT and a daemon
tail thread that appends every line to an *unbounded* `logs_buffer` while
printing to console until a ready event. Budget: a readiness timeout only
(param, default 1800 s) — no execution cap. Lifecycle: readiness-timeout
failure does `kill()` with no wait/reap and the tail thread unjoined;
normal teardown is delegated and self-described as non-atomic ("Ideally,
the following happens atomically"). Process state is monkey-patched onto
the LM object (`lm.process`, `lm.thread`, `lm.get_logs`) — no handle
type, no registry. `get_free_port()` is a classic TOCTOU bind race, and
the server binds 0.0.0.0.

### HumanEval batch ancestor

`dr_dspy/humaneval/task.py:555` (`run_subprocess_batch`) — the direct
ancestor of dr-code's batch machinery: identical payload and protocol,
but raw inline `subprocess.run`. The delta to dr-code is a measured
evolution record of what the rewrite added: an injection seam plus pure
build/interpret halves, an output cap branch, kill-code attribution,
per-case timing, the timeout value in messages and results, and
partial-result-carrying exceptions — the ancestor has none of these, and
every failure (including unexpected ones) either propagates raw or
collapses into generic per-case error data. `runtime.py:37` forces the
multiprocessing start method (`fork` on non-Windows, against CPython's
macOS default) with `force=True` under a suppressed `RuntimeError` —
silently clobbering any prior setting.

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

### Tailwind shell-string build and pre-check script

`styles/build.mjs:12` — the fleet's one shell-string invocation
(`execSync("npx tailwindcss -i ./input.css -c ./tailwind.config.js
--minify")`): the entire minified CSS transits stdout under Node's
*default 1 MiB `maxBuffer`* — the one place in the fleet where the unset
default is a realistic failure mode, not theoretical; stderr inherits,
so a tailwind error throws without diagnostics in the exception. No
timeout. `scripts/pre-check.sh` — near-twin of dr-code's with a marimo
step added and a strict check commented out in place; its `run_report`
is a dual-delivery pattern (tee to terminal + durable per-check artifact
under `.cache/pre-check/`, true status recovered via `PIPESTATUS[0]`),
`set -uo pipefail` *without* `-e` deliberately enabling failure
aggregation — while `run_silent`'s four autofix steps propagate no
status at all and can fail silently.

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

### Re-invocation wrappers and cross-repo dr_exp calls

`src/deconcnn/cli.py:31-75` — five identical console entry points that
re-exec a sibling script as a child (`[sys.executable, script] +
sys.argv[1:]`, inherited stdio, exit code propagated via `sys.exit`)
instead of importing its main; script resolution falls back to
`Path.cwd()`, making the *executable's location* cwd-dependent. The
cross-repo `dr_exp` calls (`scripts/run_jobs/*`, three sites) reach the
sibling's venv by hardcoding `cwd="/scratch/…/dr_exp"` and then passing
`--base-path ../deconCNN/experiments` — two path conventions that must
agree for anything to work. No timeouts anywhere; one helper collapses
launch failure and job failure into a bare `False` with stderr
discarded; one process per job id in a loop (no batching, full `uv run`
startup each). One safety pattern: a dry-run gate plus a
queued-jobs-only guard before destructive `job remove` calls.

## dr_gen

### Local parallel training launcher

`scripts/parallel_runs.py` — Hydra-sweep training runs as `Popen`
children with per-job stdout/stderr log files (spooled; the parent's
handles close immediately after spawn, working only via the child's
dup'd fds), env overlay (`os.environ.copy()` +
`CUBLAS_WORKSPACE_CONFIG` for determinism + per-job
`CUDA_VISIBLE_DEVICES`). GPU assignment is a round-robin over a mutable
module-global index — not availability-based, so slots can collide when
parallelism exceeds GPU count. Budget axes — none: no timeouts; the only
bound is a max-parallel-jobs admission count. Lifecycle — busy-poll
admission and drain loops over a handle list with sleeps; no kill path,
no signal handling, no reap: Ctrl-C orphans every child. Attribution —
rc==0 binary, plus a pointer to the per-job stderr log. Observability —
`print(flush=True)` narration; one durable record,
`launcher_critical_errors.log`, for launch failures (two exception
classes distinguished, failing command included) — failed launches are
otherwise silently skipped. Hardcoded cluster paths throughout.

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

### Slurm query helpers

`src/dr_util/slurm_utils.py` — `_run_command`: captured text,
`check=True`, timeout 30 s as a module constant shared by all four
callers (`sinfo`, `sacctmgr`, `squeue`, `scontrol`). Two endpoints return
raw unparsed stdout in a pydantic `raw_output` field — unbounded string
straight into a serialization type. Notable validate-then-don't-use: the
QOS name is regex-validated but never reaches argv — `squeue` runs a
fixed command and the value is only compared in-process; only the
partition query actually interpolates its validated string.

Budget axes — wall-clock 30 s, module constant; no output bound.
Observability — zero. Lifecycle — `run` defaults, leader-only.

Attribution — asymmetric and lossy: `TimeoutExpired` → `SlurmError`, but
`SlurmError` subclasses `ValueError` — the same base as the validation
errors, so a `ValueError` catcher cannot tell "bad partition name" from
"slurm timed out"; nonzero exit and `FileNotFoundError` (the common case
off-cluster) propagate raw and untyped.

Testing seam — monkeypatch at two depths: `_run_command` faked with
hand-built stdlib `CompletedProcess` objects (the helper as de facto
seam, signature-coupled to stdlib), and `subprocess.run` patched directly
for the timeout-mapping test.

## parse_claude

### Uniform CLI end-to-end suite

44 subprocess sites across 5 test files, verified fully uniform:
`["uv","run","python","-m","parse_claude", …]`, captured text,
`cwd=` repo root, `check=False` with the returncode asserted. Zero
`timeout=`, zero `env=`, zero `shell=` anywhere. Every call pays the full
two-level `uv run` → `python` tree, and all 44 share the repo working
tree — no per-test isolation. Sharpest finding: `test_performance.py`
asserts wall-clock thresholds (<5 s, <3 s, <10 s) via `perf_counter`
around *untimed* processes — a hang blocks the suite forever instead of
failing the very threshold it was written to guard. Ruff S603/S607
suppressed globally in pyproject.

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
