# Current implementations

Catalog of existing process-execution implementations across the surveyed
repos: one
section per repo, one subsection per implementation. Companion to
`current-uses.md` — that doc records the needs dr-exec must serve; this one
records the mechanisms that exist today (anatomy, notable properties,
weaknesses) so design can mine prior art deliberately, including from
implementations that are clearly bad. Every filled section pulls out four
categories for cross-implementation comparison — Budget axes,
Observability, Lifecycle, Attribution — plus two only where they exist:
Testing seam and Amortization. The behavioral vocabulary used in judgments
throughout is defined in `target-usecases.md`. Repos ordered by most recent
PR activity (a superset of `current-uses.md`'s ordering — the six
mechanism-only repos are interleaved here).

## dr-code

### Budgeted subprocess primitive

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
- Termination — 5 s budgeted wait for group death and 1 s budgeted IPC-thread
  joins; exceeding either → `SubprocessInfrastructureError`.

Observability — none: no narration of any lifecycle step, no run
record; the run result is in-memory only and dies with the caller. Channel
separation is trivially safe (the executor emits nothing). A run that hangs
inside its deadline is indistinguishable from one making progress.

Lifecycle — the survey's reference: fresh session per spawn
(`start_new_session=True`); on every exit path `os.killpg(SIGKILL)` with a
completion-race retry after reaping the leader; 5 s budgeted termination
wait and 1 s budgeted IPC-thread joins. The only implementation in the
surveyed repos combining session isolation, whole-group kill, and a
budgeted wait — no-survivors compliant.

Attribution — typed tree `SubprocessError` → `Timeout` / `OutputLimit` /
`Infrastructure` (→ `Start`): bounds and infrastructure raise; outcomes
are data — nonzero exit never raises, and the raw returncode including
negative signal values is returned for the caller to interpret.

Testing seam — `PythonSubprocessRunner` Protocol, with two near-identical
~25-line doubles at `tests/metrics/helpers.py:153` and
`tests/humaneval/test_humaneval_primitives.py:167` (unbounded, no group
kill): the double-divergence problem a first-class fake story must
prevent. A third fake shape (`_stub_runner`, fixed
`SubprocessCompletedProcess`, no process) lives beside the doubles. The
test suite is also the survey's only lifecycle-fault-injection suite:
`os.killpg` is monkeypatched to distinguish stale-group from live-group
signal errors, pinning the reap-race semantics; env inheritance,
replacement, and `-I` isolation each have a dedicated test.

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
partial-results-through-exceptions; stdout-protocol protection — though
the child's own docstring records the hole: direct file-descriptor writes
can still reach the protocol stream. Child-side details the parent
interpretation depends on: a three-tier exception taxonomy
(`AssertionError` → failed, `BaseException` → error, metadata-collection
failures degrade to clipped traceback strings without failing the case);
`failure_metadata` *re-executes candidate code* once per failed case to
collect actual/expected reprs — untrusted re-execution inside the timing
window; load-phase failure fans out one error result per case and exits 0
so item accounting survives total failure.

Budget axes — inherits the primitive's budgets wholesale; adds one protocol
budget:

- Result fields — per-field traceback clipping at 8000 chars
  (`FIELD_LIMIT`) in the child.

Observability — no narration and no run record, but two strong partial
properties: channel separation is the design's centerpiece (the child
reassigns `sys.stdout` to stderr so candidate prints cannot corrupt
protocol output), and per-case `elapsed_seconds` measurements plus
partial-results-through-exceptions preserve what happened in-memory even on
failure. Nothing survives the calling process — and the child emits all
results at exit, so a late-item crash loses the batch: noncompliant with
the results-leave-the-child-incrementally behavior.

Lifecycle — delegated wholesale to the primitive.

Attribution — the survey's most developed: `CANDIDATE_KILL_RETURNCODES`
({-SIGKILL, -SIGSEGV}) → candidate-attributed error; timeout → every case
TIMEOUT; output limit → every case ERROR; other nonzero exit, JSON parse
failure, shape violation, or unknown/duplicate case IDs →
`EvaluationHarnessError` carrying partial results. The working prototype
of the driver trust split and item accounting.

Amortization — one child per (candidate, task), shared across that task's
cases: an existing declared sharing boundary, with interpreter startup
amortized over the case dimension only.

## whetstone-ai

Provenance: this repo was rebuilt into a `rebuild/*` branch stack; the
code below lives on `archive/impl-11-before-eight-pr-rebuild` (partially
on `rebuild/11`/`12`), not on main, which currently holds only
`lm`/`eval_failures`.

### Codex exec transports

Two sibling shapes for running `codex exec` with model-authored prompts.
`src/whetstone/optimization/codex_runner.py:256` — codex itself spawns
the MCP server, wired via `-c mcp_servers.whetstone.command/.args/.env.*`
TOML overrides threaded one key at a time (so serialized JSON stays a
TOML string — a cross-process config-transport pattern); the server
imports its evaluator via a validated `module:callable` spec from
`WS_MCP_EVALUATOR` and *calls* it — arbitrary code loaded by env-var
path. `stdin=DEVNULL`, `env={**os.environ}` (a no-op overlay),
`shutil.which` pre-check, nonzero → `OpaqueStepError` embedding
`stderr[:2000]` (a *head* slice; the separate `stdout[-2000:]` tail is
success-path evidence, not error diagnosis). Results return through a
shared SQLite tool-call store the MCP subprocess reopens from
`WS_MCP_SQLITE_PATH` — durable-store-as-IPC, the survey's only instance
of state rather than output crossing the process boundary.
`src/whetstone/optimization/codex_proposer.py:146` —
`--output-last-message <tmpfile>` with a scratch-dir cwd; the invoker
never raises (timeout → typed `CodexInvocation(timed_out=True)`), but
the transport layer above it *does*: a three-way `CodexProposerError`
taxonomy for timeout, nonzero exit, and — notably — exit-0-with-empty
output, a named, documented failure mode (model-rejection HTTP 400
exits 0 and writes nothing). No `which` pre-check on the proposer: a
missing binary is an uncaught `FileNotFoundError`.

Budget axes — wall-clock only, leader-only enforcement:

- Wall-clock — runner 600 s; proposer `_DEFAULT_TIMEOUT_S = 180`; live
  smoke test 300 s; auth probe (`codex login status`) 30 s.
  `subprocess.run`'s timeout kills only the direct child.
- Output — unbounded capture; the proposer's file delivery is disk-backed
  and unbounded.
- Termination — none: no process-group handling, no escalation.

Observability — the child's JSONL event stream is live progress narration
the runner captures but never consumes during the run. The SQLite
tool-call store *is* a durable cross-process record; the proposer's
result tmpfile is transient.

Lifecycle — none beyond `subprocess.run` defaults: leader-only timeout
kill, no group, no escalation; n/a — no reap concern (call-scoped `run`).

Attribution — runner: nonzero → `OpaqueStepError` (exception-as-outcome).
Proposer: the three-way taxonomy above, with empty-output-as-signal
proving exit code alone is insufficient attribution.

Testing seams — three layered shapes: `CodexCliInvoker` is a real
Protocol seam with a documented contract ("MUST NOT raise on nonzero or
timeout") and a scripted fake; `#!/bin/sh` stubs on PATH exercise the
real spawn path, including an argv-recording stub that dumps `$@` and
asserts the safety flags are present (argv-as-contract testing); and
`FakeCodexRunner` is a scripted MCP *client* driving the real JSON-RPC
handshake — a protocol-level fake, not a process-boundary one.

### HumanEval oracle driver (broken)

`src/whetstone/envs/ed1m_oracle.py` and `envs/ed1_scoring.py` — a driver
program (`_DRIVER_SOURCE`, :46-63) that reads a JSON request from stdin,
`exec`s a model-produced reconstruction, and writes single-line JSON to
stdout, intended to run under dr-code's bounded primitive (the driver
lives in `ed1m_oracle.py`; `ed1_scoring.py` only imports the runner
surface). Currently nonfunctional: `dr_code` *is* installed — pinned to
commit `5810f30` — but the pinned wheel predates the module move to
`dr_code.execution.subprocess`, and the call site passes `input_json=`
where the current signature takes `input_text=`. Notable as prior art for
the batch-driver protocol shape, and as the survey's clearest example of
cross-repo drift: a *pinned but stale* execution dependency, where the
pin preserved the breakage instead of preventing it.

Budget axes — none of its own: intended to inherit the dr-code primitive's
budgets wholesale; the driver protocol itself imposes no field or result
bounds.

Observability — none: the single-line JSON response is the only artifact,
and the breakage itself demonstrates the cost — nothing recorded the drift
until the call site failed.

Lifecycle — n/a (broken). Attribution — n/a (broken); the intended design
inherits both from the dr-code primitive.

### Docker availability probe

`src/whetstone/runner/execution_mode.py:106` — `docker info` in bytes mode,
`shutil.which` pre-check, returncode collapsed to a boolean. Budget axes:
wall-clock 10 s; nothing else applicable. Observability: the boolean
collapse discards all diagnostic detail — and `OSError` and
`SubprocessError` (including the timeout) are also caught into `False`,
so even the 10 s budget produces no distinguishable signal. Lifecycle:
`subprocess.run` defaults. Attribution: the boolean collapse, nothing
finer.

## whetstone-envs

### Symlink-farm workspaces for a vendored generator

`src/whetstone_envs/c18/upstream.py:169` — `[sys.executable,
"run_experiment.py", ...]` drives a vendored generator whose output
filename is fixed, so each call gets a `TemporaryDirectory` populated with
symlinks into the read-only vendored tree (:143-146) as its `cwd`:
parallel calls cannot collide, and the shared tree stays untouched. The
result is read from a file whose name is *reconstructed* from the run
config (an explicit kept-in-lockstep coupling contract with the vendored
code) — file-delivered, not captured. (Provenance: this code lives on
unmerged `envs/*` branches/worktrees, not main.) Test-side patterns, all
in `tests/c23/test_upstream.py`: a reverse-then-forward `patch --batch`
round-trip against pinned SHA256s — the strongest vendoring-integrity
mechanism across the repo survey; full env replacement with constructed
`PATH`; and
`PYTHONHASHSEED` *randomization* (not pinning): the subprocess exists
precisely because the hash seed cannot vary in-process —
subprocess-as-environment-control. An 8-way ThreadPoolExecutor test pins
concurrency safety and global-RNG-state preservation.

Budget axes — wall-clock is a caller parameter (`timeout_s: float =
300.0`), rare across the surveyed repos; output captured unbounded (the result travels
by file, disk-backed and unbounded).

Observability — none live; on failure `UpstreamError` carries
`stderr[-500:]` — the recurring tail-slice pattern: a post-hoc excerpt
standing in for a record nothing kept in full.

Lifecycle — `subprocess.run` defaults, leader-only.

Attribution — two branches, not one: nonzero exit → `UpstreamError` with
the stderr tail, and exit-0-with-no-output-file → a distinct
`UpstreamError` — output-presence as an attribution signal, matching the
whetstone-ai proposer's empty-output case.

Amortization — a fresh symlink farm per call: cheap (symlinks, not
copies), a deliberate trade of per-call setup for parallel safety.

## fchord

### Timeout-bearing git helper with persisted failure records

`src/fchord/github_pull.py:186` (`_git`) — captured, text, `check=True`,
and — rare among the survey's git helpers — a timeout: `_GIT_TIMEOUT =
300.0` (:36), per *invocation*, and `_sparse_shallow_clone` makes three
(clone, sparse-checkout, rev-parse), so the effective clone bound is up to
900 s. Repo scoping via `git -C` argv; the checkout lives in a
`TemporaryDirectory` discarded on every path. Shell scripts: three are
pure `exec` process replacement; `parse-codex-session.sh` does
mktemp + `trap rm EXIT` + atomic `mv` + trap disarm — a complete
atomic-write-with-cleanup idiom in shell.

Budget axes — wall-clock only (module constant, per invocation not per
operation); no output bound.

Observability — the survey's clearest failure-as-persisted-record: clone
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
env-var path injection, not a runner seam. Note: `skill_dispatch.py` is a
pure-filesystem checker; dotfiles has no agent-CLI execution.

Budget axes — none: no timeout parameter and no seam to add one, on local
*and* network git calls; `git archive` output is materialized wholly in
memory with no cap. Observability — none: no narration and no run record;
stderr is routed to DEVNULL on the returncode-as-boolean paths and
discarded on the merge path. Lifecycle — `subprocess.run` defaults,
leader-only; inherited stdin with no `GIT_TERMINAL_PROMPT=0` means a
credential prompt hangs a merge forever with no kill path. Attribution —
three incompatible disciplines in one file: raw `CalledProcessError`;
returncode-as-boolean collapsing missing repo, corrupt object, and absent
commit into `False`; and every nonzero from `git merge-file` reinterpreted
as "conflict", so genuine errors (−1/128) masquerade as conflicts —
collapsed attribution three ways.

### pi extensions (TypeScript)

`zsh-user-bash.ts:31` — every model-issued command becomes
`exec <zsh> -fc <command>` passed as a *string* to pi's bash-based exec:
double-shelled by construction (bash parses the outer, zsh the inner),
with single-quote `shellQuote` correct for one level — a direct violation
of the argv-only behavior; bounding and kill
delegated entirely to pi. `copy-all.ts` — `spawn("pbcopy")` with stderr
accumulated into the rejection message and spawn-error distinguished
from exit failure, but no stdin error handler (EPIPE → unhandled) and no
timeout. `diff.ts` — `pi.exec` with a bare-literal 5000 ms timeout
repeated per site, called up to four times per flow with no aggregate
budget.

Budget axes — wall-clock only, and only in `diff.ts` (a 5000 ms literal
repeated per site, no aggregate budget); `zsh-user-bash.ts` delegates all
bounding to pi; `copy-all.ts` has none. Observability — none: no narration
and no run record; `copy-all.ts`'s stderr accumulation into a rejection
message is the only retained evidence. Lifecycle — delegated entirely to
pi for the zsh and `diff.ts` paths; `copy-all.ts` has no kill path and no
timeout, so a wedged `pbcopy` is never terminated. Attribution —
`copy-all.ts` distinguishes spawn error from exit failure (the one
distinction here); elsewhere attribution is pi's, unexamined.

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

Budget axes — wall-clock only: 30 s on the git helper, 60 s on the ruff
call, none at all on `gen-api.mjs`; no output bounds anywhere.
Observability — none live; the ruff path's documented null-vs-false
discipline is the only recorded signal, and the git helper's `None`
records nothing at all. Lifecycle — `subprocess.run` and `execFileSync`
defaults, leader-only; the only cleanup is `gen-api.mjs`'s
`try/finally rmSync` of the scratch dir.

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
code may *look like* (no classes, no globals) — shape-policing, which the
containment-constrains-reach-not-shape behavior rejects, for research
code.

Budget axes:

- Memory — `RLIMIT_AS` 256 MB default: a task-scale interior default —
  and *best-effort*: the whole setrlimit is try/excepted to a silent
  return, so a platform where it fails runs unbounded with no signal.
- Result size — 1 MB pickled-result bound, enforced as three sequential
  gates (sanitize through the type graph → pre-pickle → size-check), with
  the stated rationale that pre-serializing prevents queue feeder-thread
  errors from being *misattributed as timeouts* — the sharpest
  attribution reasoning in the survey.
- Wall-clock — per-execution timeout (default 1.0 s); the "startup
  timeout" is derived (`max(timeout_sec, 1.0)`), not independent.
- Input — unbounded: pickled args cross with no size validation, the
  file's clearest budget gap (1 MB out, unlimited in). Budget parameters
  themselves are carefully validated (bool rejected, inf/nan rejected).
- Termination — SIGTERM→SIGKILL with 0.2 s joins; leadership is checked
  only when the process is still *alive* — on an already-dead process
  `killpg` fires uncheck'd, a pid-reuse signal hazard in the cleanup
  path.

Observability — no capture of worker output (accidental passthrough to
the parent's fds); a module logger exists but speaks only DEBUG-level
swallowed-cleanup breadcrumbs; spawn-bootstrap diagnostics are a
three-part *inference* (CPython message match + `__main__` introspection
+ start-method check) ending in a remedy-bearing error naming the exact
env-var fix — the best startup attribution in the survey, though the
string match is version-coupled.

Lifecycle — TERM→KILL escalation with 0.2 s joins, degradable and
unprovable: `os.setsid()` failures are swallowed (group kill silently
becomes leader-only) and every teardown step is individually excepted to
DEBUG. Forkserver preferred over spawn (a second, unremarked amortization
boundary); the one-shot `_run_isolated` path — including its queue-feeder
grace drain — is dead code: only the persistent worker is reachable.

Attribution — six typed classes, but worker *crash* — the case that
matters most — escapes as bare `RuntimeError` with the exit code
interpolated into a string; exit-code discrimination exists
(crashed-with-code vs exited-without-result) but is untyped.

Amortization — `_PersistentWorker` reuses one containment-bearing worker
across calls: the survey's clearest warm-worker amortization.

In practice — the trust gate is ceremony: all eleven validator call sites
pass `trust_untrusted_code=True` unconditionally and none passes a
timeout or memory limit, so every real execution runs at the 1 s/256 MB
interior defaults — the categorization-is-declared behavior defeated at
every call site. And there are zero tests: 880 lines of signal, group,
and serialization lifecycle with no test file and no injection seam
beyond a start-method env var.

### Compiled-parity, formatting, and quality-check execution

Three more execution sites.
`verification/parity.py` — compile-once-run-many: a context manager
compiles generated Java/Rust once into a prefix-named TemporaryDirectory
and yields a frozen runner handle whose `command_prefix` the per-case
loop reuses — a second genuine amortization boundary; `shutil.which`
preflight raises one error listing *all* missing tools; per-case failures
are data (`ParityFailure` rows, sweep continues) while compile failures
raise — item-failures-as-data, artifact-failures-as-exceptions;
`_format_subprocess_error` distinguishes timeout from nonzero exit and
echoes the command; one 20 s constant shared by compile and run, no
output cap. `langs/formatting.py` — `lru_cache`d subprocess memoization
(2048 entries keyed on source — an amortization mode with no analogue
elsewhere); tools resolved by `which` at *import time* (PATH snapshotted
once); every failure — timeout, crash, parse — silently returns the
unformatted input with no log line: the purest failure-degrades-to-input
across the surveyed repos; file-as-return-channel via `--replace` +
read-back.
`generated_code_quality.py` (top-level, not under `langs/`) — 30 s
constant; five-tool preflight whose error names every missing tool *and*
the exact bypass flag; timeout converted to `SubprocessError` with output
attached (two-bucket taxonomy: tool-said-no vs tool-didn't-work); lint
strictness injected into the generated source (`#![deny(warnings)]`)
rather than the command line; failures accumulate across the sweep and
raise once, displaying the first 20 with a count of the rest — a rare
explicit cap on *diagnostic* size. Four uncoordinated wall-clock
constants now exist across one repo: 1 s, 15 s, 20 s, 30 s.

Budget axes — wall-clock only, as uncoordinated module constants: one 20 s
constant shared by parity's compile and run phases, 30 s in
`generated_code_quality.py`; no output caps anywhere; the one cap on
anything else is the diagnostic display cap (first 20 failures plus a
count). Observability — none: no narration and no run record; the
formatting path is the extreme case, silently returning unformatted input
on timeout, crash, or parse failure with no log line, while parity's
`_format_subprocess_error` echoes the command only into the raised error.
Lifecycle — `subprocess.run` defaults, leader-only; the only cleanup is
parity's prefix-named `TemporaryDirectory`, discarded when the context
manager exits. Attribution — mixed by site: parity distinguishes timeout
from nonzero exit and splits item failures (`ParityFailure` data) from
artifact failures (raised); `generated_code_quality.py` uses a two-bucket
tool-said-no vs tool-didn't-work taxonomy; `langs/formatting.py` collapses
timeout, crash, and parse failure into "return the input unchanged" —
collapsed attribution at its purest.

## dr-platform

### Spawn-context crash-recovery probe

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

Budget axes — wall-clock only, on two tiers: the child's self-polled 10 s
deadline and the parent's 20 s join, the margin sized so `terminate()`
cannot destroy the child's diagnostic traceback; no output or memory
bounds. Observability — the four durable DB signals are the record, and
the deliberately preserved child traceback is the narration; nothing
narrates the spawn itself. Lifecycle — one-step escalation: `terminate()`
followed by an *unbounded* join, no group targeting and no kill after the
terminate.

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

Budget axes — none: no timeout, no output bound. Observability — none: no
narration and no run record; the only artifact is the offending module
names on stderr, surfaced in the pytest failure message. Lifecycle —
`subprocess.run` defaults, leader-only; nothing outlives the call.

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

Budget axes — none: no timeouts anywhere, no output bounds (the probes do
not capture at all). Observability — none of the executor's own: with no
capture, failure detail lands only on inherited stderr, visible in
pytest's capture and absent from the `CalledProcessError`; no run record.
Lifecycle — `subprocess.run` defaults, leader-only; the `exec_module`
site runs in-process, so it has no child lifecycle at all. Attribution —
`check=True` raise-on-nonzero and nothing finer; the child asserts
in-process, so payload and executor failure are indistinguishable from
the parent's side, and the `exec_module` script's failure is
misattributed as a pytest collection error.

## dr-subs

### SSH remote worker invocation

`src/dr_subs/machines.py:121-198` (`_run_remote_worker`) — the survey's one
remote execution path: `Popen(("ssh", *_SSH_OPTIONS, source_id,
*_REMOTE_WORKER_COMMAND))` with the peer drawn from a closed enum (:51), a
JSON request on stdin, and the response read via `selectors` + `os.read`.
Companion probes: an ssh reachability check (DEVNULL, timeout 8) before
expensive work, and a `scutil` hostname query where failure *and*
unrecognized-hostname both degrade to `"local"`. The write-before-read
hazard is conditional: the
write is `BrokenPipeError`-guarded and ssh forwards stdin, so both sides
block only if the peer emits >64 KB of response before consuming the
request. The ssh options are themselves a nested transport budget:
`BatchMode` (no auth hang), `ConnectTimeout=5`, `ServerAliveInterval=15
× 2` (~30 s dead-peer detection inside the 3600 s wall-clock); the
remote command is absolute-path-pinned with `--no-sync` (no remote
dependency resolution).

Budget axes — wall-clock (default 3600 s) and response cap (64 MiB) are
keyword parameters — though no in-repo caller overrides them; the
deadline is *shared* across read and reap (`wait(timeout=remaining)`),
and the cap is enforced by reading one byte past it: overrun-detection
rather than silent truncation. Input unbounded.

Observability — `ProgressSink` narration threads through the whole path
(availability check, scan start, candidate counts, per-peer error-code
lines), and failures are recorded structurally as `MachineFailure(code,
message)` inside the scan report — failure-as-data at the report
boundary, not an escaping exception.

Lifecycle — `finally`-ordered cleanup: close selector → close pipes →
`_stop_process` (SIGTERM → 1 s → SIGKILL, single process, no group) —
pipe-close-first gives the child EOF before signals; whatever ssh spawned
remotely is out of reach entirely.

Attribution — a seven-code transport-aware taxonomy: ssh-level
(`ssh_timeout`, `ssh_unavailable`) vs peer-level (`peer_worker_
unavailable`/`timeout`/`failed`, `peer_response_limit`,
`peer_protocol_error`), plus `peer_unknown` constructed without any spawn.
Protocol validation is strict set-equality on response fields, version
match, and — strongest correctness pattern in the survey — the response
must *echo the request* (returned roots and config compared against what
was sent).

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

Budget axes — wall-clock only and only in places: a 15 s wait backstopping
the DevTools URL scrape, and `smoke.mjs`'s virtual-time budget; the CDP
clients have no response timeout at all, so a lost reply hangs forever; no
output bounds. Observability — none: no narration and no run record;
stderr is consumed as a scrape target for the `ws://` URL rather than
recorded, and `smoke.mjs` accumulates stdout only to feed downstream
content checks. Lifecycle — the weakest in the surveyed repos: cleanup is
called only from try/catch bodies, so no signal or exit handler exists and
Ctrl-C orphans the Chrome tree and its temp profile; kills are leader-only
`SIGKILL` with no wait, racing profile `rmSync` against Chrome's own
shutdown writes; `cap.mjs` has no kill at all.

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

Budget axes — wall-clock only, in `pi-image-run.sh`: the Perl SIGALRM
deadline plus a 5 s TERM→KILL escalation window; `run-codex.sh` has none.
No output bounds on either. Observability — asymmetric: `run-codex.sh`
spools a full trace per invocation (the repo's one run record), while
`pi-image-run.sh` narrates nothing and `eval "$(mise env)" || true`
silently proceeds under a wrong toolchain. Lifecycle — `pi-image-run.sh`
does TERM → 5 s → KILL on a single pid with no `setsid`, so pi's tool
subprocesses survive; `run-codex.sh` has no lifecycle handling at all.
Attribution — `pi-image-run.sh` reconstructs shell-convention exit codes
(128+N) on the normal path but exits 124 from inside the SIGALRM handler,
so a deadline kill can never report the child's actual signal;
`run-codex.sh` discards attribution entirely — it always exits 0 and
aggregates no per-document status.

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

Budget axes — output only, and only by accident: `gen-graph-schema.mjs`
inherits Node's default 1 MiB `maxBuffer`; no timeouts anywhere.
Observability — none: no narration and no run record; on the
`stdio:'inherit'` path output goes to the operator's terminal and is not
retained, and `gen-graph-schema.mjs` feeds raw stdout straight into
`JSON.parse`, so any uv warning corrupts the result with no trace.
Lifecycle — `spawnSync`-family defaults, leader-only; nothing outlives the
call, and no kill path exists. Attribution — broken on the inherit path:
`.trim()` on a null `stderr` crashes the reporter with a TypeError instead
of reporting, missing-binary (`status null`) hits the same crash, and
`result.error` is never consulted; `gen-graph-schema.mjs` has no error
handling at all.

### Vercel install wrapper

`scripts/vercel-install.mjs` — the survey's cleanest JS injection seam:
environment, spawn function, and output sinks all defaulted parameters
with typed JSDoc contracts, fully drivable without a real process.
Redaction is thoughtful but bounded: the token is redacted in raw *and*
percent-encoded forms, but per-chunk (a token split across stream chunks
leaks), and it still lives in the child's env — visible to every
descendant of `pnpm install`; the parent-env delete prevents
re-forwarding, not exposure. Attribution: exit code preserved in the
message, but the spawn-error path discards the underlying error for a
fixed string (ENOENT and EACCES indistinguishable). No timeout.

Budget axes — none: no timeout, no output bound. Observability — output
sinks are injectable parameters, so the caller chooses the destination,
but nothing is narrated or persisted by the wrapper itself; redaction is
executor-side and per-chunk, so a token split across stream chunks leaks
into whatever sink the caller supplied. Lifecycle — `spawn` defaults,
leader-only: no kill path and no escalation, so every descendant of
`pnpm install` outlives a wedged run.

## nlae

### Two-transport fetcher with filesystem reconciliation

`src/nlae/arxiv_library/fetch.py` — a two-backend design: `Transport.AUTO` picks gcloud iff `shutil.which` finds it,
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

Budget axes — inverted across the two backends: the in-process httpx path
carries a 60 s timeout plus retries, while the spawning gcloud path has no
timeout and no capture at all; no output or input bounds on either.
Observability — none from the spawn: gcloud's stdio is inherited and
uncaptured, so progress is the operator's terminal and nothing is
recorded; the destination-directory re-listing is the only durable
evidence of what happened. Lifecycle — `subprocess.run` defaults,
leader-only, with no timeout to trigger even that; the `.part` +
atomic-rename convention (httpx path only) is the sole cleanup
discipline.

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
discard `TimeoutExpired`'s recovered partial output — executor-side
redaction of the kind the faithful-record behavior forbids, and it empties
the failure message. `latency_ms` is a
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

Budget axes — almost none, and one that lies: captured bytes with no
timeout on the docker runner, no timeout on the psql restore, and no
output bounds anywhere; `wait_docker_ready`'s `timeout_seconds` is an
*iteration count* over untimed `docker exec` calls while its sibling
`wait_dsn_ready` is a true monotonic deadline — same parameter name,
different semantics. Observability — none of the executor's own: no
narration and no run record; psql's stdout goes to DEVNULL so restore
diagnostics are stderr-only, and the one narration instance is
`docker_swap_in_db`'s cleanup failure reported via `exception.add_note`.
Attribution — classification by stderr *substring* into five typed errors
plus a fallback, with the returncode never inspected: locale- and
version-fragile, and call sites layer further ad-hoc string checks on top
("already running" → success). Retriability in the readiness loop is
derived from those same substrings. Tool-missing is handled three
different ways in one repo with no `shutil.which` preflight anywhere.

## nl_latents

### Containment tripwire and provider worker scripts

`evaluation_runner.py:184-213` — the survey's one caller-side containment
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
required config; and the survey's highest-fidelity command narration —
`printf ' %q'` logging each launched command shell-quoted and
re-runnable.

Budget axes — none on execution: no wall-clock, output, or memory bounds
on the docker path or the worker runner; the only declared limits are
admission-shaped — per-provider worker counts and retry policies as
env-defaults, with rate-limit-derived concurrency ceilings.
Observability — the `printf ' %q'` command narration is the strength (each
launched command logged shell-quoted and re-runnable), and the containment
tripwire's `git status --short` snapshots are a filesystem-witnessed
pre-flight record; nothing persists a run record beyond that.
Lifecycle — the bash runner is a hand-rolled process-group manager: pids
array plus background jobs plus a bare `wait`, with cleanup as `kill` —
TERM only, no groups, no escalation, and the trap wired by the sourcing
caller rather than the script itself. Attribution — the containment
tripwire refuses pre-execution when the working tree changed (a declared
verdict with the filesystem as evidence); `: "${VAR:?}"` fail-fast
attributes missing required config before any spawn; beyond those, failure
attribution rides bare exit codes.

## dr-cognee

### Vendored mirror copy

`src/dr_cognee/vendored/github_docs_mirror.py` — verified byte-identical
to dr-notion's file except a one-line vendoring header ("keep edits
upstream"); no drift, and actively imported (committed `__pycache__`).
Every dr-notion finding applies at a +1 line offset.

Budget axes — n/a — byte-identical vendored copy; dr-notion's findings
apply unchanged. Observability — n/a — same. Lifecycle — n/a — same.
Attribution — n/a — same.

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
`validators/import_resolve.py` (`find_spec` on the root of each dotted name
only — parent packages are never imported; relative imports are skipped
entirely).

Budget axes — wall-clock only:

- Wall-clock — per-instance `timeout_s` (default 5 s); on expiry, partial
  stdout/stderr are recovered from `TimeoutExpired`.
- Output and input — unbounded.

Observability — no narration and no run record, but the result
envelope is diagnosis-friendly: `tool_found`, `timed_out`, `duration_s`,
and timeout-recovered partial output all arrive as data. Nothing
persists — and ty's persisted diagnostics reference tempdir paths that no
longer exist by the time they're read.

Lifecycle — `subprocess.run` defaults: leader-only timeout kill, no group
handling; no `env=` and no `cwd` at all, so ty resolves its config from
the *tempdir's* ancestry, not the project.

Attribution — the envelope discriminates, but consumers throw the
discrimination away: both ruff helpers branch on `res.ok` alone, so
missing tool, timeout, and parse error collapse into one warning
diagnostic; `returncode=-1` means three different things (not-found,
timeout, or a real signal death); `ruff check --exit-zero` deliberately
neutralizes the exit code. Two bright spots: diagnostics carry a
`DiagnosticSource` tag (SUBPROCESS vs NORMALIZER) — layer attribution as
persisted data — and tool absence is modeled as `success=True` with a
no-op result plus a warning: absence as a *correct* outcome, distinct
from failure.

Testing seam — constructed `SubprocessRunner` instances passed to
consumers (configuration-as-injection) — but injection is optional with
silent default-construction fallback. One acknowledged TOCTOU: the L5
path `which`es ty twice with an explicit "ty disappeared mid-run"
branch.

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

Budget axes — no run timeout (unbounded by design; the transcript's
unbounded growth is a deliberate grant), but not budget-free: a
process-collection-level `CONCURRENCY_CAP = 3` admission budget, and
stuck-detection
via the transcript file's *mtime* compared against a configured
threshold — file mtime as an inferential heartbeat, the only
wall-clock-ish bound on a run. One hole: an untimed, uncheck'd `git`
call sits inside the spawn path.

Observability — every lifecycle transition
(spawn, resume, state change, kill) is journaled as structured events in
SQLite — arguably the survey's best lifecycle narration — and the
transcript is spooled live and durable (stdout and stderr multiplexed
into one stream file, non-JSON stderr lines tolerated by a lenient
parser: lossy-by-design). The pid registry is a durable ownership record
for claude runs only — every codex run stores the *shared app-server
pid*, so the column means two different things.

Lifecycle — group-targeted SIGTERM, no escalation, no wait, no reap; the
pid-reuse guard is absent (`os.kill(pid, 0)` liveness,
`PermissionError` treated as alive).

Attribution — exists, just not exit-code-based: run outcome is read from
the transcript's `result` event (`is_error` → FAILED, else COMPLETED),
with the liveness probe as fallback for vanished runs — in-band exit
observation. Codex runs get a restart-aware mapping table:
`TurnState.UNKNOWN → FAILED` with the comment "the daemon restarted
mid-run" — inferring run death from the supervisor's own amnesia, unique
across the surveyed repos.

### Codex app-server JSON-RPC singleton

`src/symphony_lite/codex_appserver.py:67` — a long-lived singleton child
(`codex app-server`, stdin=PIPE, stdout=PIPE, stderr=append-mode log
file, `start_new_session=True`) speaking bidirectional JSON-RPC: a reader
thread demultiplexes responses by id into per-request queues; a write
lock serializes stdin. Cancellation is protocol-level: `kill_codex_run`
sends a `turn/interrupt` RPC rather than a signal — in-flight work
cancelled without touching the process (though when the daemon restarted
and no `turn_id` is known, the run is journaled KILLED *without any
interrupt sent* — a kill that didn't kill). Flaws: `_read_loop` iterates
stdout with no size bound; `_pending` has two unlocked accesses, and the
reader thread's check-then-index race means a timeout popping the entry
between them raises an uncaught `KeyError` that kills the demux thread
permanently — every later request then times out; the stdin write is
unguarded against `BrokenPipeError`; a crashed child is never reaped or
restarted; `stop()` is leader-only `terminate()`; the append-mode log
handle is never closed (an fd leak per start); sandbox and approval
policy are hardcoded constants (`workspace-write`, `never`) — hardcoded
where the declared-profile behavior requires caller declaration. No testing seam
anywhere in the execution paths.

Budget axes — per-request wall-clock 60 s (interactions budgeted while
the child is not — the supervised interactions-are-budgeted shape); no
output bound on the RPC stream.

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
expressed in argv — the survey's only resource bounding done by asking for
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

Budget axes — one, and it is not a kill: bandwidth bounded by protocol via
`--depth 1 --filter=blob:none --sparse`, expressed in argv; no wall-clock
timeout on the clone and no output bound. Observability — none: the clone
is uncaptured, `run_git` captures but propagates raw `CalledProcessError`
whose stderr is absent from `str(exc)`, and nothing is persisted.
Lifecycle — `subprocess.run` defaults, leader-only, with no timeout to
trigger even that; the fixed, unlocked checkout path is the only
persistent state, and concurrent mirrors race on it. Attribution —
raise-on-nonzero and nothing finer: failures surface as "exit status 128"
with git's actual error invisible, and a credential prompt on a private
URL blocks forever rather than failing at all.

## dr-dspy

### Deno capability-sandboxed Python interpreter

`dspy/primitives/python_interpreter.py` (vendored) — the survey's one
non-container containment mechanism: a persistent Deno process running Pyodide (Python
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
no narration, no run record; silent restart of a dead sandbox is
exactly the silent-replacement pattern the supervised behaviors forbid.

Lifecycle — persistent child with restart-on-death (`_ensure_deno_process`
polls before each use); no kill path, no escalation, no reap of the
replaced process (shutdown-deliberate-and-complete requires reaping).

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

Budget axes — a readiness timeout only (parameter, default 1800 s); no
execution cap, and the `logs_buffer` the tail thread appends to is
explicitly unbounded. Observability — every child line is printed to
console until the ready event and retained in the in-memory
`logs_buffer`, reachable only through a monkey-patched `lm.get_logs`; no
run record and no narration of the lifecycle itself. Attribution —
none: readiness-timeout failure is the only distinguished outcome, and
because the child is never reaped or restarted a crashed server is
indistinguishable from a slow one.

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

Budget axes — wall-clock only, and it never reaches the caller: the
ancestor has no output cap branch and does not carry the timeout value
into messages or results. Observability — none: no narration, no run
record, no per-case timing (all added by the rewrite), and no
partial-result-carrying exceptions, so a failure loses what the batch had
already produced. Lifecycle — raw inline `subprocess.run` defaults,
leader-only, with no injection seam; `runtime.py:37` additionally forces
the multiprocessing start method globally with `force=True` under a
suppressed `RuntimeError`.

## llmflow

### Bash tool executor (TypeScript)

`packages/runtime/src/tool-executor.ts` — model-issued commands via
`bash -lc` — an argv-only violation by design (the tool's contract is
shell execution) — with `cwd`, SIGTERM → 1 s → SIGKILL (leader only). Notable
property: `BASH_TOOL_CONTRACT` (`packages/core/src/tools.ts:84-130`) — tool
schema, approval requirement, timeout defaults/ceilings, and output limit
co-located in one frozen contract object. Weaknesses: login-shell
semantics; leader-only kills.

Budget axes — wall-clock and output, both pinned in the contract object
(10 s default, 60 s ceiling, 16 KiB output):

- Wall-clock — over-ceiling requests are *clamped*, never rejected — the
  schema's `.max()` is unreachable; a model asking for 300 s silently
  gets 60.
- Output — `cappedAppend` re-materializes the full concatenation per
  chunk: output is bounded but the *work* is not — a capped runaway child
  still costs unbounded allocation, and it is the aggregate-in-memory
  antipattern. Truncation is silent — noncompliant with the
  visible-truncation behavior (overflow policy: failure or visible
  truncation, never silent loss).
- Termination — SIGTERM then a 1 s SIGKILL timer that is `unref`'d: if
  the event loop has nothing else pending, Node exits before the SIGKILL
  fires — best-effort escalation dependent on unrelated loop state.
- Timeout discards evidence: the rejection carries only the timeout
  value; all accumulated stdout/stderr is thrown away.
- `exitCode ?? 1` renders signal deaths unrepresentable (the `signal`
  argument is never read). Stdin is explicitly closed — a deliberate
  anti-hang measure. No `env` control; `-lc` sources the operator's rc
  files. The bash side has *no* injection seam (module-private const),
  while the codex side has a clean one — two opposite testability
  stances in one repo.

Observability — the silent `cappedAppend` truncation is the section's
anti-pattern: information loss with no marker, so a capped transcript is
indistinguishable from a complete one — noncompliant with the
visible-truncation behavior (overflow policy: failure or visible
truncation, never silent loss). No executor narration or run
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

- Wall-clock — `CODEX_COMMAND_TIMEOUT_MS = 60_000`, a hardcoded module
  constant with no override path.
- Output — unbounded stdout accumulation.
- Input — prompt via `stdin.end()` with no error handler (EPIPE
  unhandled).

Observability — the `--json` event stream is parsed and *thrown away*: a
four-key text-extraction heuristic returns "" on any parse failure;
error events, tool calls, and token counts are silently discarded — the
opposite of dr-llm's in-band error handling. No narration, no record.

Lifecycle — SIGTERM only, leader only; the spawn-error path performs no
kill at all. Argv pins a fully non-interactive containment profile:
`--sandbox read-only --ask-for-approval never --ephemeral` — the survey's
strongest profile-in-argv instance.

Attribution — two-tier exit policy: nonzero → error with stderr as the
message; exit-0-with-stderr → a warning event. Testing seam — a typed
optional `runCommand` constructor parameter with a real default: the
cleanest JS seam beside vercel-install's.

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
the faithful-record behavior: output that is itself sensitive is a domain
fact only the caller can know, so redaction is caller-side and
post-capture; the executor records verbatim. (llmflow's in-flight
`redactToken` is the survey's one executor-side redaction instance — the
approach dr-exec deliberately does not take.) Lifecycle — `subprocess.run`
defaults, leader-only; nothing outlives the call. Attribution —
`check=True` raise-on-nonzero, nothing finer.

## dr-queues

### Detached stage workers with a two-channel stop protocol

`src/dr_queues/runtime/lifecycle.py:126` (`start_stage_workers`) —
`Popen(cmd, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL,
start_new_session=True)`; command prefix via `shutil.which` with
`sys.executable -m` fallback; grace-period sleep then `poll()` as a
startup liveness check. A second variant
(`pipeline/runner.py:296`) spawns one worker per stage with inherited
stdio. The design's shape: supervision is deliberately relocated off the process handle
and into the datastore, precisely *because* `start_new_session` detaches.
Workers self-register (`WorkerRecord` with pid/host/status), heartbeat
the DB every 2 s via a daemon thread, and mark themselves stopped in
their own `finally`. Stop is two-channel and host-aware
(`stop_workers`, :180): a DB stop-request flag as the primary,
cross-host, cooperative channel (the heartbeat thread notices
`STOP_REQUESTED` and stops the pool), plus a same-host-only `os.kill`
SIGTERM accelerator gated on `record.host == current_host()`; worker
side wires SIGTERM/SIGINT into `pool.stop()` with a bounded
`pool.join(timeout=5)`. `replace_stage_workers` is stop-then-start; both
verbs are CLI-exposed, as is `workers --json`.

Budget axes — no execution timeout on workers, but teardown is bounded:
`pool.join(timeout=5)`, tap-thread join 5 s, and a `TimeoutError` on
pipeline completion.

Observability — stdio is DEVNULL'd but observability lives in the DB:
registration, 2 s heartbeats, stop marks, a `list_workers` API and JSON
CLI view — a durable, queryable worker registry instead of streams.

Lifecycle — cooperative-first: SIGTERM is the *terminal* action (no
SIGKILL escalation), `stop_workers` returns without confirming death,
nothing is ever reaped, and a wedged non-heartbeating cross-host worker
is unkillable through this path; `PermissionError` on signaling is
swallowed to stderr.

Attribution — worker status lives in the DB record lifecycle
(registered/heartbeating/stopped), not in exit codes; the parent never
observes exit.

Testing seam — monkeypatched `Popen` in `tests/test_lifecycle.py`.

## marimo_utils

### Tailwind shell-string build and pre-check script

`styles/build.mjs:12` — the survey's one shell-string invocation
(`execSync("npx tailwindcss -i ./input.css -c ./tailwind.config.js
--minify")`) — the survey's one argv-only violation in first-party build
code: the entire minified CSS transits stdout under Node's
*default 1 MiB `maxBuffer`* — the one place across the surveyed repos
where the unset default is a realistic failure mode, not theoretical;
stderr inherits,
so a tailwind error throws without diagnostics in the exception. No
timeout. `scripts/pre-check.sh` — near-twin of dr-code's with a marimo
step added and a strict check commented out in place; its `run_report`
is a dual-delivery pattern (tee to terminal + durable per-check artifact
under `.cache/pre-check/`, true status recovered via `PIPESTATUS[0]`),
`set -uo pipefail` *without* `-e` deliberately enabling failure
aggregation — while `run_silent`'s four autofix steps propagate no
status at all and can fail silently.

Budget axes — output only, and only by accident: Node's default 1 MiB
`maxBuffer` on the tailwind build, which the minified CSS realistically
approaches; no timeouts anywhere. Observability — `pre-check.sh`'s
`run_report` is the strength: dual delivery, tee to terminal for live
progress plus a durable per-check artifact under `.cache/pre-check/`, with
the true status recovered via `PIPESTATUS[0]`; the tailwind build records
nothing and inherits stderr, so a tailwind error throws with no
diagnostics in the exception. Lifecycle — `execSync` and shell defaults,
leader-only; no kill path, no escalation, nothing outlives the call.
Attribution — `set -uo pipefail` without `-e` deliberately enables
failure aggregation across checks, and `PIPESTATUS[0]` preserves the real
per-check status; but `run_silent`'s four autofix steps propagate no
status at all, and the tailwind failure arrives as an exception stripped
of the child's diagnostics.

## nl-code

### Container-gated execution worker

`src/nl_code/code_execution/worker.py` — `exec` of model-generated code
behind `_require_docker_execution`, which raises unless an env var is set
*and* `_is_running_in_container()` verifies the claim: the origin of the
containment-is-verified behavior. In-container defenses are
mode-dependent: the AST denylist guards only `function_call` mode
(assertion and unittest modes `exec` with no AST validation), and
per-item `setitimer`/SIGALRM deadlines exist only in *batch* mode —
single-item modes have no in-worker wall-clock at all. Five rlimits
applied (CPU, AS 256 MB, FSIZE, NOFILE, NPROC), so memory is bounded
twice (rlimit inside, `--memory` outside). Caller side
(`code_execution/runner.py`) delegates spawning to
`dr_docker.SubprocessDockerAdapter` and translates its envelope into
`CodeExecutionInfrastructureError` with a **12-value `stage=` taxonomy**
(worker_nonzero_exit, docker_timeout, …) — though one stage is derived by
stderr substring-matching, and the exit-code channel is discarded
(`exit_code or 0`; the worker returns 0 on unexpected exceptions), so
crash-vs-success is distinguishable only by payload fields.

Budget axes — layered, with derived formulas on both sides: the batch
container timeout is `timeout_per_item × N × 1.5 + 10` while the worker
independently widens its CPU soft limit to `used + timeout_per_item × N`
(usage-relative, restored in a `finally`) — same inputs, different
formulas, opposite sides of the boundary. Caller stdin cap 50 MiB
(env-overridable); stream caps 1 MiB single-item but
`max(10 MiB, 50 KiB × N)` for batches; chunking at 200 items.

Observability — structured stage codes plus `retriable`; `stderr[:200]`
head-slices in two messages; no narration or run record.

Lifecycle — delegated to the adapter; inside the worker, per-item
`os.chdir` + directory wipe between items — per-item filesystem hygiene
inside the shared container.

Attribution — the container gate refuses pre-execution; AST rejection is
a payload verdict (one mode only); everything else rides the stage
taxonomy.

Amortization — container at the batch dimension, deadline at the item
dimension, with per-item scratch hygiene as the sharing boundary's cost.

Testing seam — effectively none: `_import_dr_docker()` is called lazily
nine times and its 9-tuple indexed positionally; faking requires patching
module imports.

Novel — the budget crosses the process boundary *as data*: the caller
serializes its limits via `JsonWorkerExecutionConfig.to_env()` into
container env, and the worker rehydrates the same frozen model with
`from_env()` — one contract object, two processes. Worker delivery is a
fourth program-transport mode: the script bind-mounted read-only and run
as `python3 -I /sandbox/worker.py`.

## dr_exp

### Slurm worker fleet launcher

`src/dr_exp/worker/launcher.py` — the survey's most complete supervised
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
  disk-backed delivery the budgets principle prefers.
- Per-worker/per-job wall-clock — none at this layer.

Observability — the survey's richest: narration to console and a launcher
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
repo's own `dr_exp` CLI via `subprocess.run(capture_output=True)`.
Self-invocation as an API: the process boundary substitutes for a library
call. The `check=` split is coherent, not mixed: `check=True` for probe
calls, `check=False` for submissions. Success is returncode-gated; only
the job *identity* is string-parsed ("Job submitted with ID:") — and when
the marker line is absent, submission is still logged as success with
`job_id or "unknown"`: a silent identity-degradation bug. Duplicate
detection parses `job list` output with a bare `except` that degrades to
an empty set — duplicate protection fails open. No timeouts anywhere.
Budget axes — none. Lifecycle — `run` defaults. Observability —
`SubmissionLogger` writes a durable timestamped JSON
append log per experiment (config/seed/job_id/success/error), read back
as the *idempotency key set* on re-runs, and the summary generates a
copy-pasteable retry command — submission is idempotent-by-persisted-log
rather than by server state, one of the survey's better run-record
patterns. Attribution — returncode-gated success with a coherent
`check=` split (`check=True` for probes, `check=False` for submissions),
but two collapses below it: an absent "Job submitted with ID:" marker
still logs success with `job_id or "unknown"`, and the duplicate-detection
parse degrades through a bare `except` to an empty set, so duplicate
protection fails open.

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

Budget axes — none: no timeouts anywhere and no output bounds; the only
admission-shaped control is the dry-run gate and queued-jobs-only guard
before destructive `job remove` calls. Observability — none of the
executor's own: the re-invocation wrappers inherit stdio so progress is
the operator's terminal and nothing is retained, and the helper that
collapses to `False` discards stderr; no run record. Lifecycle — `run`
defaults, leader-only, with no timeout to trigger even that; each job is a
fresh process in a loop paying full `uv run` startup, and nothing outlives
the call. Attribution — the wrappers propagate the child's exit code
faithfully via `sys.exit`, but the cross-repo helper collapses launch
failure and job failure into a bare `False` with stderr discarded, so
absence and payload failure are indistinguishable.

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
`print(flush=True)` narration; one run record,
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
thread joined without timeout, unvalidated env values and command entries
(image, limits, and paths are pydantic-validated), no plain-argv
(non-Docker) entry point. `cleanup.py:21` is a best-effort `docker rm -f`
with output discarded and no timeout.

Budget axes — the survey's widest set, mostly containment-backed:

- Wall-clock — a true monotonic *deadline* spanning both phases:
  `remaining` recomputed per select iteration and again to bound the
  final `proc.wait()`.
- CPU time — cpu ulimit `max(1, ceil(timeout_seconds))`, a formula
  independently duplicated in `json_stdio.py` — one rule, two
  implementations, no shared constant.
- Memory/processes/files — `--memory`, `--cpus`, `--pids-limit`, ulimits;
  but *three different default sets* coexist (contract 256m/64 pids,
  worker policy 512m/256, in-container config 256MB/64) — a live drift
  surface — and `DR_DOCKER_WORKER_SKIP_LIMITS` is an ambient off-switch
  for the CPU bound.
- Output — per-stream caps; but the truncation marker is appended
  *in-band* into the stream text with no `truncated` field on the result,
  so a JSON-parsing consumer sees a parse failure, not a truncation
  signal (the in-container `BoundedTextCapture` does it right with a
  proper boolean — same package, two policies).
- Input — bytes stdin written by a daemon thread with `BrokenPipeError`
  suppressed twice: a child that never reads cannot deadlock the writer,
  but non-delivery is silently discarded.
- Termination — three unbounded `kill(); wait()` pairs, and the `finally`
  cleanup's `docker rm -f` has no timeout — the timeout-recovery path is
  itself unbounded.

Observability — true byte totals are tracked so markers report real
sizes; the adapter's module logger is entirely dead (never called in 362
lines); real logging exists only in cleanup/cidfile. No run record.

Lifecycle — split-brain: container-side teardown is the strength — `--rm`
in argv *plus* cidfile-tracked `docker rm -f` in `finally` (belt and
braces), with the cidfile dance (private 0o700 dir, reserve-then-unlink
because docker refuses existing paths) and a guarded reclamation that
refuses to delete a directory it cannot prove it created. Client-side is
weak: bare leader-only `kill()`, no escalation, unbounded waits.

Attribution — a three-code taxonomy (TIMEOUT/UNAVAILABLE/INTERNAL_ERROR)
that is lossy: payload nonzero-exit and executor RuntimeError share
INTERNAL_ERROR; signal deaths undistinguished; `retriable=True` is set on
exactly one path (timeout). The envelope's ok/error invariant is
*structurally enforced* by a model validator, and `adapters.py`
re-validates it defensively with machine-readable violation tags.

Amortization — `batching.py` is a first-class
batch-amortization primitive — one container for many jobs with strict
result-count alignment (misaligned results refuse to return), and
`run_batch_with_failure_isolation` *recursively bisects* a failing chunk
to isolate the poison item, so one bad payload doesn't lose the batch —
dr-docker already solved the incremental-survival problem for the
batch-level case. Per-call container setup remains the cost when batching
isn't used.

Testing seam — `RuntimeAdapter` is a `@runtime_checkable` Protocol: a
first-class injection seam. `json_stdio`'s config round-trips
caller-declared budgets through env with parsers that raise on malformed
values — while nl-code's own env parsing warns-and-defaults: two policies
for the same operation across the package boundary.

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

Budget axes — none: zero `timeout=` across all 44 sites and no output
bounds, which is exactly what makes `test_performance.py`'s wall-clock
assertions unenforceable. Observability — none of the executor's own: no
narration and no run record; captured stdout/stderr is consumed only by
per-test assertions and discarded. Lifecycle — `subprocess.run` defaults,
leader-only, with no timeout to trigger even that; every call pays the
full two-level `uv run` → `python` tree, and all 44 share the repo working
tree with no per-test isolation. Attribution — uniform and deliberate:
`check=False` with the returncode asserted per test, so exit status is
data rather than an exception — but with no timeout, a hang is
indistinguishable from progress and never attributed at all.

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
