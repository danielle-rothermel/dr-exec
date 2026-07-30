# Subprocess usage audit — fleet facts

Audit of all 46 repos under `~/drotherm/repos`, conducted 2026-07-30 by seven
parallel read-only agents. First-party code only; `.venv`, `node_modules`,
vendored skill packs, and data directories excluded. File:line references are
against working trees as of the audit date. Facts only; no recommendations.

## Reference implementation

`dr-code/src/dr_code/execution/subprocess.py` (branch
`rebuild/01-subprocess-execution`, 345 lines):

- Bounded stdin: 4 MiB, text-only (UTF-8 encoded). Bounded output: 1 MiB
  shared across stdout+stderr; overflow raises `SubprocessOutputLimitError`.
- Wall-clock deadline required (finite, positive). `start_new_session=True`;
  on every exit path `os.killpg(SIGKILL)` with a completion-race retry, 5 s
  bounded termination wait, 1 s bounded IPC-thread joins.
- Typed errors: `SubprocessError` → `Timeout` / `OutputLimit` /
  `Infrastructure` / `Start`. Argv validation (nonempty, no NUL, no shell).
  Environment: inherit (None) or full replacement (validated mapping).
  Text-only decode, `errors="replace"`. Raw returncode returned, including
  negative signal values; never raises on nonzero exit.
- `run_python_subprocess`: `sys.executable -I -c <source>` with environment
  `{"OPENBLAS_NUM_THREADS": "1"}`. Injection seam: `PythonSubprocessRunner`
  Protocol. Limits are module-level constants, not parameters.
- Not present: `cwd`, streaming/inherited stdio, bytes I/O, per-stream caps,
  truncate-instead-of-error, rlimits, raise-on-nonzero mode, env overlay,
  optional/absent stdin, per-call limit parameters, retries, async, pty.

## Repos with no first-party process execution

parse_agents, parse_hepta, parse_papers, parse_roam, dr-hf, dr-store,
dr-serialize, dr_wandb, workspace (Chrome extension only), infer-explore,
skill-lens (starts Vite in-process via `createServer()`), dossier-grid,
gen-viewer (renders generated code, never executes it), explainers (docs
only), by-tomorrow-app (no subprocess; `python-backend/main.py:16` calls
`pd.read_pickle` on a committed first-party artifact at import time).

parse_git reaches git through the GitPython library API
(`pyproject.toml:11`, `src/parse_git/repository.py:23,38,116,132`); no
first-party spawn, no raw-command escape hatch. dr_widget:
`src/dr_widget/inline/runtime_loader.py:14-26` reads a prebuilt `runtime.js`
from disk and inlines it into a `<script>` tag (guards `</script>`); the
build command is never invoked from Python.

## dr-code (outside the execution module)

- Tests: `tests/test_schemas.py:10` (`uv run python -m dr_code.schemas`,
  `check=True`); `tests/trace/test_import_hygiene.py:26,59`
  (`sys.executable -c`, `check=True`, no timeout);
  `tests/synthetic/test_cli.py:21` (`cwd=` repo root);
  `tests/execution/test_subprocess.py:32` (`-I -c` with
  `env={"PYTHONPATH": ...}` replacement).
- Two near-identical ~25-line `local_runner` test doubles for
  `PythonSubprocessRunner`: `tests/metrics/helpers.py:153` and
  `tests/humaneval/test_humaneval_primitives.py:167` (unbounded output, no
  group kill, `TimeoutExpired` → `SubprocessTimeoutError`).
- `src/dr_code/humaneval/batch_runner.py`: sole production consumer.
  `CANDIDATE_KILL_RETURNCODES = {-SIGKILL, -SIGSEGV}` drives
  candidate-versus-harness attribution; timeout → all cases TIMEOUT; output
  limit → all cases ERROR; other nonzero exit / JSON parse failure / shape
  violation / unknown-or-duplicate case IDs → `EvaluationHarnessError`
  carrying partial results.
- `src/dr_code/humaneval/batch_runner_script.py`: child-side program run via
  `-I -c`; reads one JSON value from stdin; reassigns `sys.stdout` (and
  `sys.__stdout__`) to stderr, keeping a private handle for protocol output;
  per-case `exec` with clipped tracebacks (`FIELD_LIMIT = 8000`); load-phase
  failure emits error-for-every-case and exits 0. Shipped as resource text
  (`importlib.resources`), never imported. `exec`/`eval` sites at lines
  42, 43, 64, 77, 117 execute candidate code (this file is the payload).
- `src/dr_code/code_analysis.py:111`: `compile()` for syntax validation only.
- `scripts/pre-check.sh`: bash driving `uv run ruff/ty` and `pnpm`, tee'd
  streaming, exit-status aggregation, no timeout. Near-verbatim twin exists
  in marimo_utils.

## parse_claude

46 call sites, all in tests, all invoking the package's own CLI. Shape A
(42 sites): `["uv","run","python","-m","parse_claude",...]` across
`tests/test_e2e_workflows.py`, `tests/test_documentation_accuracy.py`,
`tests/test_edge_cases.py`, `tests/test_performance.py`. Shape B (4 sites):
`[sys.executable,"-m","parse_claude",...]` in `tests/test_format_defaults.py`.
Uniform axes: no stdin, `capture_output=True`, text, `cwd=` repo root, no
env, **zero `timeout=` args**, `check=False` with returncode asserted.
`test_performance.py:151,169,204,232` assert wall-time thresholds measured
with `perf_counter` after completion. Shape A spawns a two-level
`uv run` → `python` process tree; no process-group handling anywhere.
`pyproject.toml:117` globally suppresses Ruff S603/S607.

## dr-llm

- `src/dr_llm/llm/providers/transports/headless_base.py:322`: runs headless
  agent CLIs (`codex exec`, `claude`) via `subprocess.run(input=stdin_text,
  text=True, capture_output=True, timeout=..., env=..., check=False)`.
  Output capture unbounded (truncation exists only in logging:
  `sanitize_io_for_logs`, :191-205). Nonzero exit raised as
  `HeadlessExecutionError` (:369-379). `subprocess.run`'s timeout kills only
  the direct child. Shell-executable denylist at :31-38/:176-188 (`sh`,
  `bash`, `zsh`, `fish`, `pwsh`, `powershell`). Codex argv pinned to
  `--sandbox read-only` (`impls/codex/provider.py:36-37`); API key injected
  into child env (`impls/claude_code/provider.py:163-175`).
- `src/dr_llm/project/docker_runner.py:73` (`_call_docker_impl`):
  `subprocess.run(["docker",*args], input=..., capture_output=True,
  check=False, env=...)`. Optional bytes stdin (`call_docker_bytes`,
  :101-106); bytes/text as explicit overloads (:45-62); no timeout; no cwd;
  error taxonomy derived from stderr content (`docker_error`, :22-42).
- `src/dr_llm/project/docker_psql.py:65-99` (`_run_docker_process`): `Popen`
  with bidirectional binary streaming (stdin fed from a `BinaryStream`
  :118-125, stdout drained to a stream :153-158, stderr drained on a daemon
  thread :83-87); no timeout; cleanup on exception only
  (`process.kill(); process.wait()`, :91-92, direct child only).
- `src/dr_llm/project/postgres_sync.py:748`: `psql` restore with
  `stdin=<open file handle>`, `PGPASSFILE` env (:761-767), captured bytes,
  no timeout.
- `src/dr_llm/demo/cli_calls.py:20`: `uv run dr-llm`, timeout (default 120 s,
  :11), returncode → `RuntimeError` (:26-28). `:37`: stdout inherited
  (streams to terminal), stderr piped, `check=True`, no timeout.
- `src/dr_llm/project/docker_lifecycle.py:23-49`: poll-with-retry readiness
  loop over `docker exec pg_isready`. `docker_inspect.py:17,35` routes
  through `docker_runner`.
- Tests fake the seam: `tests/llm/providers/conftest.py:26-46` monkeypatches
  `subprocess.run`; `tests/test_project_docker.py:869` fakes
  `Popen.__new__`.

## dr-cognee / dr-notion (shared vendored file)

`github_docs_mirror.py` exists in dr-notion (source,
`src/dr_notion/github_docs_mirror.py`) and dr-cognee (vendored copy marked
"vendored from dr-notion@4117b4e" at `src/dr_cognee/vendored/`).
- `:193-206` — `git clone --depth 1 --filter=blob:none --sparse <url>`,
  `check=True`, output not captured (progress streams to terminal), no
  timeout; URL is config-supplied, passed as argv.
- `:785-793` (`run_git`) — `subprocess.run(["git",*args], cwd=checkout_path,
  check=True, capture_output=True, text=True)`, no timeout, inherited env;
  callers at :186-:230 (fetch/checkout/sparse-checkout).

## dr-graph / dr-providers / dr-platform / dr_setup

- dr-graph `tests/test_imports.py:31`: `sys.executable -c` import-hygiene
  probe, captured, `check=False`, no timeout.
- dr-providers `tests/test_public_api.py:125,133`: `sys.executable -c`
  import-hygiene, `check=True`, no capture, no timeout;
  `tests/test_audit_ground_truth.py:10-17` loads a first-party checked-in
  script via `importlib.util.spec_from_file_location` + `exec_module`;
  `scripts/live_test_llm_configs.sh` is operator-run bash.
- dr-platform: no subprocess module use.
  `tests/test_dbos_recovery_boundary.py:158` uses
  `multiprocessing.get_context("spawn").Process` with a join timeout (:169)
  and `terminate()`+`join()` (:171-172); comment at :30-32 documents a
  deliberate join-margin-vs-terminate design.
- dr_setup `tests/test_cli_integration.py:313,336`:
  `[sys.executable,"-m","dr_setup.scripts.merge_pyproject",...]`, captured,
  `cwd=`, `check=False`, no timeout. `scripts/setup_repo.sh` is bash driving
  `git clone`/`gt create`/`uv init`/`uv sync`.

## dr-util

`src/dr_util/slurm_utils.py:50-62` (`_run_command`): `subprocess.run(
capture_output=True, text=True, check=True, timeout=30)`. Callers: `sinfo`
(:66), `sacctmgr` (:135), `squeue` (:146), `scontrol show partition` (:162).
Caller-supplied partition/QOS names validated against `^[a-zA-Z0-9_-]+$`
(:157, :141). No output bound. `TimeoutExpired` wrapped into `SlurmError`
(:59-62); nonzero exit propagates raw `CalledProcessError`. Bare tool names
resolved via PATH (`# noqa: S607`).

## dr-subs

- `src/dr_subs/machines.py:129-133` (`_run_remote_worker`, :121-198):
  `Popen(("ssh",*_SSH_OPTIONS,source_id,*_REMOTE_WORKER_COMMAND))`, peer ID
  from a closed enum (:51). JSON request written synchronously to stdin
  before the read loop (:149); write precedes reads, so a request larger
  than the pipe buffer cannot complete. Response read via `selectors` +
  `os.read` with a 64 MiB cap (`_MAX_REMOTE_RESPONSE_BYTES`, :93, :160-175);
  per-call parameters for timeout (default 3600 s, :92) and max bytes
  (:125-126). stderr not captured (inherits). Bytes decoded UTF-8 with a
  typed error (:184-190). Returncode inspected (:319-324). Cleanup
  `_stop_process` (:110-118): SIGTERM, 1 s wait, SIGKILL — single process,
  no `start_new_session`, no `killpg`. Error type `RemoteScanError(code,
  message)` with codes peer_worker_unavailable / peer_worker_timeout /
  peer_response_limit / peer_protocol_error / ssh_timeout / ssh_unavailable
  (:103).
- `:247-253`: `/usr/sbin/scutil --get LocalHostName`, timeout 3,
  `check=False`; any failure degrades to `"local"` (:254-258).
- `:295-301`: ssh reachability probe, output to DEVNULL, timeout 8,
  returncode inspected (:310).

## dr-diagram (Node/JS + shell)

- Headless-Chrome spawns: `viz/run-scrubbable/scripts/export-frames.mjs:70-74`
  (stderr consumed incrementally to scrape the DevTools ws:// URL :77-80;
  cleanup `proc.kill('SIGKILL')` on the leader + `rmSync` of temp profile
  :82-87); same pattern in `iteration-cockpit/scripts/render-dump.mjs:60-65,
  74-79` and two `smoke.mjs` files;
  `reflexion-workbench/scripts/smoke.mjs:63-72` accumulates stdout with a
  20 s `setTimeout(() => child.kill('SIGKILL'))` watchdog (:72), comment at
  :56-58 documents that a stale `--user-data-dir` hangs headless Chrome;
  `phase7/skill-audit/shots/cap.mjs:11` uses a fixed debugging port. Chrome
  forks renderer/GPU/zygote children; all kills are leader-only.
- Python-validator invocations: `viz/one-page-optimizer/src/verify.mjs:35,41`
  and identical sites in `pipeline-visible/src/verify.mjs:42`,
  `seriated-map/src/verify.mjs:36`, `reflexion-workbench/scripts/verify.mjs:42`
  — `execFileSync(<venv python>, [validator,...], {stdio:'pipe'})`, no
  timeout, throws on nonzero; callers read `e.stderr`/`e.stdout` off the
  thrown error (:38, :44).
- `spawnSync('sh', ['-c', `command -v ${cmd}`])` ×5 (export-frames.mjs:210,
  two smoke.mjs, render-dump.mjs:176, reflexion smoke.mjs:116); `cmd` is a
  hardcoded literal at every caller.
- `phase6/multi-model-review/pi-image-run.sh:29-42`: deadline implemented in
  Perl — `fork`+`exec`, `SIGALRM` → `kill TERM`, `sleep 5`, `kill KILL`,
  `waitpid`, exit 124; configurable via `PI_RUN_DEADLINE_SECS` (default
  3600); signals a single pid; `eval "$(mise env)"` at :24.
- `phase7/skill-audit/run-codex.sh:29-49`: `codex exec` under
  `env -i HOME=... PATH=... TMPDIR=...` with every `ANTHROPIC_*` variable
  blanked; prompt via stdin herestring (:47); `set -uo pipefail` without
  `-e` (:5); the `echo "exit=$?"` at :50 reports the preceding command's
  exit, not codex's.

## dotfiles

- `scripts/sync_skills.py:237` (`run_git`): `["git","-c",
  "core.quotepath=off",*args]`, `cwd=`, `check=True`, captured text, no
  timeout, no output bound. `:249` (`run_git_bytes`): same in bytes mode;
  used at :737 for `git archive --format=tar`. `:409`/`:719`: `git
  cat-file -e` / tree-exists with output to DEVNULL, returncode as boolean.
  `:946` (`merge_file_bytes`): `git merge-file -p`, bytes stdout,
  `check=False`; nonzero returncode means "conflict", not failure.
- `scripts/skill_dispatch.py:661`: `[~/.local/bin/claude, "-p", prompt,
  "--model", "claude-haiku-4-5", "--strict-mcp-config"]`,
  `capture_output=True`, `timeout=120`, no returncode check (stdout returned
  regardless), no cwd/env control, no output bound; timeout kills only the
  direct child.
- pi extensions (TS): `agents/config/pi/extensions/copy-all.ts:1,32` —
  `spawn("pbcopy")`, full transcript written to child stdin, exit code →
  promise rejection, no timeout. `diff.ts:41,122` — `pi.exec("git"/"zed",
  [...], {cwd, timeout: 5000})`, returncode inspected.
  `zsh-user-bash.ts:25-33` — overrides pi's `user_bash` so every
  model-issued command runs as `exec '/bin/zsh' -fc '<command>'` (quoting
  via `shellQuote`); bounding/timeout/kill delegated to pi's runtime.
- Tests: `tests/test_skill_dispatch.py:255,270,320` — `uv run --script`
  with stdin payloads and env overlays (`{**os.environ, ...}`), no timeout;
  `tests/test_sync_skills.py:28,42,56,415,430` — git fixtures and
  `uv run --with ... python sync_skills.py`, `cwd=`. Four test modules load
  first-party scripts at module scope via `spec_from_file_location` +
  `exec_module` (test_sync_skills.py:18-20, test_skill_dispatch.py:21-23,
  test_sync_claude_user_config.py:11-13, test_vocab_export_checker.py:12-14).

## symphony-lite

- `src/symphony_lite/claude_runner.py:50`: `Popen(["claude","-p",
  full_prompt, ..., "--output-format","stream-json", ...], cwd=workspace,
  stdout=<open file>, stderr=STDOUT, stdin=DEVNULL,
  start_new_session=True)`. Fire-and-forget: no wait, no reap; pid recorded
  in SQLite; runs last minutes-to-hours; no timeout; transcript file grows
  unbounded by design. cwd is load-bearing (Claude session IDs are
  cwd-scoped, per module docstring). `config.assert_subscription_auth_env()`
  at :39 asserts `ANTHROPIC_API_KEY` is absent before spawn.
- `kill_run` (:104-119): `os.killpg(run.pid, SIGTERM)` swallowing
  `ProcessLookupError`; no wait/reap, no escalation, no pid-reuse guard.
  Liveness via `os.kill(pid, 0)` (:173-182).
- `src/symphony_lite/codex_appserver.py:67`: `Popen(["codex","app-server"],
  stdin=PIPE, stdout=PIPE, stderr=<append-mode log file>, text=True,
  start_new_session=True)`. Long-lived singleton; bidirectional JSON-RPC
  over stdio; reader thread (`_read_loop`, :167) demultiplexes responses by
  id into per-request `queue.Queue`s; per-request timeout 60 s (:158);
  `_write_lock` serializes stdin writes. `stop()` (:88-90) calls
  `terminate()` on the leader only. `_read_loop` iterates stdout without any
  size bound. `_pending` is mutated at :162 and :176 under different locks.
  stdin write at :155 unguarded against `BrokenPipeError`. No restart or
  reaping of a crashed child.
- `src/symphony_lite/codex_runner.py:97-110` (`kill_codex_run`): cancels via
  protocol-level `turn/interrupt` JSON-RPC, not a signal.
- `src/symphony_lite/cli.py:165`: `os.execvp("claude", ["claude","agents"])`
  — process replacement.
- Git: `claude_runner.py:186` (branch query, returncode unchecked);
  `workspace.py:72` (`git fetch`, timeout 120, returncode deliberately
  ignored); `:103` (`rev-parse`, bytes, returncode-as-predicate); `:111`
  (`_git` helper, timeout 300, nonzero → `WorkspaceError`; repo scoping via
  `git -C` argument rather than `cwd=`).

## code-eval

- `src/code_eval/subprocess_runner.py` (`SubprocessRunner`, pydantic frozen
  model): `shutil.which` pre-resolution → `tool_found=False` as data (:53);
  `subprocess.run([exe,*args], input=stdin_text, capture_output=True,
  text=True, timeout=self.timeout_s, check=False)` (:68); default timeout
  5.0 s (`names.py:25`); result object `SubprocessResult(ok, timed_out,
  tool_found, duration_s, ...)`, never raises; partial stdout/stderr
  recovered from `TimeoutExpired` (:76-88). No output bound, no input bound,
  no env control, no process-group handling.
- Call sites (all linters over generated source): `normalizers/_ruff_runner.py:24`
  (`ruff format --stdin-filename - `, candidate source on stdin), `:61`
  (`ruff check --fix-only`), `normalizers/l5_ty_fix.py:53` (`ty check
  <tmpfile>`; ty has no stdin mode), `subprocess_runner.py:108`
  (`<tool> --version`), constructors in l2/l3/l4 normalizers,
  `pipeline/normalize_step.py:60`, `validator.py:41-42,67`.
- In-process on generated code: `validators/compile_check.py:17` —
  `compile(source, "<candidate>", "exec")`, never executed;
  `validators/import_resolve.py:21` — `importlib.util.find_spec(name)` on
  import names extracted from generated code; `find_spec` on a dotted name
  imports and executes parent packages.
- Tests load first-party scripts via `spec_from_file_location`
  (`tests/unit/test_extraction_ladder.py:16-19`,
  `tests/unit/test_trace_viewer.py:15-18`).

## genfxn

- `src/genfxn/core/safe_exec.py` (881 lines): executes model-generated
  Python via `multiprocessing` workers (forkserver/spawn; `fork` accepted as
  env override, :336-346). Properties: `resource.setrlimit(RLIMIT_AS)`
  default 256 MB (:236-246); structured value return with an allowlisted
  type graph and nesting depth 32 (:403-443); results pickled before
  `queue.put` so serialization failures surface synchronously, size bound
  1 MB default (:446-505, :471-473); persistent reusable worker
  (`_PersistentWorker` :717-841, `_IsolatedFunction` :590-634) with separate
  startup timeout (:257-258); AST pre-validation blocking `__import__`,
  `eval`, `exec`, `compile`, `open`, `getattr`, dunder attributes, `Import`,
  `ClassDef`, `Global`, top-level non-function statements (:63-154),
  self-described as "not a security sandbox" (:66-68); explicit
  `trust_untrusted_code` opt-in gate raising `SafeExecTrustRequiredError`
  (:843-864); spawn-bootstrap diagnostics (:290-331); `atexit`/`__del__`
  cleanup (:608, :626-633); six-type error taxonomy. No stdout/stderr
  capture or bound of any kind (child writes to inherited fds). No stdin
  path (inputs travel as pickled args, unbounded).
  `_set_process_group` (:157-163) swallows all exceptions from `os.setsid()`;
  `_terminate_process_tree` (:181-233) does SIGTERM→SIGKILL with 0.2 s
  joins, uses `killpg` only when group leadership is confirmed, and swallows
  signaling failures (:190-201). `exec` sites: `:371` (`_exec_worker`),
  `:649` (`_persistent_worker`).
- Ten family validators call `execute_code_restricted(code,
  _ALLOWED_BUILTINS, trust_untrusted_code=True)` on generated task code:
  temporal_logic/validate.py:328, piecewise:273, sequence_dp:309,
  stateful:242, bitops:330, fsm:305, intervals:325, graph_queries:309,
  simple_algorithms:329, stringrules:351.
- `src/genfxn/verification/parity.py:337`: executes the compiled binary of
  generated Java/Rust code (`java -cp <tmp> Main` / `<tmp>/main`) with
  inputs encoded as argv; captured output, unbounded; timeout; `check=True`
  → raises `CalledProcessError`; no process-group kill. `:366` `javac`,
  `:389` `rustc` compile generated source (`check=True`, capture, timeout).
- `src/genfxn/langs/formatting.py:57`: `google-java-format --replace
  <tmpfile>` (result read back from the file), timeout 15 s, exceptions
  swallowed → unformatted code returned, memoized with
  `lru_cache(maxsize=2048)`; `:80` `rustfmt`, same shape.
- `src/genfxn/generated_code_quality.py:90` (`_run_checked_subprocess`):
  linters/compilers over generated sources with `cwd=`, `check=True`,
  capture, timeout; on `TimeoutExpired`, recovers `exc.stdout`/`exc.stderr`
  into the raised message (:98-107).
- `langs/registry.py:83`: `importlib.import_module` on first-party registry
  paths.

## whetstone-ai

- `src/whetstone/optimization/codex_runner.py:256`: `codex exec` with a
  model-authored prompt, wired to an MCP server spawned as
  `sys.executable -m whetstone.optimization.mcp_server`. Axes:
  `stdin=DEVNULL`; `capture_output=True` unbounded (stdout is a JSONL event
  stream, later sliced `[-2000:]`); `env={**os.environ}` plus MCP config
  vars (:239-244); timeout 600 s; `check=False` → `OpaqueStepError`
  (:265-269); `shutil.which` pre-check (:235); no process-group kill.
- `src/whetstone/optimization/codex_proposer.py:146`: `codex exec
  --skip-git-repo-check -s read-only --output-last-message <tmpfile>`;
  result read from the temp file, not stdout (:160; docstring :111-113);
  `cwd=` (:132); `env={**os.environ}`; `TimeoutExpired` caught → typed
  `CodexInvocation(text="", returncode=-1, timed_out=True)` (:156-159).
- `src/whetstone/runner/execution_mode.py:106`: `docker info` probe,
  bytes mode (no `text=True`), timeout 10, returncode → bool,
  `shutil.which` (:102).
- `src/whetstone/envs/ed1m_oracle.py:35` and `envs/ed1_scoring.py:40` import
  `dr_code.humaneval.subprocess_runner` — a module path that does not exist
  in dr-code (moved to `dr_code.execution.subprocess`); `dr_code` is not
  installed in whetstone-ai's `.venv`; the call at `ed1m_oracle.py:107-111`
  passes `input_json=` where the current signature takes `input_text=`.
- `envs/ed1m_oracle.py:46-63` (`_DRIVER_SOURCE`): a driver program string
  that reads a JSON request from stdin, `exec`s a model-produced
  reconstruction (:51), and writes single-line JSON to stdout; runs under
  the subprocess runner.
- `src/whetstone/optimization/mcp_server.py:58`:
  `importlib.import_module(module_name)` with the name from env var
  `WS_MCP_EVALUATOR`.
- Tests: `tests/optimization/test_codex_live_smoke.py:42` (`codex login
  status`, timeout 30); `tests/optimization/test_codex_proposer.py:189,215,
  228,245,310` write `#!/bin/sh` stub scripts to a temp bindir to fake the
  codex CLI, including a `sleep 5` stub to force the timeout path.

## whetstone-envs

`src/whetstone_envs/c18/upstream.py:169`: `[sys.executable,
"run_experiment.py", ...]` driving a vendored generator; `cwd=` a
`TemporaryDirectory` populated with symlinks into the read-only vendored
tree (:143-146) so parallel calls cannot collide on the generator's fixed
output filename; timeout; `check=False` → `UpstreamError` with
`stderr[-500:]` (:177-182); result read from a file the child wrote
(:183-195). Tests: `tests/c22/test_isolation.py:65` (`-c` probe, no
timeout); `tests/c23/test_upstream.py:121,140` (`patch --batch -p1` against
pinned SHA256s, `cwd=`, `check=True`); `:228` (`env={"PATH": _path()}` full
replacement); `:339` (`env={"PYTHONHASHSEED": ..., "PATH": ...}`).

## whetstone-viewer

- `src/whetstone_viewer/etl/hydration/runner.py:111` (`_run_dump`):
  `["uv","run","--frozen","--project",<repo>,"python","-",*args]` with
  `input=<script>` — a first-party Python script piped to `python -` via
  stdin (:101-105); captured output parsed as JSON (:129); timeout;
  `check=False` → `HydrationError` with `stderr[-2000:]`; `OSError`/
  `SubprocessError` mapped to `HydrationError` (:119-122).
- `:153` (`_git`): `git -C <repo> rev-parse HEAD` / `status --porcelain`,
  timeout 30, all failures → `None`.
- `etl/hydration/task_intrinsic.py:270`: `ruff format --isolated <tmpfile>`,
  in-place mutation read back from the file (:281), timeout 60, any failure
  → `None`; `:287` `ruff --version`.
- `web/scripts/gen-api.mjs:26`: `execFileSync("uv", [...], {cwd,
  stdio:"inherit"})` — full passthrough, no capture, no timeout, throws on
  nonzero.

## unitbench (Node)

- `scripts/gen-api.mjs:34`: `spawnSync` driving `uv run ... python -m
  <facade>.serve` OpenAPI export against sibling repos; captured; no
  timeout; throws on nonzero with stderr+stdout in the message (:41).
- `scripts/gen-graph-schema.mjs:21`: `execFileSync('uv', ['--directory',
  <sibling>, 'run', 'python', '-c', EXPORT_SCHEMA])` — first-party inline
  `python -c`; captured; no timeout; throws on nonzero.
- `scripts/vercel-install.mjs:47`: `spawn('pnpm', ['install',
  '--frozen-lockfile'], {env, shell:false, stdio:['inherit','pipe','pipe']})`
  — output streamed live to the parent's stdio (:53-56) with a GitHub token
  folded into `GIT_CONFIG_*` and stripped from the child env (:20-27) and
  redacted in-flight from forwarded output (`redactToken`, :29-33); no
  timeout; exit code → promise resolve/reject (:58-62).

## nl_latents / nl-code / nlae / llmflow

- nl_latents: `src/nl_latents/sampling/decoder/evaluation_runner.py:213` —
  `git status --short`, `check=True`, captured, no timeout (a post-sandbox
  working-tree check); `tests/test_t1_shell_args.py:83` — `bash -c
  'source <config> && printf ...'`, `cwd=`, `check=True`. ~19 shell scripts
  drive Python (`shared_provider_worker_runner.sh:44` → `uv run python`),
  not the reverse.
- nl-code: `src/nl_code/test_cli.py:31` — `[sys.executable,"-m","pytest"]`,
  stdio inherited (streams to terminal), `check=False` → `typer.Exit(rc)`,
  no timeout. `src/nl_code/code_execution/runner.py:307,330,345,395` —
  spawning delegated to `dr_docker.SubprocessDockerAdapter`; caller-imposed
  stdin cap 50 MiB (:47); per-stream stdout/stderr caps 1 MiB default,
  env-overridable (:207-211); adapter error envelope translated to raising
  `CodeExecutionInfrastructureError` (:376-421). `runner.py:497` —
  `compile(code, "<generated>", "exec")` syntax check only.
  `worker.py:185,334,389` — `exec` of model-generated code, gated by
  `_require_docker_execution` (:100-107), which raises unless an env var is
  set and `_is_running_in_container()` is true; surrounding defenses: AST
  denylist rejecting Async nodes and dunder access, `__builtins__`
  shallow-copy namespace (:50), `RLIMIT_CPU` (:119-131), per-item
  `signal.setitimer(ITIMER_REAL)` + SIGALRM (:500, :544-570), bounded stdin
  and stdout capture from `dr_docker.workers.json_stdio`
  (`read_stdin_bounded`, `BoundedTextCapture`).
- `dr_docker.SubprocessDockerAdapter` (pinned `dr-docker==0.4.5`, read at
  `nl-code/.venv/.../dr_docker/subprocess_adapter.py`, 362 lines): builds
  `docker run` with `--network=none`, `--read-only`, `--cap-drop`,
  `--security-opt=no-new-privileges`, `--memory`, `--cpus`, `--pids-limit`,
  `--ulimit cpu/fsize/nofile/nproc`; `selectors`-based reader; per-stream
  caps with truncation markers (`[stdout truncated: N bytes total, capped at
  M]`); bytes stdin; working dir/tmpfs/bind mounts/env injection; result
  envelope `DockerRuntimeResult(ok, error=ErrorEnvelope(code, retriable))`;
  container cleanup via cidfile in `finally` (`docker rm -f`, CID validated
  against `[0-9a-f]{64}`); `cpu` ulimit derived from `timeout_seconds`. No
  `start_new_session`/`killpg` (bare `proc.kill()` on the docker CLI
  client); no TERM→KILL escalation; `proc.wait()` after kill unbounded;
  writer thread joined without timeout; no NUL/empty-argv validation;
  Docker-only (no plain-argv entry point).
- nlae: `src/nlae/arxiv_library/fetch.py:129-136` (`_run_gcloud_copy`):
  `["gcloud","storage","cp","-I",dest]` with a newline-joined URL list on
  stdin, `check=True`, no timeout, no capture. `runner=subprocess.run` as a
  default-argument injection seam (:207, :231); `:231` compares
  `runner is subprocess.run` to gate a `shutil.which("gcloud")` preflight;
  tests monkeypatch it matching `subprocess.run`'s exact kwarg names
  (`tests/test_fetch.py:59,95,177`).
- llmflow (TS): `packages/providers/src/codex.ts:163` — `spawn("codex",
  ["exec","--json",...,"--sandbox","read-only",...,"-"])` with the
  model-authored prompt written to stdin; stdout accumulated with no size
  bound; timeout via `CODEX_COMMAND_TIMEOUT_MS`; AbortSignal cancellation;
  SIGTERM only, leader only. `packages/runtime/src/tool-executor.ts:170`
  (`executeBash`) — model-issued command run via `bash -lc` (login shell;
  schema documents this at `packages/core/src/tools.ts:78-96`);
  `stdio:['ignore','pipe','pipe']`; per-stream cap via `cappedAppend`
  (:231-232), which materializes the full concatenation before slicing and
  truncates silently; `cwd` supported; timeout with a configured ceiling;
  SIGTERM → 1 s → SIGKILL (:184-193), leader only; approval required and
  sequential execution configured at `tools.ts:106-118`. Tool schema,
  approval, timeout defaults/ceilings, and output limit co-located in a
  frozen `BASH_TOOL_CONTRACT` object (`tools.ts:84-130`).

## codearc / fchord / diff-walkthrough / marimo-pair / marimo_utils

- codearc: ~35 git fixture/query sites in `tests/test_cli.py`,
  `tests/test_miner.py`, `scripts/demo_miner.py` (`cwd=`, `check=True`, no
  timeout); `scripts/github_loc_stats_t1.py:125` — `gh auth token`,
  captured (a secret on stdout), `check=True`;
  `scripts/local_loc_stats_t1.py:64` (`run_git`, `git -C`), `:74`
  (`git config --global`, `check=False`, empty stdout treated as absent).
  `src/` executes nothing: mining via pydriller (`mining/miner.py:4,33`) and
  libcst/ast; extracted code stored as text in DuckDB.
- fchord: `src/fchord/github_pull.py:186` (`_git`) — captured, text,
  `check=True`, `timeout=_GIT_TIMEOUT`; no process-group cleanup.
  `tests/test_github_pull.py:15` — git fixtures. Bash scripts under
  `scripts/` use `exec uv run python` and `jq` with mktemp+trap.
- diff-walkthrough: `apps/viewer/bin/diff-walkthrough.js:520` (`git()`) —
  `execFileSync('git', ['-C', repo, ...])`, `encoding:'utf8'`,
  `maxBuffer: 50 * 1024 * 1024`, `allowFailure` option converting throw to
  empty-string return; no timeout. Test fixture equivalent at
  `apps/viewer/unit/builder.test.js:38`.
- marimo-pair: `skills/marimo-pair/scripts/execute-code.sh` — sends
  agent-authored Python to a running marimo kernel over HTTP+SSE
  (`curl -sN` POST :203-208); code from file arg or stdin (:41, :43); SSE
  events demultiplexed to local stdout/stderr in real time (:177-201);
  exit code synthesized from the protocol's `done` event (:174, :192,
  :210); non-local-host warning (:54-60); server discovery via
  `$XDG_STATE_HOME/marimo/servers/*.json` with pid-liveness checks and
  stale-entry GC (:74-116); no timeout; no output bound; auth via
  `MARIMO_TOKEN` env (preferred over argv because argv is visible in `ps`).
  `discover-servers.sh:28` — `kill -0` probing plus `rm -f` of stale
  registry entries.
- marimo_utils: `styles/build.mjs:12` — `execSync("npx tailwindcss -i ... 
  --minify", {cwd, encoding:"utf8"})`: a shell command string (the only
  shell-string invocation found in the fleet's first-party code); stdout
  captured and written to a file; throws on nonzero; no timeout.
  `scripts/pre-check.sh` — near-verbatim twin of dr-code's.

## Cross-cutting tallies

- Approximately 205 first-party call sites fleet-wide; roughly 90% invoke
  trusted first-party tools; the remainder execute model-generated code,
  compiled artifacts of it, or headless agent CLIs.
- No call site outside `dr-code/src/dr_code/execution/subprocess.py`
  combines session isolation, whole-group kill, and a bounded termination
  wait. Partial forms that exist: symphony-lite sets `start_new_session` and
  signals the group with SIGTERM but never waits, escalates, or reaps;
  llmflow's bash tool escalates SIGTERM→SIGKILL on the leader only; genfxn
  escalates TERM→KILL but its group creation can silently fail; everything
  else kills leaders only or relies on `subprocess.run`'s leader-only
  timeout kill.
- Sites with no timeout of any kind include: all 46 parse_claude test sites,
  all dotfiles git helpers, dr-notion/dr-cognee git operations (including
  network clones), dr-llm docker/psql paths, nlae's gcloud copy, all
  dr-diagram validator/Chrome sites except one watchdog, diff-walkthrough,
  unitbench gen-scripts, symphony-lite agent runs (unbounded by design).
- Output-bound facts: the reference caps at 1 MiB shared;
  diff-walkthrough sets `maxBuffer` 50 MiB; dr-subs parameterizes up to
  64 MiB per call; dr-docker and llmflow cap per-stream with truncation;
  code-eval, dr-util, dr-llm, whetstone codex paths, and all git helpers
  are unbounded.
- In-process execution of generated code exists in: genfxn (safe_exec
  workers + 10 validators), nl-code (container-gated worker), code-eval
  (`compile` only, plus `find_spec` parent-package execution), dr-code
  (`batch_runner_script.py`, which is itself the subprocess payload),
  whetstone-ai (`_DRIVER_SOURCE`, also a subprocess payload).
- Runner-injection seams exist in: dr-code (`PythonSubprocessRunner`
  Protocol), nlae (`runner=subprocess.run` default argument with identity
  comparison), dr-llm and symphony-lite tests (monkeypatching), code-eval
  (constructed `SubprocessRunner` instances).
