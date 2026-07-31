# Current uses

Catalog of first-party process execution across the fleet: one section per
repo, one subsection per usage. This is the curated picture of what dr-exec
must serve — content is added deliberately as behaviors and terminology are
settled, and each usage accretes its mapping onto the vocabulary defined
in `target-usecases.md` (use case, terms, budget axes) in place. Companion
to `current-implementations.md`, which records the mechanisms; a repo whose
execution code is purely mechanism appears only there. Some usages map to
the contract's cross-cutting sections (Testing, Observability) or to
declared non-goals (in-process evaluation, the far side of ssh) rather
than a numbered use case. Repos ordered by most recent PR activity.

## dr-code

### HumanEval batch evaluation

### Self-invocation test probes

### Execution faking in tests

### In-process syntax validation

### Pre-check developer script

## whetstone-ai

### Codex-driven optimization steps

### HumanEval oracle scoring

### Docker availability probing

## whetstone-envs

### Parallel-safe vendored-generator invocation

## fchord

### Sparse git docs pulling

### Codex session scripts

## dotfiles

### Git-based skill vendoring and merge

### pi extension tool calls

## whetstone-viewer

### Cross-repo data hydration

### Formatting normalization of task code

### Web API codegen

## genfxn

### Generated-task validation

### Cross-language parity verification

### Generated-code formatting

### Generated-code quality checks

## dr-platform

### Crash-recovery boundary testing

## dr-graph

### Import-hygiene probing

## dr-providers

### Import-hygiene probing

### Checked-in script loading

## dr-subs

### Remote peer scanning over ssh

(The contract serves the local half — the ssh invocation is a trusted-tool
call; the remote side is caller domain by declared scope.)

### Host identity and reachability probes

## dr-diagram

### Headless-browser rendering and capture

### Python validator invocation

### Batch codex runs

## unitbench

### Sibling-repo schema and API codegen

### Deploy-time dependency install

## nlae

### Bulk artifact fetching

## dr-llm

### Headless agent CLI transport

### Dockerized service lifecycle

### Database restore and sync streaming

### Demo CLI self-invocation

## nl_latents

### Containment verification tripwire

### Provider worker fleets

### Shell-config test oracles

## dr-cognee

### GitHub docs mirroring

## symphony-lite

### Detached agent-run supervision

### Codex app-server RPC

### Workspace git operations

## dr-notion

### GitHub docs mirroring

## dr-dspy

### HumanEval batch evaluation

### Local model server management

### Sandboxed interpreter execution

## codearc

### Git fixture building and queries

### GitHub stats collection

## marimo_utils

### Tailwind CSS build

### Pre-check developer script

## nl-code

### Containerized generated-code evaluation

### Test-suite self-invocation

## dr_exp

### GPU worker fleet supervision

### Slurm job submission

### CLI self-invocation job management

## deconCNN

### Sibling-script re-invocation

### Cross-repo experiment management

## dr_gen

### Parallel local training runs

## dr-util

### Slurm cluster queries

## parse_claude

### CLI end-to-end testing
