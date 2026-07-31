# Changelog

## 2026-07-31 (consistency pass)

- Three-reviewer consistency pass over the doc system. Resolved
  contradictions in target-usecases.md (record guarantee vs best-effort,
  faithful-record vs truncation, fresh-state vs sharing, no-survivors
  scoping, machine-protection composition); retitled use case 5 to
  "Containment profiles"; added six terms and an absence attribution
  party; corrected catalog claims that earlier verification rounds had
  fixed only partially (dr-docker argv validation, code-eval find_spec,
  llmflow timeout constant, dr-queues stop protocol, symphony-lite
  narration).

## 2026-07-31

- Removed `docs/subprocess-usage-audit.md`. The fleet audit served as
  decision-support input for the initial design pass; its facts were
  re-verified against source and superseded by the curated catalogs
  (`docs/current-uses.md`, `docs/current-implementations.md`), which
  corrected several audit errors in the process.
