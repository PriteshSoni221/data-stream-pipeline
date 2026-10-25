# Decision Log

Dated record of decisions and the reasons behind them. Newest first. These
feed the methodology chapter and answer examiners' "why did you…?" questions.

## 2026-10-25 — Target submission end of January 2027
Fixed 6-month phases dropped in favour of an optimistic 14-week plan to
29 Jan 2027, with milestones M1–M4 (see `project-plan.md`).
**Why:** the work did not follow the original month-by-month plan, and the
aim is to finish quickly.

## 2026-10-25 — Dashboard deferred; Postgres loader optional
The Redis + React/FastAPI live dashboard moves to "only after results are
frozen". The Postgres batch loader is built only if the analysis needs it.
**Why:** neither answers a research question, and the January timeline leaves
no room for them. Results are analysed directly from the JSONL output.

## 2026-10-25 — Repository restructured into code/, thesis/, docs/, resources/
The `app/` repo was lifted to the thesis root. LaTeX moved out of the code
tree. Papers are managed by Zotero in a git-ignored `resources/`.
**Why:** one repo for the whole thesis; copyrighted PDFs stay out of git;
`thesis/references.bib` (exported from Zotero) links literature to the text.

## 2026-10-25 — Arm names: `llm_only` and `cascade`
These names replace the mix of naive/optimized/NAIVE/tiered. Migration
happens as code is touched.
**Why:** inconsistent names had spread across the code, SQL, README and plan.

## Before Oct 2026 — reconstructed from the code
- **Results go to a JSONL sink, not straight to Postgres.** This keeps a
  database round-trip out of the measured latency.
- **Parallelism is an experiment variable.** PyFlink has no async I/O, so
  parallelism is the only way to overlap blocking inference calls. The P0
  spike tests whether that works.
- **Python bundle size/time pinned to 1.** Flink's defaults would add up to
  1 s of hidden latency to every measurement.
- **Fresh consumer group per run, reading from the earliest offset.** Each
  run replays the same preloaded topic, which makes runs comparable.
- **Mock LLM backend with lognormal latency.** It characterises system
  behaviour without spending API budget. It produces no predictions, so mock
  runs can't be mistaken for quality results.

## Open (decide by M1, 8 Nov 2026)
- Task framing: incident classification vs anomaly detection.
- LLM provider/model, SLM choice, SML method.
- Whether to add a public labelled log dataset alongside the synthetic logs.
