# Thesis repo — guidance for Claude

Master's thesis (TU Darmstadt) comparing an **LLM-only** design with a staged
**SML → SLM → LLM cascade** for classifying a real-time stream of API logs
(Kafka + PyFlink). The topic, research questions and scope are **provisional**
and are steered by the literature review. Don't treat them as fixed.

## Where things are
- `docs/project-plan.md` is the single source of truth for status, timeline,
  scope and open decisions. Read it before planning work. Update its
  "Current Status" section when a milestone is reached; don't keep status
  anywhere else.
- `docs/decisions.md` is the dated log of decisions and their reasons. Add
  an entry whenever scope, method, models or naming change.
- `code/` holds the streaming stack. See `code/README.md`.
- `thesis/` holds the LaTeX source (TUDa template, LuaLaTeX + Biber). LaTeX is
  not installed locally.
- `resources/` holds papers, managed by Zotero. It is git-ignored and must
  never be committed.

## Running the code
- Run `docker compose` from `code/` (Docker project name: `code`).
- Host-side Python: `code/.venv` (Python 3.9, matching `/usr/bin/python3`).
  Recreate it if it's broken; don't move it.
- `code/results/` (experiment JSONL) and `code/kafka-data/` are git-ignored.
  `kafka-data/` can be rebuilt; `results/` holds experiment output, so don't
  delete it without asking.
- P0 sweep: `./experiments/p0_throughput_sweep.sh`, then
  `experiments/analyze_p0.py`.

## Conventions
- **Arm names:** `llm_only` and `cascade`. The `naive/` and `optimized/`
  folders and `NAIVE`/`OPTIMIZED` labels are legacy. Don't introduce new uses,
  and migrate them when touching that code.
- **Measurement schema:** `code/jobs/common/instrumentation.py`
  (`RECORD_FIELDS`) is authoritative. Every arm emits exactly these fields.
  `database/create-table.sql` and `operators/slm_operator.py` still use older
  names and need aligning.
- **Mock vs real:** the mock LLM backend returns `predicted_category = None` on
  purpose. Never report mock runs as quality results.
- **Latency:** report `service_ms` and `queue_ms` separately, not only `e2e_ms`.

## Literature and citations
- Never invent or guess a citation, author, venue or result. A paper is
  citable only once it's in Zotero (collection `thesis`) and therefore in
  `thesis/references.bib`, exported by Better BibTeX.
- Paper names suggested without checking (for example, in the plan's gap
  list) are leads to look up, not sources. Label them that way.
- Ground claims about the thesis topic in papers the user has, and say when
  something is only general knowledge.

## Git
- Default branch is `main`, remote `PriteshSoni221/data-stream-pipeline`.
- Commit when asked, or when finishing a requested change. Never push, rename
  the GitHub repo or change its visibility unless the user asks.
