# Master's Thesis Project Plan

*Last updated: 25 Oct 2026 · Target submission: end of January 2027*

This is a living document. The topic, research questions and scope are still
provisional and will be steered by the literature review. Sections marked
**(open)** are decisions not yet made. Decisions already taken, and the
reasons for them, are in [decisions.md](decisions.md).

## Working Title (provisional)
**Evaluating Optimized and LLM-Only Approaches for Real-Time Text Stream Processing**

## Direction
The thesis compares two ways of applying language models to a real-time text
stream:

- **LLM-only arm** (code: `llm_only`, folder `naive/`): every event goes to an LLM.
- **Cascade arm** (code: `optimized/`): a cheap statistical model (SML) handles
  what it can, a small language model (SLM) takes the middle ground, and only
  the remaining events escalate to the LLM.

The use case is backend API logs. The working task is assigning each log line
to one of 8 incident categories (7 error types + `NORMAL`).

The goal is to find out when the cascade's lower inference cost justifies its
extra system complexity. The comparison covers latency, throughput, cost,
output quality and developer experience.

### Candidate research questions (open)
These are starting points that the literature review will sharpen.

1. How much LLM cost can a staged SML → SLM → LLM cascade save on a log
   stream, and how much quality does it lose doing so?
2. How do the two arms behave under load in a stream processor without async
   I/O? This covers throughput ceilings, queueing and how they scale with
   parallelism.
3. What extra engineering effort does the cascade cost, and when is it worth it?

### Open decisions
- **Task framing:** multi-class *incident classification* (what the generator
  does now, with uniform classes) or *anomaly detection* (rare anomalies in
  mostly normal traffic)? The answer decides the class balance, the metrics
  and which literature applies.
- **Models:** which LLM provider and model, which SLM (local vs hosted), and
  which SML method (e.g. TF-IDF + logistic regression, or an embedding cache).
- **Data:** synthetic logs only, or also a public labelled log dataset as an
  external validity check?

## Current Status (25 Oct 2026)

### Built
- Dockerised stack: Kafka 3.7 (KRaft) and Flink 1.19 / PyFlink, with 16 task
  slots.
- Synthetic log producer. Each event carries a ground-truth category and a
  complexity tier (simple / medium / complex), and runs are reproducible with
  seeds.
- **LLM-only arm**, end to end, with a **mock** LLM backend (lognormal
  latency, median 500 ms). Mock runs produce no predictions, so they can never
  be mistaken for quality results.
- **Measurement framework:** a shared per-event record schema that separates
  latency into service, queue and end-to-end time, plus a JSONL sink with one
  file per subtask.
- **P0 de-risk spike** (does throughput scale with parallelism, given PyFlink
  has no async I/O?). Partial results so far:

  | Parallelism | Events | Throughput | Scaling | Service p50 |
  |---|---|---|---|---|
  | 1 | 400 | 1.67 ev/s | 1.00× | 504 ms |
  | 2 | 400 | 3.40 ev/s | 2.04× | 484 ms |

  Scaling is linear so far. Parallelism 4, 8 and 16 have not been run yet.
- Repository restructured into `code/`, `thesis/`, `docs/` and `resources/`
  (the last managed by Zotero). The LaTeX template is in place.

### Not built yet
- SML router and SLM operator (currently placeholders).
- Real LLM backend and token/cost accounting (the cost fields are empty).
- Cascade arm on the instrumented interface (it currently refuses to run).
- Postgres loader. The schema in `create-table.sql` predates the measurement
  schema and needs aligning.
- Redis, the dashboard, and any thesis text.

## Scope for a January Submission
To finish by the end of January, the core is the two arms, the measurements
and the written thesis. Everything else is optional.

| Item | Status in plan |
|---|---|
| LLM-only and cascade arms, instrumented | **Core** |
| Quality, cost, latency and throughput experiments | **Core** |
| Developer-experience comparison (lightweight: an effort log, components, setup steps, lines of code) | **Core** |
| Results analysis with plots from the JSONL output (scripts or notebook) | **Core** |
| Postgres batch loader | Optional: only if the analysis needs it |
| Redis and the live React/FastAPI dashboard | **Deferred**: demo nice-to-have, only after results are frozen |

## Timeline (26 Oct 2026 – 29 Jan 2027)

| Weeks | Dates | Focus | Milestone |
|---|---|---|---|
| 1–2 | 26 Oct – 8 Nov | Literature sprint on the gaps below; Zotero + Better BibTeX → `thesis/references.bib`; finish the P0 sweep (parallelism 4/8/16); decide task framing and models | **M1:** research questions fixed, related-work outline |
| 3–5 | 9 – 29 Nov | Build SML router, SLM operator, real LLM backend with cost accounting; port the cascade arm to the instrumented interface. Write the Background and Related Work chapters alongside | **M2:** both arms run end to end on real models |
| 6–8 | 30 Nov – 20 Dec | Run experiments (quality by complexity tier, cost, latency under load, scaling) and keep the developer-experience log. Write the Design and Implementation chapters | **M3:** results frozen |
| 9–10 | 21 Dec – 3 Jan | Holiday buffer; light writing (first Evaluation draft) | — |
| 11–13 | 4 – 24 Jan | Evaluation and Discussion, Introduction and Conclusion; full draft to supervisor; revisions | **M4:** complete draft (~15 Jan) |
| 14 | 25 – 29 Jan | Final polish, formatting, submission | **Submit: 29 Jan 2027** |

Writing runs alongside the build from week 3 onward rather than waiting until
the end.

## Literature

### Current set (8 papers)
| Theme | Papers | Coverage |
|---|---|---|
| Real-time text stream processing / streaming NLP | *Real-time Text Stream Processing: A Dynamic and Distributed NLP Pipeline*; *Streaming for Large Scale NLP: Language Modeling* | Adequate as a starting point |
| Cost-aware LLM inference / cascades | *Cut Costs, Not Accuracy: LLM-Powered Data Processing with Guarantees*; *AOP: Automated and Interactive LLM Pipeline Orchestration* (orchestration rather than cascades) | **Thin**, and this is the core of the cascade arm |
| Small vs large models, efficiency | *A Survey on Small Language Models in the Era of LLMs*; *The Rise of Small Language Models*; *From LLM to SLM*; *Beyond Efficiency: A Systematic Survey of Resource-Efficient LLMs* | Good |

### Gaps to fill (priority order)
1. **Model cascades and routing:** papers on routing queries between cheap
   and expensive models and on confidence-based escalation. Candidates to
   check: FrugalGPT, RouteLLM, work on LLM cascades.
2. **The use case itself, log analysis:** log parsing, log anomaly detection
   and classification, and recent work using LLMs for logs. Candidates to
   check: Drain, DeepLog, LogBERT, the Loghub datasets, and LLM-based log
   parsers and analysers. *Nothing on this yet, and examiners will likely ask
   about it first.*
3. **Semantic caching for LLMs** (this backs the `CACHE_HIT` route).
   Candidate: GPTCache.
4. **Classical / statistical text classification** as the SML tier baseline.
5. **Stream-processing benchmarking methodology:** how to measure latency and
   throughput fairly in streaming systems.
6. **Developer experience / engineering effort:** how software-engineering
   research measures it, so the developer-experience comparison has a method
   rather than an anecdote.

Every paper goes into Zotero (collection `thesis`) with a short note on what
claim it supports.

## Risks
- **Real-model throughput:** an SLM running on a laptop, or LLM rate limits,
  may cap throughput well below the mock. Mitigation: measure early (M2) and
  scale event counts to fit.
- **API cost and credentials:** set a budget and get an API key in week 2.
- **External validity of synthetic data:** mitigate with a public labelled
  log dataset, if one fits the chosen task framing.
- **Holidays:** supervisor feedback is unlikely between 21 Dec and 3 Jan, so
  get feedback on the outline and research questions before then.

## Expected Contribution (provisional)
An empirical comparison of an LLM-only design and a staged SML → SLM → LLM
cascade on a real-time log stream. It covers cost, quality, latency under
load, and engineering effort, and identifies the conditions under which the
cascade's added complexity pays off. A secondary contribution is a
reproducible streaming testbed with per-event instrumentation.
