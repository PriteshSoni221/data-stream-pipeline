# Master's Thesis — Cost-Aware Inference for Real-Time Text Streams

Working title (not final): *Evaluating Optimized and LLM-Only Approaches for
Real-Time Text Stream Processing*. Topic and objectives are still being refined
as the literature review progresses; see [docs/project-plan.md](docs/project-plan.md).

## Layout

```
.
├── code/        Streaming stack: Kafka + PyFlink pipelines, producers, experiments
├── thesis/      LaTeX source (TUDa thesis template)
├── docs/        Project plan and working notes
└── resources/   Papers and other material — managed by Zotero, not tracked in git
```

- **Code:** setup and usage in [code/README.md](code/README.md).
- **Thesis:** compile `thesis/main.tex` with LuaLaTeX + Biber.
- **Literature:** kept in the Zotero `thesis` collection. The bibliography used by
  LaTeX is exported from Zotero (Better BibTeX, "keep updated") into
  `thesis/references.bib`, which is tracked.
