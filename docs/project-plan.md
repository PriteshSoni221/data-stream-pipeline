# Master's Thesis Project Plan

## Working Title
**Evaluating Optimized and LLM-Only Approaches for Real-Time Text Stream Processing**

## Project Overview
This thesis investigates how real-time text streams can be processed effectively by comparing an optimized inference approach with a direct LLM-only baseline in a streaming environment . The project focuses on backend API log streams for anomaly detection and incident labeling, with emphasis on latency, throughput, inference cost, output quality, and developer experience .

The study is structured as a comparative evaluation of two parallel pipelines operating on the same Kafka stream. One pipeline applies direct cloud LLM inference, while the second pipeline applies an optimized approach that reduces unnecessary high-cost inference through intermediate processing stages.

## Research Scope
The thesis evaluates both approaches across analytical and engineering dimensions.

### Approach comparison
- **LLM-only baseline:** direct LLM-based inference for all relevant anomaly logs.
- **Optimized approach:** a lower-cost processing strategy that filters, reuses, or routes requests before LLM escalation.

### Evaluation dimensions
- Latency, throughput, and inference cost .
- Classification quality and decision effectiveness .
- Developer experience, including implementation effort, maintainability, and integration overhead.

This comparison is intended to determine whether the optimized approach provides measurable practical benefits over a direct LLM-only design, and under which conditions the extra system complexity is justified .

## Related Work Direction
The related work will position the thesis across three themes:

- Real-time text stream processing and distributed NLP architectures for streaming workloads .
- Cost-aware inference and model cascade ideas for stream-oriented systems, especially staged decision flows and confidence-based escalation .
- Comparison of relevant model and tool families, including statistical ML methods, compact embedding or small-model components, and cloud LLM services.

The literature review will support the selection of the concrete models and tools to be compared in the implementation, with attention to their practical suitability for real-time deployment and their trade-offs in quality, cost, and development effort .

## System Architecture
The implementation is based on a Dockerized experimental environment with Apache Kafka, PyFlink, and isolated processing paths for fair side-by-side comparison. Redis is used for real-time metric storage through point-overwrites, while PostgreSQL is used for historical persistence through bulk writes with JSONB fields for logs, routing metadata, and generated diagnoses.

A React dashboard with a FastAPI backend provides live visualization of both approaches, including throughput, latency, and accumulated token cost. This dashboard functions both as an experimental observability layer and as the final demonstration interface.

## Six-Month Project Plan

| Month | Focus | Planned outcome |
|---|---|---|
| Month 1 | Environment setup, data generation, initial stream validation, and preliminary literature collection | Functional Docker-based sandbox with Kafka and PyFlink components, synthetic log producer, validated parallel ingestion, and initial paper set |
| Month 2 | Structured related work review and design of the optimized comparison approach | Consolidated literature foundation, selected comparison models and tools, and initial optimized processing design |
| Month 3 | Integration of the optimized approach with Redis and PostgreSQL | Working comparative pipeline with real-time metrics and historical persistence |
| Month 4 | Completion of the LLM-only baseline and unified measurement framework across both paths | Full comparative system with aligned instrumentation and stable execution environment |
| Month 5 | Experimental evaluation of latency, cost, quality, setup effort, and developer experience | Comparative results across analytical and engineering dimensions |
| Month 6 | Final dashboard refinement, thesis writing, and preparation of final project submission | Completed thesis draft, polished demonstration system, and submission-ready materials |

## Expected Contribution
The thesis is expected to provide an empirical basis for understanding the benefits and trade-offs of an optimized processing approach compared with an LLM-only baseline in real-time text stream systems . In addition to analytical findings, the project contributes a practical comparison of setup complexity, developer experience, and maintainability across alternative inference designs.