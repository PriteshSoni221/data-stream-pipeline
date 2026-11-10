# Reading List (literature sprint, 10 Nov 2026)

Candidates for the gaps listed in [project-plan.md](project-plan.md).
**None of these is citable yet.** A paper becomes citable once it is in Zotero
(collection `thesis`) and therefore in `resources/refs.bib`.

How to read this list:

- **Verified** means the title, authors and identifier were checked against
  the arXiv API or Crossref on 10 Nov 2026. It does not mean the paper was
  read. The "why" column is based on titles and abstracts only, so check each
  claim against the paper before using it.
- Venues are as stated in the arXiv comment field or Crossref record. Prefer
  the published version over the preprint when adding to Zotero.
- Priority: **1** = read first, it shapes an open decision; **2** = needed
  for Background or Related Work; **3** = optional.

## A. Model cascades and routing (gap 1)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 1 | Nie et al., *Online Cascade Learning for Efficient Inference over Streams* (ICML 2024) | arXiv:2402.04513 | Closest match found: a cascade of cheaper models in front of an LLM, on streams. Likely the main related work to position against. |
| 1 | Chen, Zaharia, Zou, *FrugalGPT: How to Use Large Language Models While Reducing Cost and Improving Performance* | arXiv:2305.05176 | The standard reference for LLM cascades. |
| 1 | Moslem, Kelleher, *Dynamic Model Routing and Cascading for Efficient LLM Inference: A Survey* (TMLR 2026) | arXiv:2603.04445 | Recent survey; gives the vocabulary (routing vs cascading) and a map of the field. |
| 2 | Ong et al., *RouteLLM: Learning to Route LLMs with Preference Data* | arXiv:2406.18665 | Routing (pick one model) as the alternative to cascading. |
| 2 | Ding et al., *Hybrid LLM: Cost-Efficient and Quality-Aware Query Routing* (ICLR 2024) | arXiv:2404.14618 | Small/large model routing by predicted query difficulty. |
| 2 | Gupta et al., *Language Model Cascades: Token-level Uncertainty and Beyond* | arXiv:2404.10136 | Confidence-based deferral rules, relevant to the escalation rule. |
| 2 | Aggarwal et al., *AutoMix: Automatically Mixing Language Models* (NeurIPS 2024) | arXiv:2310.12963 | Self-verification as the escalation signal. |
| 2 | Shankar, Zeighami, Parameswaran, *Task Cascades for Efficient Unstructured Data Processing* (SIGMOD 2026) | arXiv:2601.05536 | Same group as *Cut Costs, Not Accuracy*; cascades for data processing. |
| 3 | Varangot-Reille et al., *Doing More with Less: A Survey on Routing Strategies for Resource Optimisation in LLM-Based Systems* | arXiv:2502.00409 | Second survey. |
| 3 | Behera et al., *Towards Efficient Multi-LLM Inference: Characterization and Analysis of LLM Routing and Hierarchical Techniques* | arXiv:2506.06579 | Third survey, edge-oriented. |

Already held as PDF but missing from `refs.bib`: Zeighami, Shankar,
Parameswaran, *Cut Costs, Not Accuracy: LLM-Powered Data Processing with
Guarantees* (arXiv:2509.02896, to appear at SIGMOD 2026), and the *AOP* paper
(not found on arXiv by title; add from the PDF's own metadata).

## B. Log analysis (gap 2)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 1 | Akhtar, Khan, Parkinson, *LLM-based Event Log Analysis Techniques: A Survey* | arXiv:2502.00677 | Overview of LLMs on logs; helps decide the task framing. |
| 1 | Zhu et al., *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics* (ISSRE 2023) | arXiv:2008.06448 | The public datasets; decides whether an external dataset fits. |
| 1 | Cui et al., *LogEval: A Comprehensive Benchmark Suite for Large Language Models in Log Analysis* | arXiv:2407.01896 | Shows which log tasks and metrics LLMs are evaluated on. |
| 1 | Le, Zhang, *Log-based Anomaly Detection with Deep Learning: How Far Are We?* (ICSE 2022) | arXiv:2202.04301 | Critical evaluation of anomaly detection setups; relevant to framing and to threats to validity. |
| 2 | He et al., *Experience Report: System Log Analysis for Anomaly Detection* (ISSRE 2016) | doi:10.1109/ISSRE.2016.21 | Classical (statistical) methods on logs: a basis for the SML tier. |
| 2 | He et al., *Drain: An Online Log Parsing Approach with Fixed Depth Tree* (ICWS 2017) | doi:10.1109/ICWS.2017.13 | Standard online (streaming) log parser. |
| 2 | Du et al., *DeepLog: Anomaly Detection and Diagnosis from System Logs through Deep Learning* (CCS 2017) | doi:10.1145/3133956.3134015 | Standard deep-learning baseline. |
| 2 | Guo, Yuan, Wu, *LogBERT: Log Anomaly Detection via BERT* | arXiv:2103.04475 | Small language model on logs. |
| 2 | Jiang et al., *LILAC: Log Parsing using LLMs with Adaptive Parsing Cache* (FSE 2024) | doi:10.1145/3643733 | An LLM plus a cache to avoid repeated LLM calls on logs; close in spirit to the cascade and to the `CACHE_HIT` route. |
| 3 | Beck et al., *System Log Parsing with Large Language Models: A Review* | arXiv:2504.04877 | Review of LLM-based log parsers. |
| 3 | Zhu et al., *Tools and Benchmarks for Automated Log Parsing* (ICSE-SEIP 2019) | arXiv:1811.03509 | Log parsing benchmark. |
| 3 | Jiang et al., *A Large-Scale Evaluation for Log Parsing Techniques: How Far Are We?* (ISSTA 2024) | doi:10.1145/3650212.3652123 | Loghub-2.0 evaluation. |
| 3 | Le, Zhang, *Log Parsing with Prompt-based Few-shot Learning* (ICSE 2023) | arXiv:2302.07435 | Few-shot log parsing with a small language model. |

## C. Semantic caching (gap 3)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 2 | Bang, *GPTCache: An Open-Source Semantic Cache for LLM Applications Enabling Faster Answers and Cost Savings* (NLP-OSS 2023) | doi:10.18653/v1/2023.nlposs-1.24 | Backs the `CACHE_HIT` route. |
| 3 | Gill et al., *MeanCache: User-Centric Semantic Caching for LLM Web Services* (IPDPS 2025) | arXiv:2403.02694 | Peer-reviewed follow-up on semantic caching. |
| 3 | Regmi, Pun, *GPT Semantic Cache: Reducing LLM Costs and Latency via Semantic Embedding Caching* | arXiv:2411.05276 | Preprint only. |

## D. Statistical vs small vs large models for text classification (gap 4)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 1 | Cunha, Rocha, Gonçalves, *A Thorough Benchmark of Automatic Text Classification: From Traditional Approaches to Large Language Models* | arXiv:2504.01930 | Compares exactly the three tiers (traditional, SLM, LLM) on effectiveness and cost. |
| 2 | Pecher, Srba, Bielikova, *Comparing Specialised Small and General Large Language Models on Text Classification* (EMNLP 2025) | arXiv:2402.12819 | When a small specialised model matches a large general one. |
| 2 | Edwards, Camacho-Collados, *Language Models for Text Classification: Is In-Context Learning Enough?* (LREC-COLING 2024) | arXiv:2403.17661 | Fine-tuned small models vs prompted large ones. |
| 3 | Joulin et al., *Bag of Tricks for Efficient Text Classification* | arXiv:1607.01759 | fastText, a candidate SML method. |

## E. Stream processing: systems, benchmarking, LLMs in streams (gap 5)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 1 | Karimov et al., *Benchmarking Distributed Stream Data Processing Systems* (ICDE 2018) | arXiv:1802.08496 | Method for measuring latency and throughput in stream processors. |
| 1 | Chen, Raghavan, Çetintemel, *Continuous Prompts: LLM-Augmented Pipeline Processing over Unstructured Streams* | arXiv:2512.03389 | LLM operators inside a stream processing system; direct related work for the system side. |
| 3 | dos Santos et al., *Towards a Multimodal Stream Processing System* (vision paper) | arXiv:2510.14631 | LLMs as stream operators. |

## F. Developer experience (gap 6)

| P | Paper | Identifier | Why it matters here |
|---|---|---|---|
| 2 | Forsgren et al., *The SPACE of Developer Productivity* (ACM Queue 19(1), 2021) | doi:10.1145/3454122.3454124 | Framework for choosing which dimensions the effort comparison measures. |

## Leads not verified in this sprint

Named from general knowledge; the Crossref lookup did not return them. Look
them up before use.

- The original Kafka paper (Kreps et al., NetDB 2011).
- The Apache Flink paper (Carbone et al., IEEE Data Engineering Bulletin 2015).
- *Hidden Technical Debt in Machine Learning Systems* (Sculley et al., NeurIPS
  2015), for the cost of system complexity.
- The VectraFlow system paper (CIDR 2025), which *Continuous Prompts* builds on.
- A source on TF-IDF + linear classifiers for the SML tier.

## Gaps still open

- Gap 6 has one framework paper and no source on measuring engineering effort
  of ML pipelines specifically.
- No peer-reviewed paper was found on LLM inference inside Apache Flink; the
  search returned vendor blogs and documentation only.
