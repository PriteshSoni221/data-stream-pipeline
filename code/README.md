# Real-Time API Log Classification — Streaming Stack

A streaming platform for comparing text-processing operators (statistical, SLM,
and LLM) on an Apache Flink pipeline. Synthetic API logs are produced into
Kafka, consumed by two competing Flink pipelines (a naive LLM-only baseline and
a cost-aware optimized pipeline), and — in later steps — persisted for live
dashboards and historical analysis.

## Architecture

```
producers (Python)  ->  Kafka topic (text-events)  ->  Flink pipelines  ->  sinks
                                                         ├─ naive     (LLM only)
                                                         └─ optimized (SML -> SLM -> LLM)
```

Current scope: producers and both Flink consumers are implemented. The AI
operators (SML router, SLM, LLM) are placeholders to be implemented later. The
Redis and PostgreSQL sinks are not yet wired; processed events are printed to
the TaskManager output.

## Components

| Component | Image / Runtime | Purpose |
|-----------|-----------------|---------|
| Kafka | `apache/kafka:3.7.1` (KRaft, single broker) | Message broker for the log stream |
| Flink JobManager | `thesis-flink-py:1.19.2` (custom) | Coordinates job execution; Web UI on `:8081` |
| Flink TaskManager | `thesis-flink-py:1.19.2` (custom) | Executes pipeline tasks (2 slots) |
| Producers | Python 3.9 + `confluent-kafka` | Generate and emit synthetic API logs |

The custom Flink image adds PyFlink and the Kafka connector
(`flink-sql-connector-kafka-3.2.0-1.19`) on top of the official Flink image.

## Repository layout

```
code/
├── docker-compose.yml          # Kafka + Flink JobManager + TaskManager
├── docker/flink-py/Dockerfile  # Custom PyFlink image (Kafka connector included)
├── producers/                  # Kafka log producer
│   ├── log_generator.py        # Synthetic API log generator (Kafka-agnostic)
│   ├── producer.py             # CLI producer (confluent-kafka)
│   └── requirements.txt
├── jobs/                       # PyFlink jobs
│   ├── common/                 # Shared config, Kafka source, (de)serialization
│   ├── operators/              # SML router, SLM, LLM operators (placeholders)
│   ├── naive/                  # Naive pipeline (LLM only)
│   ├── optimized/              # Optimized pipeline (tiered routing)
│   └── hello_stream.py         # Minimal demo job
└── database/
    └── create-table.sql        # PostgreSQL schema for historical storage
```

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for running the producers from the host)

## 1) Start the stack

```bash
docker compose up -d --build
```

The first build can take a few minutes on Apple Silicon because the PyFlink
dependencies are compiled inside the Flink image.

Check container status:

```bash
docker compose ps
```

The Flink Web UI is available at http://localhost:8081.

### Kafka listeners

Kafka exposes two client listeners:

- `INTERNAL` (`kafka:9092`) — used by other containers (Flink).
- `EXTERNAL` (`localhost:9092`) — used by host tools (the Python producer).

Containers connect via `kafka:9092`; host processes connect via
`localhost:9092`. The `text-events` topic is created automatically on first use
(auto-create is enabled).

## 2) Set up the producer environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r producers/requirements.txt
```

## 3) Produce synthetic logs

The producer emits synthetic API log events to the Kafka topic. Each event
carries a realistic raw log line (`message`), a ground-truth label
(`true_category`), and a difficulty tier (`complexity`) for evaluation.

```bash
# Emit 10 events at 5 events/second (defaults)
python producers/producer.py

# Stream 50 events/second indefinitely (stop with Ctrl+C)
python producers/producer.py --rate 50 --count 0

# Restrict categories and complexity, with a fixed seed for reproducibility
python producers/producer.py --categories DATABASE_ERROR TIMEOUT --complexity complex --seed 42
```

Producer options:

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap-servers` | `localhost:9092` | Kafka bootstrap servers |
| `--topic` | `text-events` | Target topic |
| `--rate` | `5` | Target events per second |
| `--count` | `10` | Number of events (`0` streams forever) |
| `--categories` | all | Restrict to specific categories |
| `--complexity` | all | Restrict to `simple` / `medium` / `complex` |
| `--seed` | none | Seed for reproducible streams |

The generator can also be run standalone to inspect sample events:

```bash
python producers/log_generator.py
```

## 4) Inspect the topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic text-events \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 8000
```

## 5) Submit a Flink pipeline

Both pipelines consume from `text-events`. The `-pyfs` flag puts the `common`
and `operators` packages on the Python path of the client and the workers.

```bash
# Naive pipeline (every log goes to the LLM)
docker compose exec jobmanager flink run -d \
  -py /opt/flink/jobs/naive/naive_pipeline.py \
  -pyfs /opt/flink/jobs

# Optimized pipeline (router -> SML | SLM | LLM)
docker compose exec jobmanager flink run -d \
  -py /opt/flink/jobs/optimized/optimized_pipeline.py \
  -pyfs /opt/flink/jobs
```

Each pipeline uses its own Kafka consumer group, so both can process the same
stream independently.

### Observe pipeline output

In the Docker-based Flink image, `print()` output is written to the container
stdout rather than a `.out` file:

```bash
docker compose logs -f taskmanager | grep -E "NAIVE|OPTIMIZED"
```

### Manage jobs

```bash
docker compose exec jobmanager flink list
docker compose exec jobmanager flink cancel <JobID>
```

Jobs can also be inspected and cancelled from the Web UI at
http://localhost:8081.

## 6) Stop everything

```bash
docker compose down
```

## Current status and next steps

Implemented:

- Kafka + Flink stack with PyFlink and the Kafka connector
- Synthetic log producer (controllable category, complexity, rate, and count)
- Naive and optimized Flink consumers with modular operator placeholders

Planned:

- Real SML router, SLM, and LLM operators
- Redis sink for live dashboard plotting
- PostgreSQL service and sink for historical storage (schema in
  `database/create-table.sql`)
