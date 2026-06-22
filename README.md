# Minimal Kafka + Flink + PyFlink (Step-by-Step)

This is the smallest possible starting point for your thesis stack:

- Kafka (single broker, KRaft mode)
- Flink cluster (JobManager + TaskManager)
- PyFlink job submission (`flink run -py`)

## 1) Start the stack

```bash
docker compose up -d --build
```

Note: first build can take a few minutes on Apple Silicon because PyFlink dependencies are compiled in the Flink image.

Check containers:

```bash
docker compose ps
```

Open Flink UI:

- http://localhost:8081

## 2) Create a Kafka topic (inside Kafka container)

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic text-events \
  --partitions 1 \
  --replication-factor 1
```

List topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

## 3) Submit your first PyFlink job

```bash
docker compose exec jobmanager flink run -py /opt/flink/jobs/hello_stream.py
```

Watch TaskManager logs to see printed events:

```bash
docker compose logs -f taskmanager
```

## 4) Stop everything

```bash
docker compose down
```

## Why this is minimal

- No external database yet
- No model server yet
- No LLM API integration yet
- First focus: understand stream infra + job lifecycle

## Next step (after this works)

Build one tiny Kafka->Flink->print pipeline in PyFlink before adding real classifiers.
