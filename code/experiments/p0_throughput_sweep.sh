#!/usr/bin/env bash
#
# P0 de-risk spike: does throughput scale with parallelism?
#
# PyFlink has no Async I/O, so a blocking inference call occupies its Python
# worker for the whole call. The only concurrency lever is parallelism. This
# sweep answers whether that lever actually works before any of the research
# design is built on top of it.
#
# Method: preload the topic with a fixed number of events, then drain it once
# per parallelism level with a fresh consumer group. The queue is saturated
# throughout, so what is measured is maximum sustained throughput.
#
# Expected: throughput ~= parallelism / latency_median. Flat scaling past some
# point means the architecture needs redesigning, and that is exactly what this
# spike is meant to surface early.
#
# Usage:  ./experiments/p0_throughput_sweep.sh [EVENTS] [LATENCY_MS]

set -euo pipefail

EVENTS="${1:-400}"
LATENCY_MS="${2:-500}"
PARALLELISMS=(1 2 4 8 16)
TOPIC="text-events"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/results"

cd "${REPO_ROOT}"

echo "==> P0 throughput sweep: ${EVENTS} events, mock latency ${LATENCY_MS}ms"
echo "==> Parallelism levels: ${PARALLELISMS[*]}"

if [[ ! -x .venv/bin/python ]]; then
  echo "ERROR: .venv not found. Run: python3 -m venv .venv && .venv/bin/pip install -r producers/requirements.txt" >&2
  exit 1
fi

# Fresh topic so the event count is exactly known.
echo "==> Recreating topic '${TOPIC}'"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --delete --topic "${TOPIC}" 2>/dev/null || true
sleep 3
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --topic "${TOPIC}" \
  --partitions 16 --replication-factor 1 2>/dev/null || true

# 16 partitions: Kafka caps consumer parallelism at the partition count, so
# fewer partitions than slots would cap the sweep rather than the design.

echo "==> Producing ${EVENTS} events"
.venv/bin/python producers/producer.py \
  --bootstrap-servers localhost:9092 \
  --topic "${TOPIC}" \
  --count "${EVENTS}" \
  --rate 100000 \
  --seed 42 >/dev/null

for P in "${PARALLELISMS[@]}"; do
  RUN_ID="p0-par${P}"
  echo ""
  echo "==> Parallelism ${P} (run_id=${RUN_ID})"

  rm -rf "${RESULTS_DIR:?}/${RUN_ID}"

  SUBMIT_OUTPUT=$(docker compose exec -T jobmanager flink run -d \
    -py /opt/flink/jobs/naive/naive_pipeline.py \
    -pyfs /opt/flink/jobs \
    --run-id "${RUN_ID}" \
    --parallelism "${P}" \
    --latency-median-ms "${LATENCY_MS}" 2>&1)

  JOB_ID=$(echo "${SUBMIT_OUTPUT}" | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}' | head -1)
  if [[ -z "${JOB_ID}" ]]; then
    echo "ERROR: could not submit job." >&2
    echo "${SUBMIT_OUTPUT}" >&2
    exit 1
  fi
  echo "    JobID ${JOB_ID}"

  # Drain, with a generous ceiling so a stalled run fails loudly instead of hanging.
  TIMEOUT=$(( EVENTS * LATENCY_MS / 1000 / P + 120 ))
  START=$(date +%s)
  while true; do
    # Braces + `|| true`: before the first record lands the glob does not match,
    # and under `pipefail` that would otherwise abort the whole sweep.
    COUNT=$( { cat "${RESULTS_DIR}/${RUN_ID}"/*.jsonl 2>/dev/null || true; } | wc -l | tr -d ' ')
    ELAPSED=$(( $(date +%s) - START ))
    if [[ "${COUNT}" -ge "${EVENTS}" ]]; then
      echo "    processed ${COUNT}/${EVENTS} in ${ELAPSED}s"
      break
    fi
    if [[ "${ELAPSED}" -gt "${TIMEOUT}" ]]; then
      echo "    TIMEOUT after ${ELAPSED}s with ${COUNT}/${EVENTS} processed"
      break
    fi
    sleep 2
  done

  docker compose exec -T jobmanager flink cancel "${JOB_ID}" >/dev/null 2>&1 || true
  sleep 3
done

echo ""
echo "==> Sweep complete. Analyzing:"
echo ""
.venv/bin/python experiments/analyze_p0.py
