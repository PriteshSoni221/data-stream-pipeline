#!/usr/bin/env python3
"""
Analyze the P0 throughput sweep.

Reads the JSONL emitted by each run and reports, per parallelism level, the
sustained throughput and the latency decomposition.

Throughput is computed from the record timestamps rather than from wall-clock
job duration, and excludes a warmup and cooldown margin. Job startup (JVM boot,
Python worker spawn, Kafka assignment) is several seconds and would otherwise
dominate the shorter high-parallelism runs.

Stdlib only, deliberately - this runs on the host, not in the Flink image.
"""

import json
import os
import statistics
import sys

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")

WARMUP_FRACTION = 0.10
COOLDOWN_FRACTION = 0.05


def load_run(run_dir: str) -> list:
    records = []
    for name in sorted(os.listdir(run_dir)):
        if not name.endswith(".jsonl"):
            continue
        with open(os.path.join(run_dir, name), encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def percentile(values: list, pct: float):
    if not values:
        return None
    ordered = sorted(values)
    index = min(int(round(pct / 100.0 * (len(ordered) - 1))), len(ordered) - 1)
    return ordered[index]


def steady_state_throughput(records: list):
    """Events per second over the middle of the run, from record exit times."""
    exits = sorted(r["t_op_exit"] for r in records if r.get("t_op_exit") is not None)
    if len(exits) < 10:
        return None, len(exits)
    start = int(len(exits) * WARMUP_FRACTION)
    end = len(exits) - int(len(exits) * COOLDOWN_FRACTION)
    window = exits[start:end]
    span = window[-1] - window[0]
    if span <= 0:
        return None, len(window)
    return (len(window) - 1) / span, len(window)


def fmt(value, digits=1):
    return "n/a" if value is None else f"{value:.{digits}f}"


def main() -> int:
    if not os.path.isdir(RESULTS_DIR):
        print(f"No results directory at {RESULTS_DIR}", file=sys.stderr)
        return 1

    run_dirs = sorted(
        d for d in os.listdir(RESULTS_DIR)
        if d.startswith("p0-par") and os.path.isdir(os.path.join(RESULTS_DIR, d))
    )
    if not run_dirs:
        print("No p0-par* runs found. Run experiments/p0_throughput_sweep.sh first.", file=sys.stderr)
        return 1

    rows = []
    for run in run_dirs:
        records = load_run(os.path.join(RESULTS_DIR, run))
        if not records:
            continue
        throughput, window_n = steady_state_throughput(records)
        service = [r["service_ms"] for r in records if r.get("service_ms") is not None]
        e2e = [r["e2e_ms"] for r in records if r.get("e2e_ms") is not None]
        queue = [r["queue_ms"] for r in records if r.get("queue_ms") is not None]
        rows.append({
            "parallelism": records[0]["parallelism"],
            "records": len(records),
            "subtasks": len({r["subtask"] for r in records}),
            "throughput": throughput,
            "window_n": window_n,
            "service_p50": percentile(service, 50),
            "service_p99": percentile(service, 99),
            "queue_p50": percentile(queue, 50),
            "e2e_p50": percentile(e2e, 50),
            "e2e_p99": percentile(e2e, 99),
        })

    rows.sort(key=lambda r: r["parallelism"])

    print(f"{'par':>4} {'recs':>6} {'subt':>5} {'eps':>8} {'scale':>7} "
          f"{'svc_p50':>8} {'svc_p99':>8} {'queue_p50':>10} {'e2e_p50':>9} {'e2e_p99':>9}")
    print("-" * 82)

    baseline = next((r["throughput"] for r in rows if r["parallelism"] == 1), None)
    for row in rows:
        if baseline and row["throughput"]:
            scale = f"{row['throughput'] / baseline:.2f}x"
        else:
            scale = "n/a"
        print(
            f"{row['parallelism']:>4} {row['records']:>6} {row['subtasks']:>5} "
            f"{fmt(row['throughput'], 2):>8} {scale:>7} "
            f"{fmt(row['service_p50']):>8} {fmt(row['service_p99']):>8} "
            f"{fmt(row['queue_p50']):>10} {fmt(row['e2e_p50']):>9} {fmt(row['e2e_p99']):>9}"
        )

    print()
    print("eps       = sustained events/sec (steady-state window)")
    print("scale     = speedup vs parallelism 1; near-linear means the lever works")
    print("svc_*     = time inside the classifier (the mock's own latency)")
    print("queue_p50 = time waiting in Kafka/backpressure before classification")
    print()
    print("Read this as: if 'scale' flattens well below the parallelism level,")
    print("parallelism is NOT a sufficient substitute for async I/O and the")
    print("operator needs an internal thread pool.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
