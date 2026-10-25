"""
Measurement schema shared by every pipeline arm.

Every experimental arm emits exactly one record per event in this shape, so a
single offline analysis script can compare arms without knowing which pipeline
produced the data.

Timing model
------------
Four timestamps are captured per event (epoch seconds, float):

    t_event   producer wall-clock, parsed from the log's own `timestamp` field
    t_ingest  when the Flink pipeline deserialized the record
    t_op_enter / t_op_exit   around the classification call itself

From those we derive:

    service_ms  = t_op_exit - t_op_enter    time actually spent classifying
    e2e_ms      = t_op_exit - t_event       total time from production
    queue_ms    = e2e_ms - service_ms       everything else: Kafka + backpressure

Reporting service and queue time separately is the point. Under load, e2e is
dominated by queueing, so an arm can look "slow" purely because it is
saturated. Only the decomposition distinguishes a slow model from an
overloaded pipeline.

Caveat: t_event comes from the producer's clock (the host) while the other
three come from the Flink container's clock. Docker Desktop keeps these within
a few ms, but any cross-clock figure (e2e_ms, queue_ms) inherits that skew.
Purely in-Flink figures (service_ms, ingest->exit) are skew-free.
"""

import time
from datetime import datetime, timezone
from typing import Optional

# Every field an arm must emit. Ordered for readability in the JSONL output.
RECORD_FIELDS = (
    # run identity
    "run_id",
    "arm",
    "parallelism",
    "subtask",
    # event identity + ground truth
    "log_id",
    "true_category",
    "complexity",
    # prediction
    "predicted_category",
    "tier",
    "routing_decision",
    "confidence_score",
    # cost accounting
    "model_id",
    "tokens_in",
    "tokens_out",
    "cost_usd",
    # timing
    "t_event",
    "t_ingest",
    "t_op_enter",
    "t_op_exit",
    "service_ms",
    "e2e_ms",
    "queue_ms",
)


def now() -> float:
    """Wall-clock epoch seconds. Used for all cross-process timestamps."""
    return time.time()


def parse_event_time(timestamp: Optional[str]) -> Optional[float]:
    """Convert the producer's ISO-8601 `timestamp` field to epoch seconds."""
    if not timestamp:
        return None
    try:
        # datetime.fromisoformat only accepts 'Z' from Python 3.11 onward.
        normalized = timestamp.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (ValueError, TypeError):
        return None


def _millis(later: Optional[float], earlier: Optional[float]) -> Optional[float]:
    if later is None or earlier is None:
        return None
    return (later - earlier) * 1000.0


def build_record(
    event: dict,
    run_id: str,
    arm: str,
    parallelism: int,
    subtask: int,
) -> dict:
    """Project an enriched event (log fields merged with classifier output) into a measurement record."""
    t_event = parse_event_time(event.get("timestamp"))
    t_ingest = event.get("t_ingest")
    t_op_enter = event.get("t_op_enter")
    t_op_exit = event.get("t_op_exit")

    service_ms = _millis(t_op_exit, t_op_enter)
    e2e_ms = _millis(t_op_exit, t_event)
    queue_ms = None
    if e2e_ms is not None and service_ms is not None:
        queue_ms = e2e_ms - service_ms

    return {
        "run_id": run_id,
        "arm": arm,
        "parallelism": parallelism,
        "subtask": subtask,
        "log_id": event.get("log_id"),
        "true_category": event.get("true_category"),
        "complexity": event.get("complexity"),
        "predicted_category": event.get("predicted_category"),
        "tier": event.get("tier"),
        "routing_decision": event.get("routing_decision"),
        "confidence_score": event.get("confidence_score"),
        "model_id": event.get("model_id"),
        "tokens_in": event.get("tokens_in"),
        "tokens_out": event.get("tokens_out"),
        "cost_usd": event.get("cost_usd"),
        "t_event": t_event,
        "t_ingest": t_ingest,
        "t_op_enter": t_op_enter,
        "t_op_exit": t_op_exit,
        "service_ms": service_ms,
        "e2e_ms": e2e_ms,
        "queue_ms": queue_ms,
    }
