"""
OPTIMIZED pipeline: Kafka -> SML router -> (SML | SLM | LLM) -> sink.

The cost-aware approach. Each log first hits the cheap statistical router,
which decides whether it can be answered immediately (cache / SML), handed to
the small language model, or must be escalated to the expensive LLM.

The tiers are deliberately MODULAR: `sml_router`, `slm_operator` and
`llm_operator` are independent placeholders you can implement / swap one at a
time without touching this orchestration code.

Run inside the Flink container:

    flink run \
      -py /opt/flink/jobs/optimized/optimized_pipeline.py \
      -pyfs /opt/flink/jobs
"""

import os
import sys

# Make the sibling packages (common, operators) importable when this file is
# launched directly as the Flink entry point.
_JOBS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from common import config
from common.kafka_source import build_kafka_source
from common.serde import parse_log
from operators.slm_operator import classify_with_slm
from operators.sml_router import route

# Not yet ported to the instrumented operator interface used by the LLM-only
# arm. Blocked on the real SML router; porting it against the always-escalate
# stub would just duplicate the LLM-only arm.
_NOT_PORTED = (
    "The cascade arm is not implemented yet. Only the LLM-only arm "
    "(jobs/naive/naive_pipeline.py) is runnable. This arm needs the real SML "
    "router before it measures anything the LLM-only arm does not."
)


def enrich(log: dict) -> dict:
    """Run the tiered routing logic and attach the chosen result."""
    decision, result = route(log)

    if decision in ("CACHE_HIT", "SML_CLASSIFIED"):
        # The router classified it itself; `result` carries the enrichment.
        result = result or {}
    elif decision == "SLM_CLASSIFIED":
        result = classify_with_slm(log)
    else:
        raise NotImplementedError(_NOT_PORTED)

    return {**log, **result, "pipeline_type": "OPTIMIZED"}


def format_result(log: dict) -> str:
    """Render one processed event as a readable line for the console sink."""
    return (
        f"[OPTIMIZED] true={log.get('true_category')} "
        f"pred={log.get('predicted_category')} "
        f"route={log.get('routing_decision')} "
        f"msg={log.get('message', '')[:80]}"
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = build_kafka_source(
        topic=config.KAFKA_TOPIC,
        group_id=config.OPTIMIZED_GROUP_ID,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-source"
    )

    (
        stream
        .map(parse_log, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda log: log is not None)
        .map(enrich, output_type=Types.PICKLED_BYTE_ARRAY())
        .map(format_result, output_type=Types.STRING())
        .print()
    )

    env.execute("optimized-tiered-pipeline")


if __name__ == "__main__":
    raise SystemExit(_NOT_PORTED)
