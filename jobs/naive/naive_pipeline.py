"""
NAIVE pipeline: Kafka -> LLM -> sink.

The baseline / brute-force approach: EVERY log goes straight to the LLM, no
routing or cheaper tiers. This is the expensive-but-simple reference point the
optimized pipeline is compared against.

Run inside the Flink container:

    flink run \
      -py /opt/flink/jobs/naive/naive_pipeline.py \
      -pyfs /opt/flink/jobs

(`-pyfs /opt/flink/jobs` puts the `common` and `operators` packages on the
PYTHONPATH of both the client and the Python workers.)
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
from operators.llm_operator import classify_with_llm


def enrich(log: dict) -> dict:
    """Attach the LLM classification result to the log event."""
    result = classify_with_llm(log)
    return {**log, **result, "pipeline_type": "NAIVE"}


def format_result(log: dict) -> str:
    """Render one processed event as a readable line for the console sink."""
    return (
        f"[NAIVE] true={log.get('true_category')} "
        f"pred={log.get('predicted_category')} "
        f"route={log.get('routing_decision')} "
        f"msg={log.get('message', '')[:80]}"
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = build_kafka_source(
        topic=config.KAFKA_TOPIC,
        group_id=config.NAIVE_GROUP_ID,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-source"
    )

    (
        stream
        # raw JSON string -> dict (drop anything that fails to parse)
        .map(parse_log, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda log: log is not None)
        # dict -> dict enriched with the LLM result
        .map(enrich, output_type=Types.PICKLED_BYTE_ARRAY())
        # dict -> printable string (temporary console sink; Redis/Postgres later)
        .map(format_result, output_type=Types.STRING())
        .print()
    )

    env.execute("naive-llm-pipeline")


if __name__ == "__main__":
    main()
