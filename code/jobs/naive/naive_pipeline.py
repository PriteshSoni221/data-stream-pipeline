"""
LLM-only arm: Kafka -> LLM -> instrumented JSONL sink.

The baseline. Every log goes straight to the LLM, no routing or cheaper tiers.
This is the expensive-but-simple reference point every other arm is compared
against, and the arm whose throughput ceiling constrains the whole design.

Run inside the Flink container:

    flink run -d \
      -py /opt/flink/jobs/naive/naive_pipeline.py \
      -pyfs /opt/flink/jobs \
      --run-id p0-par8 --parallelism 8 --latency-median-ms 500

Parallelism is a first-class experiment parameter, not a constant: PyFlink has
no Async I/O, so it is the only way to overlap blocking inference calls.

The consumer group is derived from --run-id so every run is a fresh group and
therefore replays the topic from the earliest offset. Reusing a group id would
silently make the second run of an experiment consume nothing.
"""

import argparse
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
from common.instrumentation import now
from common.kafka_source import build_kafka_source
from common.serde import parse_log
from common.sink import JsonlSink, RecordBuilder
from operators.llm_operator import LlmClassifier

ARM = "llm_only"


def parse_and_stamp(record: str):
    """Deserialize a Kafka record and mark when the pipeline first saw it."""
    log = parse_log(record)
    if log is None:
        return None
    log["t_ingest"] = now()
    return log


def summarize(record: dict) -> str:
    return (
        f"[{record['arm']} p={record['parallelism']}] "
        f"true={record.get('true_category')} "
        f"pred={record.get('predicted_category')} "
        f"service_ms={record.get('service_ms')}"
    )


def parse_args(argv):
    parser = argparse.ArgumentParser(description="LLM-only pipeline arm")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--llm-backend", default="mock", choices=["mock", "api"])
    parser.add_argument("--latency-median-ms", type=float, default=500.0)
    parser.add_argument("--latency-sigma", type=float, default=0.35)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--results-dir", default="/opt/flink/results")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    source = build_kafka_source(
        topic=config.KAFKA_TOPIC,
        group_id=f"{ARM}-{args.run_id}",
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    )

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "kafka-source"
    )

    (
        stream
        .map(parse_and_stamp, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda log: log is not None)
        .map(
            LlmClassifier(
                backend=args.llm_backend,
                latency_median_ms=args.latency_median_ms,
                latency_sigma=args.latency_sigma,
                seed=args.seed,
            ),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .map(
            RecordBuilder(args.run_id, ARM, args.parallelism),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .map(
            JsonlSink(args.run_id, args.results_dir),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .map(summarize, output_type=Types.STRING())
        .print()
    )

    env.execute(f"{ARM}-{args.run_id}")


if __name__ == "__main__":
    main()
