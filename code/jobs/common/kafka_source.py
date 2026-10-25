"""
Helper to build a Kafka source for the Flink DataStream API.

Keeping this in one place means both pipelines (naive / optimized) construct
their Kafka consumer the same way and only differ in the processing logic.
"""

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)


def build_kafka_source(
    topic: str,
    group_id: str,
    bootstrap_servers: str,
) -> KafkaSource:
    """Build a KafkaSource that reads raw JSON strings from `topic`.

    Each Kafka message value is delivered to the job as a plain string
    (the JSON produced by the log producer); parsing happens downstream.
    """
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        # Start from the earliest offset so we can replay everything already
        # in the topic. Switch to .latest() to only read new messages.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
