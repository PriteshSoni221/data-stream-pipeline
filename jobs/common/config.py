"""
Shared configuration for the Flink streaming jobs.

Values come from environment variables (so they can be overridden per
container / per run) with sensible defaults for the local Docker stack.
"""

import os

# Inside the Docker network, Flink reaches Kafka via the INTERNAL listener.
# (From the host you would use localhost:9092 instead.)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Topic the producers write log events to.
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "text-events")

# Separate consumer groups so the two pipelines consume the stream
# independently (each gets its own copy of every message).
NAIVE_GROUP_ID = os.getenv("NAIVE_GROUP_ID", "flink-naive-consumer")
OPTIMIZED_GROUP_ID = os.getenv("OPTIMIZED_GROUP_ID", "flink-optimized-consumer")
