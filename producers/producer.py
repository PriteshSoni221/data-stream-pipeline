"""
Kafka log producer for the text-stream thesis pipelines.

Emits synthetic API log events (from `log_generator.LogGenerator`) to a Kafka
topic. Designed to give you fine-grained control over WHAT and HOW MUCH you send:

    python producer.py --rate 10 --count 100
    python producer.py --categories DATABASE_ERROR TIMEOUT --complexity complex
    python producer.py --rate 50 --count 0        # 0 = stream forever
    python producer.py --topic api-logs --seed 42  # reproducible run

Stop an infinite run with Ctrl+C; buffered messages are flushed on exit.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time

from confluent_kafka import Producer

from log_generator import CATEGORIES, COMPLEXITY_LEVELS, LogGenerator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit synthetic API logs to a Kafka topic.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port).",
    )
    parser.add_argument(
        "--topic",
        default="text-events",
        help="Kafka topic to produce to (auto-created if missing).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Target events per second (throttled). Use a high value for max speed.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to emit. 0 means stream forever.",
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=CATEGORIES,
        default=None,
        help="Restrict to these categories (default: all).",
    )
    parser.add_argument(
        "--complexity",
        nargs="+",
        choices=COMPLEXITY_LEVELS,
        default=None,
        help="Restrict to these complexity tiers (default: all).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible event streams.",
    )
    return parser.parse_args()


def delivery_report(err, msg) -> None:
    """Called once per message to report delivery success or failure."""
    if err is not None:
        print(f"[DELIVERY FAILED] {err}", file=sys.stderr)
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={msg.key().decode() if msg.key() else None}"
        )


def main() -> int:
    args = parse_args()

    generator = LogGenerator(
        categories=args.categories,
        complexities=args.complexity,
        seed=args.seed,
    )

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    # Graceful shutdown flag, flipped by SIGINT (Ctrl+C) / SIGTERM.
    running = {"on": True}

    def _stop(_signum, _frame):
        print("\nStopping... flushing buffered messages.")
        running["on"] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    infinite = args.count == 0

    print(
        f"Producing to topic '{args.topic}' on {args.bootstrap_servers} "
        f"(rate={args.rate}/s, count={'inf' if infinite else args.count})"
    )

    sent = 0
    try:
        for event in generator.stream(count=None if infinite else args.count):
            if not running["on"]:
                break

            # Use the originating service as the Kafka key so logs from the same
            # service land in the same partition (preserves per-service order).
            key = event["service"].encode("utf-8")
            value = json.dumps(event).encode("utf-8")

            # Serve delivery callbacks for previously produced messages.
            producer.poll(0)

            try:
                producer.produce(
                    topic=args.topic,
                    key=key,
                    value=value,
                    callback=delivery_report,
                )
            except BufferError:
                # Local queue is full; wait for it to drain, then retry once.
                producer.poll(0.5)
                producer.produce(
                    topic=args.topic,
                    key=key,
                    value=value,
                    callback=delivery_report,
                )

            sent += 1
            if interval:
                time.sleep(interval)
    finally:
        remaining = producer.flush(10)
        if remaining:
            print(
                f"[WARN] {remaining} messages still undelivered after flush.", file=sys.stderr)
        print(f"Done. Emitted {sent} event(s).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
