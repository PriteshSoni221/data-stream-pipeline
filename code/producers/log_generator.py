"""
Synthetic API log generator for the text-stream thesis pipelines.

Use case: "Multi-Class Log Classification & Incident Labeling"
(Real-Time API Log Anomaly & Diagnosis Categorization).

Each generated event carries:
  - `message`       : a realistic raw application-log line (timestamp, level,
                      service, trace id + diagnostic text) the pipelines classify.
  - `true_category` : the ground-truth label, used later to score accuracy / F1.
  - `complexity`    : a difficulty tier (simple / medium / complex) so models can
                      be evaluated across a spectrum of linguistic difficulty.

This module ONLY generates events. It knows nothing about Kafka, so it can be
reused by the producer, by tests, or by an offline dataset builder.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Iterable, Iterator, Optional

# ---------------------------------------------------------------------------
# Static pools used to fill in template placeholders.
# ---------------------------------------------------------------------------

SERVICES = [
    "payments-api",
    "auth-service",
    "user-service",
    "orders-api",
    "inventory-service",
    "notification-service",
    "gateway",
]

ENDPOINTS = [
    "GET /api/v1/users/{id}",
    "POST /api/v1/payments",
    "GET /api/v1/orders/{id}",
    "PUT /api/v1/inventory/{id}",
    "POST /api/v1/auth/login",
    "DELETE /api/v1/sessions/{id}",
    "GET /api/v1/products",
]

DB_TABLES = ["users", "orders", "payments",
             "inventory", "sessions", "accounts"]

COMPLEXITY_LEVELS = ["simple", "medium", "complex"]

# ---------------------------------------------------------------------------
# Log templates grouped by category and complexity tier.
#
# - "simple"  : explicit keywords, easy for any classifier (e.g. "ERROR: timeout").
# - "medium"  : realistic phrasing, moderately explicit.
# - "complex" : subtle wording that requires understanding rather than keywords.
#
# Placeholders in {curly_braces} are filled in by `_fill_placeholders`.
# ---------------------------------------------------------------------------

TEMPLATES: dict[str, dict[str, list[str]]] = {
    "DATABASE_ERROR": {
        "simple": [
            "ERROR: database connection failed on table {table}",
            "ERROR: could not connect to PostgreSQL database",
            "FATAL: database query failed for table {table}",
        ],
        "medium": [
            "Query execution exceeded pool timeout while reading from {table} ({latency} ms)",
            "Connection pool exhausted: no available connections for {service}",
            "Deadlock detected on table {table} during transaction commit",
        ],
        "complex": [
            "Request to {endpoint} returned partial data; downstream persistence layer "
            "appears unresponsive after {latency} ms",
            "Intermittent slowdowns observed; replica lag on {table} growing beyond "
            "acceptable bounds during peak traffic",
        ],
    },
    "AUTHENTICATION_ERROR": {
        "simple": [
            "ERROR: invalid credentials for user {user}",
            "401 Unauthorized: authentication failed on {endpoint}",
            "ERROR: token expired for user {user}",
        ],
        "medium": [
            "JWT signature verification failed for request to {endpoint}",
            "Session token rejected: issuer mismatch for user {user}",
            "Repeated failed login attempts detected from {ip}",
        ],
        "complex": [
            "User {user} reached a protected resource but the bearer token's audience "
            "claim did not match the expected service identity",
            "Access to {endpoint} was denied despite a valid session; role bindings "
            "may be out of sync after the last deployment",
        ],
    },
    "RATE_LIMIT_EXCEEDED": {
        "simple": [
            "429 Too Many Requests on {endpoint}",
            "ERROR: rate limit exceeded for client {ip}",
            "Throttled: request rate limit hit for {service}",
        ],
        "medium": [
            "Client {ip} exceeded the configured quota of requests per minute on {endpoint}",
            "Rate limiter rejected burst of traffic to {service} from {ip}",
        ],
        "complex": [
            "Traffic from {ip} was shed to protect {service}; sustained call volume "
            "remained above the rolling threshold for several windows",
        ],
    },
    "TIMEOUT": {
        "simple": [
            "ERROR: request to {endpoint} timed out after {latency} ms",
            "Gateway timeout (504) on {endpoint}",
            "ERROR: upstream timeout calling {service}",
        ],
        "medium": [
            "Upstream call to {service} did not complete within the {latency} ms budget",
            "Read timeout while waiting for response from {endpoint}",
        ],
        "complex": [
            "The aggregation step for {endpoint} stalled waiting on a slow dependency; "
            "the caller gave up after {latency} ms",
        ],
    },
    "VALIDATION_ERROR": {
        "simple": [
            "400 Bad Request: missing required field on {endpoint}",
            "ERROR: validation failed for payload on {endpoint}",
            "ERROR: invalid input format for {endpoint}",
        ],
        "medium": [
            "Schema validation rejected request to {endpoint}: unexpected field type",
            "Payload for {endpoint} failed constraint checks before processing",
        ],
        "complex": [
            "Request to {endpoint} was syntactically valid but referenced an entity "
            "that does not satisfy business rules for {service}",
        ],
    },
    "INTERNAL_SERVER_ERROR": {
        "simple": [
            "500 Internal Server Error on {endpoint}",
            "ERROR: unhandled exception in {service}",
            "FATAL: null pointer exception while processing {endpoint}",
        ],
        "medium": [
            "Unhandled exception bubbled up from {service} while serving {endpoint}",
            "Background worker for {service} crashed and restarted unexpectedly",
        ],
        "complex": [
            "A rare code path in {service} produced an inconsistent state; the response "
            "for {endpoint} could not be assembled and the request was aborted",
        ],
    },
    "NETWORK_ERROR": {
        "simple": [
            "ERROR: connection refused to {service}",
            "ERROR: DNS resolution failed for {service}",
            "ERROR: TLS handshake failed with {service}",
        ],
        "medium": [
            "Transient network partition prevented {service} from reaching {endpoint}",
            "Socket reset while streaming response from {service}",
        ],
        "complex": [
            "Calls to {service} intermittently fail; packet loss between availability "
            "zones is degrading connectivity for {endpoint}",
        ],
    },
    "NORMAL": {
        "simple": [
            "200 OK {endpoint} ({latency} ms)",
            "INFO: request to {endpoint} completed successfully",
            "INFO: user {user} authenticated successfully",
        ],
        "medium": [
            "Request to {endpoint} served from cache in {latency} ms",
            "Scheduled health check for {service} passed",
        ],
        "complex": [
            "Processed batch for {service} and committed results; throughput stable "
            "at expected levels for {endpoint}",
        ],
    },
}

# Per-category metadata used to keep generated fields *coherent* (not random noise):
#   level   : typical log severity for this category.
#   status  : plausible HTTP status codes.
#   latency : (min_ms, max_ms) range so duration matches the scenario, e.g.
#             TIMEOUT is slow by definition while a rejected request is fast.
_CATEGORY_META = {
    "DATABASE_ERROR": {"level": "ERROR", "status": [500, 503], "latency": (200, 8000)},
    "AUTHENTICATION_ERROR": {"level": "WARN", "status": [401, 403], "latency": (5, 200)},
    "RATE_LIMIT_EXCEEDED": {"level": "WARN", "status": [429], "latency": (1, 50)},
    "TIMEOUT": {"level": "ERROR", "status": [504], "latency": (3000, 10000)},
    "VALIDATION_ERROR": {"level": "WARN", "status": [400, 422], "latency": (2, 80)},
    "INTERNAL_SERVER_ERROR": {"level": "ERROR", "status": [500], "latency": (50, 3000)},
    "NETWORK_ERROR": {"level": "ERROR", "status": [502, 503], "latency": (100, 6000)},
    "NORMAL": {"level": "INFO", "status": [200, 201, 204], "latency": (5, 400)},
}

CATEGORIES = list(TEMPLATES.keys())


class LogGenerator:
    """Generates synthetic API log events with ground-truth labels.

    Parameters
    ----------
    categories:
        Restrict generation to these categories. Defaults to all.
    complexities:
        Restrict generation to these complexity tiers. Defaults to all.
    seed:
        Optional seed for reproducible streams.
    """

    def __init__(
        self,
        categories: Optional[Iterable[str]] = None,
        complexities: Optional[Iterable[str]] = None,
        seed: Optional[int] = None,
    ) -> None:
        self._rng = random.Random(seed)

        self.categories = list(categories) if categories else list(CATEGORIES)
        self.complexities = list(
            complexities) if complexities else list(COMPLEXITY_LEVELS)

        self._validate()

    def _validate(self) -> None:
        unknown_cat = set(self.categories) - set(CATEGORIES)
        if unknown_cat:
            raise ValueError(
                f"Unknown categories: {sorted(unknown_cat)}. "
                f"Valid options: {CATEGORIES}"
            )
        unknown_cx = set(self.complexities) - set(COMPLEXITY_LEVELS)
        if unknown_cx:
            raise ValueError(
                f"Unknown complexity levels: {sorted(unknown_cx)}. "
                f"Valid options: {COMPLEXITY_LEVELS}"
            )

    # -- internal helpers --------------------------------------------------

    @staticmethod
    def _format_log_line(
        ts: datetime,
        level: str,
        service: str,
        host: str,
        trace_id: str,
        body: str,
    ) -> str:
        """Wrap the diagnostic text in a realistic raw application-log line.

        Example:
            2026-06-23T16:28:49.708Z ERROR notification-service \
            host=ip-10-0-4-180 trace_id=ab12cd34ef56 Gateway timeout (504) ...

        The discriminative content stays in `body`; the prefix only adds
        realistic, neutral structure (timestamp, level, service, trace id).
        """
        stamp = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return f"{stamp} {level:<5} {service} host={host} trace_id={trace_id} {body}"

    # -- public API --------------------------------------------------------

    def generate(self) -> dict:
        """Generate a single log event as a plain dict (JSON-serializable)."""
        category = self._rng.choice(self.categories)
        complexity = self._rng.choice(self.complexities)
        meta = _CATEGORY_META[category]

        # Build ONE coherent context and reuse it everywhere, so the structured
        # fields and the raw `message` line describe the same imaginary request.
        ts = datetime.now(timezone.utc)
        service = self._rng.choice(SERVICES)
        host = f"ip-10-0-{self._rng.randint(0, 9)}-{self._rng.randint(1, 254)}"
        client_ip = f"10.0.{self._rng.randint(0, 255)}.{self._rng.randint(1, 254)}"
        endpoint = self._rng.choice(ENDPOINTS).replace(
            "{id}", str(self._rng.randint(1000, 9999)))
        user = f"user_{self._rng.randint(1, 5000)}"
        table = self._rng.choice(DB_TABLES)
        trace_id = uuid.uuid4().hex[:12]

        # Latency is drawn from a category-appropriate range (not pure noise),
        # so e.g. TIMEOUT is slow and a rejected request is fast.
        latency_ms = self._rng.randint(*meta["latency"])

        # Fill the chosen template with the SAME context values.
        template = self._rng.choice(TEMPLATES[category][complexity])
        body = template.format(
            service=service,
            endpoint=endpoint,
            table=table,
            user=user,
            ip=client_ip,
            latency=latency_ms,
        )

        message = self._format_log_line(
            ts, meta["level"], service, host, trace_id, body)

        return {
            "log_id": str(uuid.uuid4()),
            "timestamp": ts.isoformat(),
            "service": service,
            "host": host,
            "level": meta["level"],
            "status_code": self._rng.choice(meta["status"]),
            "latency_ms": latency_ms,
            "message": message,
            # Ground-truth fields — used for evaluation, NOT for inference input.
            "true_category": category,
            "complexity": complexity,
        }

    def stream(self, count: Optional[int] = None) -> Iterator[dict]:
        """Yield log events.

        If `count` is None or 0, yields forever.
        """
        if not count:
            while True:
                yield self.generate()
        else:
            for _ in range(count):
                yield self.generate()


if __name__ == "__main__":
    # Quick manual smoke test: print a few sample events.
    import json

    gen = LogGenerator(seed=42)
    for event in gen.stream(count=5):
        print(json.dumps(event, indent=2))
