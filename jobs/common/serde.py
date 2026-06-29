"""
(De)serialization helpers shared by the pipelines.

The producer emits one JSON object per Kafka message. These helpers convert
between the raw JSON string on the wire and a Python dict the operators use.
"""

import json
from typing import Optional


def parse_log(record: str) -> Optional[dict]:
    """Parse a raw JSON log string into a dict.

    Returns None for malformed records so the pipeline can filter them out
    instead of crashing on bad input.
    """
    try:
        return json.loads(record)
    except (json.JSONDecodeError, TypeError):
        return None
