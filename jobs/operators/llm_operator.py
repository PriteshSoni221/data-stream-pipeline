"""
LLM classifier operator (PLACEHOLDER).

In the real system this calls a paid LLM API to read `log["message"]` and
return a category + diagnosis. For now it just returns a stub so the pipeline
wiring can be built and tested end-to-end without any model.

Swap the body of `classify_with_llm` for a real API call later; the rest of
the pipeline does not need to change.
"""

from typing import Dict


def classify_with_llm(log: dict) -> Dict:
    """Classify a log message with an LLM.

    Returns the enrichment fields the pipeline attaches to the event.
    `routing_decision` mirrors the values stored in the Postgres schema.
    """
    # TODO: replace with a real LLM API call using log["message"].
    return {
        "predicted_category": "UNKNOWN",
        "routing_decision": "LLM_FALLBACK",
        "confidence_score": None,
        "ai_diagnosis": "PLACEHOLDER: LLM not integrated yet",
        "token_cost_usd": 0.0,
    }
