"""
SML (statistical / lightweight ML) router operator (PLACEHOLDER).

This is the first, cheapest tier of the optimized pipeline. Its job is to
decide HOW each log should be handled, returning one of:

    "CACHE_HIT"       -> identical/similar message already classified before.
    "SML_CLASSIFIED"  -> the statistical model is confident enough to label it.
    "SLM_CLASSIFIED"  -> hand off to the small language model.
    "LLM_FALLBACK"    -> too hard / low confidence, escalate to the LLM.

Real statistical methods (keyword rules, TF-IDF + logistic regression, an
embedding cache, etc.) will be implemented later. For now it always escalates
to the LLM so the optimized pipeline behaves like the naive one until the
cheaper tiers exist.
"""

from typing import Dict, Optional, Tuple


def route(log: dict) -> Tuple[str, Optional[Dict]]:
    """Decide how to handle a log.

    Returns a (routing_decision, result) tuple. When the router can classify
    the log itself (CACHE_HIT / SML_CLASSIFIED) it returns the enrichment dict
    as the second element; otherwise it returns None and the pipeline escalates
    to the SLM or LLM.
    """
    # TODO: implement real statistical routing. For now, escalate everything.
    return "LLM_FALLBACK", None
