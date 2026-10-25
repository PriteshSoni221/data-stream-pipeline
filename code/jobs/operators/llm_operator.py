"""
LLM classification tier.

Two backends share one interface so the pipeline code never changes:

    mock    sleeps for a latency drawn from a lognormal distribution and
            returns no prediction. Used to characterise the *system* -
            throughput, backpressure, scaling - without spending API budget.
    api     real provider call. Not implemented yet.

The mock exists because the concurrency question and the accuracy question are
independent, and only the first one blocks the architecture. PyFlink has no
Async I/O (AsyncDataStream is Java-only), so the only lever for overlapping
blocking calls is parallelism - one Python worker process per task slot. The
mock lets that ceiling be measured before any billing is set up, and later lets
the load sweep replay measured latencies for free.

`predicted_category` is deliberately None under the mock so that mock output can
never be mistaken for a quality result.
"""

import math
import random
import time

from pyflink.datastream.functions import MapFunction, RuntimeContext

from common.instrumentation import now


class LlmClassifier(MapFunction):
    """Classify a log via the LLM tier, recording entry/exit timestamps."""

    def __init__(
        self,
        backend: str = "mock",
        latency_median_ms: float = 500.0,
        latency_sigma: float = 0.35,
        seed: int = 42,
        model_id: str = "mock-llm",
    ):
        self.backend = backend
        self.latency_median_ms = latency_median_ms
        self.latency_sigma = latency_sigma
        self.seed = seed
        self.model_id = model_id
        self._rng = None

    def open(self, runtime_context: RuntimeContext):
        # Offset by subtask so parallel workers don't draw identical latencies
        # while the run as a whole stays reproducible.
        subtask = runtime_context.get_index_of_this_subtask()
        self._rng = random.Random(self.seed + subtask)

    def _sleep_mock(self) -> None:
        # Lognormal: right-skewed, matching the shape of real API latency.
        mu = math.log(self.latency_median_ms / 1000.0)
        time.sleep(self._rng.lognormvariate(mu, self.latency_sigma))

    def map(self, log: dict) -> dict:
        t_enter = now()

        if self.backend == "mock":
            self._sleep_mock()
            result = {
                "predicted_category": None,
                "tier": "LLM",
                "routing_decision": "LLM_FALLBACK",
                "confidence_score": None,
                "model_id": self.model_id,
                "tokens_in": None,
                "tokens_out": None,
                "cost_usd": None,
            }
        else:
            raise NotImplementedError(f"LLM backend not implemented: {self.backend}")

        return {
            **log,
            **result,
            "t_op_enter": t_enter,
            "t_op_exit": now(),
        }
