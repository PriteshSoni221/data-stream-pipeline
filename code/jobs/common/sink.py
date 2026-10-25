"""
JSONL results sink.

Writes one measurement record per line to a run-scoped directory, one file per
subtask so parallel writers never contend:

    /opt/flink/results/<run_id>/part-<subtask>.jsonl

Deliberately not the PyFlink JDBC sink. Postgres is loaded afterwards by a
separate batch script; that keeps the hot path free of a database round-trip
that would otherwise show up inside the latency being measured.
"""

import json
import os

from pyflink.datastream.functions import MapFunction, RuntimeContext

from common.instrumentation import build_record


class RecordBuilder(MapFunction):
    """Project an enriched event into the shared measurement schema."""

    def __init__(self, run_id: str, arm: str, parallelism: int):
        self.run_id = run_id
        self.arm = arm
        self.parallelism = parallelism
        self._subtask = 0

    def open(self, runtime_context: RuntimeContext):
        self._subtask = runtime_context.get_index_of_this_subtask()

    def map(self, event: dict) -> dict:
        return build_record(
            event,
            run_id=self.run_id,
            arm=self.arm,
            parallelism=self.parallelism,
            subtask=self._subtask,
        )


class JsonlSink(MapFunction):
    """Append each record to this subtask's JSONL file, then pass it through."""

    def __init__(self, run_id: str, results_dir: str = "/opt/flink/results"):
        self.run_id = run_id
        self.results_dir = results_dir
        self._handle = None

    def open(self, runtime_context: RuntimeContext):
        subtask = runtime_context.get_index_of_this_subtask()
        run_dir = os.path.join(self.results_dir, self.run_id)
        os.makedirs(run_dir, exist_ok=True)
        path = os.path.join(run_dir, f"part-{subtask}.jsonl")
        # Line buffering: results survive a job cancel without an explicit flush.
        self._handle = open(path, "a", buffering=1, encoding="utf-8")

    def map(self, record: dict) -> dict:
        self._handle.write(json.dumps(record) + "\n")
        return record

    def close(self):
        if self._handle is not None:
            self._handle.close()
            self._handle = None
