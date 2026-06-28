"""Hybrid batch + micro-batch pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class HybridPipelineConfig:
    batch_source_table: str
    stream_checkpoint: str
    target_table: str


class HybridPipeline:
    """Run batch backfill then attach streaming micro-batch."""

    def __init__(self, spark, batch_pipeline, streaming_pipeline) -> None:
        self.spark = spark
        self.batch_pipeline = batch_pipeline
        self.streaming_pipeline = streaming_pipeline

    def run_batch_then_stream(self, transform_fn) -> dict:
        batch_result = self.batch_pipeline.run()
        query = self.streaming_pipeline.start(transform_fn)
        return {"batch": batch_result, "streaming_query_id": query.id}
