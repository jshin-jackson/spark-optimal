"""Streaming pipeline skeleton for Kafka -> Iceberg."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StreamingPipelineConfig:
    kafka_topic: str
    kafka_bootstrap: str
    target_table: str
    checkpoint_location: str


class StreamingPipeline:
    def __init__(self, spark, config: StreamingPipelineConfig) -> None:
        self.spark = spark
        self.config = config

    def build_query(self):
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap)
            .option("subscribe", self.config.kafka_topic)
            .option("startingOffsets", "latest")
            .load()
        )

    def start(self, transform_fn):
        stream_df = self.build_query()
        transformed = transform_fn(stream_df)
        return (
            transformed.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", self.config.checkpoint_location)
            .toTable(self.config.target_table)
            .start()
        )
