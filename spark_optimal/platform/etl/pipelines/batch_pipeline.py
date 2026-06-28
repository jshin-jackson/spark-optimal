"""Batch ETL pipeline orchestration."""

from __future__ import annotations

import time
from dataclasses import dataclass

from spark_optimal.platform.etl.components.extractors import IcebergExtractor
from spark_optimal.platform.etl.components.loaders import IcebergLoader
from spark_optimal.platform.etl.components.transformers import EnterpriseTransformer, TransformationConfig


@dataclass
class BatchPipelineConfig:
    source_table: str
    target_table: str
    target_location: str | None = None
    partition_cols: list[str] | None = None
    sla_name: str = "critical_etl"


class BatchPipeline:
    def __init__(self, spark, transformer: EnterpriseTransformer, config: BatchPipelineConfig) -> None:
        self.spark = spark
        self.transformer = transformer
        self.config = config

    def run(self) -> dict:
        start = time.time()
        source_df = IcebergExtractor(self.spark, self.config.source_table).extract()
        transformed = self.transformer.execute_with_governance(
            source_df,
            TransformationConfig(sla_name=self.config.sla_name, required_columns=list(source_df.columns)),
        )
        count = IcebergLoader(
            self.config.target_table,
            self.config.target_location,
            self.config.partition_cols,
        ).load(transformed)
        return {
            "source_table": self.config.source_table,
            "target_table": self.config.target_table,
            "output_count": count,
            "duration_seconds": time.time() - start,
        }
