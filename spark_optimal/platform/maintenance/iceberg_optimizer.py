"""Intelligent Iceberg table optimizer."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from spark_optimal.platform.maintenance.auto_compaction import AutoCompactor
from spark_optimal.platform.maintenance.predictive_maintenance import PredictiveMaintenanceEngine

logger = logging.getLogger(__name__)


@dataclass
class OptimizationRecommendation:
    priority: str
    requires_compaction: bool
    requires_expire_snapshots: bool
    file_count: int


class IntelligentIcebergManager:
    SMALL_FILE_THRESHOLD = 100

    def __init__(self, spark) -> None:
        self.spark = spark
        self.predictive = PredictiveMaintenanceEngine(spark)
        self.compactor = AutoCompactor(spark)

    def analyze_table(self, table_name: str) -> OptimizationRecommendation:
        prediction = self.predictive.predict_maintenance_needs(table_name)
        return OptimizationRecommendation(
            priority=prediction.priority,
            requires_compaction=prediction.requires_compaction,
            requires_expire_snapshots=True,
            file_count=prediction.file_count,
        )

    def optimize_table_performance(self, table_name: str, force: bool = False) -> OptimizationRecommendation:
        recommendation = self.analyze_table(table_name)
        if force or recommendation.priority == "HIGH":
            if recommendation.requires_compaction:
                self.compactor.compact_table(table_name)
            self.compactor.expire_snapshots(table_name)
        elif recommendation.priority == "MEDIUM":
            logger.info("Scheduling optimization for %s (priority=MEDIUM)", table_name)
        return recommendation
