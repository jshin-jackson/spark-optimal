"""Hybrid scheduled + threshold-based Iceberg cleanup."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from spark_optimal.platform.maintenance.iceberg_optimizer import IntelligentIcebergManager

logger = logging.getLogger(__name__)


@dataclass
class MaintenanceSchedule:
    tables: List[str]
    force: bool = False


class CleanupScheduler:
    def __init__(self, spark) -> None:
        self.manager = IntelligentIcebergManager(spark)

    def run_scheduled_maintenance(self, schedule: MaintenanceSchedule) -> List[dict]:
        results = []
        for table in schedule.tables:
            recommendation = self.manager.optimize_table_performance(table, force=schedule.force)
            results.append(
                {
                    "table": table,
                    "priority": recommendation.priority,
                    "file_count": recommendation.file_count,
                    "compaction": recommendation.requires_compaction,
                }
            )
        return results
