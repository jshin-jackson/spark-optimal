"""Iceberg table maintenance utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class MaintenanceRecommendation:
    table_name: str
    priority: str
    requires_compaction: bool = False
    requires_expire_snapshots: bool = False
    file_count: int = 0


class IcebergMaintenanceManager:
    SMALL_FILE_THRESHOLD = 100

    def __init__(self, spark) -> None:
        self.spark = spark

    def analyze_table(self, table_name: str) -> MaintenanceRecommendation:
        files_df = self.spark.sql(f"SELECT * FROM {table_name}.files")
        file_count = files_df.count()
        return MaintenanceRecommendation(
            table_name=table_name,
            priority="HIGH" if file_count >= self.SMALL_FILE_THRESHOLD else "LOW",
            requires_compaction=file_count >= self.SMALL_FILE_THRESHOLD,
            requires_expire_snapshots=True,
            file_count=file_count,
        )

    def compact_table(self, table_name: str, target_file_size_bytes: int = 268435456) -> None:
        logger.info("Running compaction on %s", table_name)
        self.spark.sql(
            f"""
            CALL spark_catalog.system.rewrite_data_files(
              table => '{table_name}',
              options => map('target-file-size-bytes', '{target_file_size_bytes}')
            )
            """
        )

    def expire_snapshots(self, table_name: str, retain_last: int = 30) -> None:
        logger.info("Expiring snapshots on %s", table_name)
        self.spark.sql(
            f"""
            CALL spark_catalog.system.expire_snapshots(
              table => '{table_name}',
              retain_last => {retain_last}
            )
            """
        )

    def run_hybrid_maintenance(
        self,
        table_name: str,
        force: bool = False,
        z_order_columns: Optional[List[str]] = None,
    ) -> MaintenanceRecommendation:
        recommendation = self.analyze_table(table_name)
        if force or recommendation.requires_compaction:
            if z_order_columns:
                cols = ", ".join(f"'{c}'" for c in z_order_columns)
                self.spark.sql(
                    f"""
                    CALL spark_catalog.system.rewrite_data_files(
                      table => '{table_name}',
                      strategy => 'sort',
                      sort_order => '{",".join(z_order_columns)}'
                    )
                    """
                )
            else:
                self.compact_table(table_name)
        if force or recommendation.requires_expire_snapshots:
            self.expire_snapshots(table_name)
        return recommendation
