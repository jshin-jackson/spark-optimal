"""Iceberg compaction and snapshot expiry."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class AutoCompactor:
    def __init__(self, spark, target_file_size_bytes: int = 268435456) -> None:
        self.spark = spark
        self.target_file_size_bytes = target_file_size_bytes

    def compact_table(self, table_name: str) -> None:
        logger.info("Compacting %s", table_name)
        self.spark.sql(
            f"""
            CALL spark_catalog.system.rewrite_data_files(
              table => '{table_name}',
              options => map('target-file-size-bytes', '{self.target_file_size_bytes}')
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

    def remove_orphan_files(self, table_name: str) -> None:
        logger.info("Removing orphan files for %s", table_name)
        self.spark.sql(
            f"CALL spark_catalog.system.remove_orphan_files(table => '{table_name}')"
        )
