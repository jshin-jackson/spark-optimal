"""HDFS to Ozone / Iceberg migration engine."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

from spark_optimal.governance.standards.spark_standards import ICEBERG_WRITE_DEFAULTS

logger = logging.getLogger(__name__)


@dataclass
class MigrationBatch:
    batch_id: int
    source_path: str
    target_table: str
    format: str = "parquet"


@dataclass
class BatchResult:
    batch_id: int
    row_count: int
    bytes_processed: int = 0
    duration_seconds: float = 0.0
    success: bool = True
    error: Optional[str] = None


@dataclass
class MigrationConfig:
    source_path: str
    target_table: str
    format: str = "parquet"
    mode: str = "overwrite"
    partition_cols: List[str] = field(default_factory=list)
    table_location: Optional[str] = None
    validate_row_count: bool = True


class MigrationProgressTracker:
    def __init__(self, total_batches: int, total_bytes: int = 0) -> None:
        self.metrics = {
            "total_batches": total_batches,
            "completed_batches": 0,
            "failed_batches": 0,
            "total_bytes": total_bytes,
            "processed_bytes": 0,
            "start_time": time.time(),
            "estimated_completion": None,
        }

    def update_progress(self, result: BatchResult) -> None:
        if result.success:
            self.metrics["completed_batches"] += 1
            self.metrics["processed_bytes"] += result.bytes_processed
        else:
            self.metrics["failed_batches"] += 1

        elapsed = max(time.time() - self.metrics["start_time"], 1)
        rate = self.metrics["processed_bytes"] / elapsed
        remaining = self.metrics["total_bytes"] - self.metrics["processed_bytes"]
        if rate > 0 and self.metrics["total_bytes"] > 0:
            self.metrics["estimated_completion"] = time.time() + (remaining / rate)

    def snapshot(self) -> dict:
        return dict(self.metrics)


class HDFSToOzoneMigrator:
    """Pattern 1: HDFS -> Spark -> Ozone (Iceberg)."""

    def __init__(self, spark, config: MigrationConfig) -> None:
        self.spark = spark
        self.config = config

    def _read_source(self):
        reader = self.spark.read.format(self.config.format)
        if self.config.format == "json":
            reader = reader.option("multiLine", "false")
        return reader.load(self.config.source_path)

    def _ensure_namespace(self) -> None:
        parts = self.config.target_table.split(".")
        if len(parts) >= 2:
            namespace = ".".join(parts[:-1])
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    def _write_target(self, df):
        self._ensure_namespace()
        writer = (
            df.writeTo(self.config.target_table)
            .tableProperty("write.format.default", ICEBERG_WRITE_DEFAULTS["write.format.default"])
            .tableProperty(
                "write.target-file-size-bytes",
                ICEBERG_WRITE_DEFAULTS["write.target-file-size-bytes"],
            )
        )
        if self.config.table_location:
            writer = writer.tableProperty("location", self.config.table_location)
        if self.config.partition_cols:
            writer = writer.partitionedBy(*self.config.partition_cols)

        if self.config.mode == "append":
            writer.append()
        elif self.config.mode == "overwrite":
            writer.createOrReplace()
        else:
            writer.create()

    def migrate_dataframe(self, df) -> dict:
        """Write a prepared DataFrame to the target Iceberg table."""
        start = time.time()
        row_count = df.count()
        logger.info("Migrating %s rows to %s", row_count, self.config.target_table)
        self._write_target(df)
        target_count = self.spark.table(self.config.target_table).count()
        if self.config.validate_row_count and row_count != target_count:
            raise RuntimeError(
                f"Row count mismatch: source={row_count}, target={target_count}"
            )
        duration = time.time() - start
        return {
            "source_path": self.config.source_path,
            "target_table": self.config.target_table,
            "source_count": row_count,
            "target_count": target_count,
            "duration_seconds": duration,
        }

    def migrate_with_validation(self) -> dict:
        start = time.time()
        source_df = self._read_source()
        source_count = source_df.count()
        logger.info("Migrating %s rows from %s", source_count, self.config.source_path)

        self._write_target(source_df)

        target_count = self.spark.table(self.config.target_table).count()
        if self.config.validate_row_count and source_count != target_count:
            raise RuntimeError(
                f"Row count mismatch: source={source_count}, target={target_count}"
            )

        duration = time.time() - start
        return {
            "source_path": self.config.source_path,
            "target_table": self.config.target_table,
            "source_count": source_count,
            "target_count": target_count,
            "duration_seconds": duration,
        }
