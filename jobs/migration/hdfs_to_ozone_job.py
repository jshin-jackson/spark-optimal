#!/usr/bin/env python3
"""Entry point: HDFS -> Ozone (Iceberg) migration job."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_optimal.monitoring.sla_monitor import SLAMonitor, ThroughputTracker
from spark_optimal.optimization.workload_classifier import JobMetadata, WorkloadClassifier
from spark_optimal.platform.factory.session_builder import SparkSessionBuilder
from spark_optimal.platform.migration.hdfs_to_ozone import HDFSToOzoneMigrator, MigrationConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HDFS to Ozone migration (Iceberg)")
    parser.add_argument("--project", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "create"])
    parser.add_argument("--data-size-gb", type=float, default=10.0)
    parser.add_argument("--priority", default="high")
    parser.add_argument("--partition-cols", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    partition_cols = [c.strip() for c in args.partition_cols.split(",") if c.strip()]

    classifier = WorkloadClassifier()
    workload = classifier.classify_job(
        JobMetadata(pattern="migration", source_path=args.source_path, data_size_gb=args.data_size_gb)
    )

    builder = SparkSessionBuilder(args.project, args.job)
    spark = builder.create_session(workload_type=workload, data_size_gb=args.data_size_gb, priority=args.priority)

    sla = SLAMonitor().start_tracking("batch_migration")
    tracker = ThroughputTracker()

    try:
        result = HDFSToOzoneMigrator(
            spark,
            MigrationConfig(
                source_path=args.source_path,
                target_table=args.target_table,
                format=args.format,
                mode=args.mode,
                partition_cols=partition_cols,
            ),
        ).migrate_with_validation()

        sla.record_completion(result["duration_seconds"], result["target_count"])
        tracker.record({"job": args.job, "pattern": "migration", **result})
        logger.info("Migration success: %s", result)
        return 0
    except Exception as exc:
        sla.record_failure(str(exc))
        logger.exception("Migration failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
