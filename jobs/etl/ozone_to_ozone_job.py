#!/usr/bin/env python3
"""Entry point: Ozone -> Ozone ETL job."""

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
from spark_optimal.platform.etl.ozone_pipeline import BaseTransformer, ETLConfig, OzoneETLPipeline
from spark_optimal.platform.factory.session_builder import SparkSessionBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class IdentityTransformer(BaseTransformer):
    def transform(self, df):
        return df


class DedupTransformer(BaseTransformer):
    def __init__(self, key_columns: list[str]) -> None:
        self.key_columns = key_columns

    def transform(self, df):
        return df.dropDuplicates(self.key_columns)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ozone to Ozone ETL")
    parser.add_argument("--project", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--dedup-keys", default="")
    parser.add_argument("--data-size-gb", type=float, default=5.0)
    parser.add_argument("--priority", default="medium")
    parser.add_argument("--department", default="data_warehouse")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dedup_keys = [c.strip() for c in args.dedup_keys.split(",") if c.strip()]

    metadata = JobMetadata(
        pattern="etl",
        has_joins=False,
        data_size_gb=args.data_size_gb,
        department=args.department,
    )
    classifier = WorkloadClassifier()
    workload = classifier.classify_job(metadata)

    builder = SparkSessionBuilder(args.project, args.job)
    spark = builder.create_session(workload_type=workload, data_size_gb=args.data_size_gb, priority=args.priority)

    transformer = DedupTransformer(dedup_keys) if dedup_keys else IdentityTransformer()
    pipeline = OzoneETLPipeline(
        spark,
        ETLConfig(source_table=args.source_table, target_table=args.target_table),
        transformer,
    )

    sla = SLAMonitor().start_tracking("critical_etl")
    tracker = ThroughputTracker()

    try:
        result = pipeline.run()
        sla.record_completion(result["duration_seconds"], result["output_count"])
        tracker.record({"job": args.job, "pattern": "etl", **result})
        logger.info("ETL success: %s", result)
        return 0
    except Exception as exc:
        sla.record_failure(str(exc))
        logger.exception("ETL failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
