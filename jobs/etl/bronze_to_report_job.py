#!/usr/bin/env python3
"""
Step 4: Bronze → Silver → Gold 리포트 ETL

[Medallion Silver Layer]
  - Bronze 데이터 정제: 타입 캐스팅, NULL 처리, SUCCESS 거래만 필터
  - 테이블: spark_catalog.sbi_financial.slvr_transactions

[Medallion Gold Layer]
  - 일별·채널별·카테고리별 집계 리포트
  - 테이블: spark_catalog.sbi_financial.gld_daily_report

[실행]
  bash scripts/submit/spark_submit.sh etl \
    --py-file jobs/etl/bronze_to_report_job.py \
    --project sbi_financial --job bronze_to_report \
    --data-size-gb 10
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_optimal.governance.standards.spark_standards import ICEBERG_WRITE_DEFAULTS
from spark_optimal.monitoring.sla_monitor import SLAMonitor, ThroughputTracker
from spark_optimal.optimization.workload_classifier import JobMetadata, WorkloadClassifier
from spark_optimal.platform.etl.financial_transformers import (
    BronzeToSilverTransformer,
    SilverToGoldReportTransformer,
)
from spark_optimal.platform.etl.ozone_pipeline import DataQualityValidator
from spark_optimal.platform.factory.session_builder import SparkSessionBuilder
from spark_optimal.platform.medallion.financial_config import load_financial_medallion_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    cfg = load_financial_medallion_config()
    parser = argparse.ArgumentParser(description="Bronze -> Silver -> Gold financial ETL")
    parser.add_argument("--project", default="sbi_financial")
    parser.add_argument("--job", default="bronze_to_report")
    parser.add_argument("--bronze-table", default=cfg["tables"]["brnz"]["name"])
    parser.add_argument("--silver-table", default=cfg["tables"]["slvr"]["name"])
    parser.add_argument("--gold-table", default=cfg["tables"]["gld"]["name"])
    parser.add_argument("--silver-location", default=cfg["tables"]["slvr"]["location"])
    parser.add_argument("--gold-location", default=cfg["tables"]["gld"]["location"])
    parser.add_argument("--data-size-gb", type=float, default=float(cfg["sdv"]["target_gb"]))
    parser.add_argument("--priority", default="medium")
    return parser.parse_args()


def _write_iceberg(spark, df, table_name: str, location: str, partition_cols: list[str]) -> int:
    """
    DataFrame을 Iceberg 테이블로 저장 (createOrReplace).

    Args:
        table_name: spark_catalog.db.table 형식
        location: Ozone 물리 경로 (ofs://...)
        partition_cols: 파티션 컬럼 목록

    Returns:
        저장된 행 수
    """
    namespace = ".".join(table_name.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    writer = (
        df.writeTo(table_name)
        .tableProperty("write.format.default", ICEBERG_WRITE_DEFAULTS["write.format.default"])
        .tableProperty(
            "write.target-file-size-bytes",
            ICEBERG_WRITE_DEFAULTS["write.target-file-size-bytes"],
        )
        .tableProperty("location", location)
        .partitionedBy(*partition_cols)
    )
    writer.createOrReplace()
    return spark.table(table_name).count()


def main() -> int:
    args = parse_args()
    cfg = load_financial_medallion_config()
    validator = DataQualityValidator()

    classifier = WorkloadClassifier()
    workload = classifier.classify_job(
        JobMetadata(pattern="etl", has_joins=True, data_size_gb=args.data_size_gb)
    )

    spark = SparkSessionBuilder(args.project, args.job).create_session(
        workload_type=workload,
        data_size_gb=args.data_size_gb,
        priority=args.priority,
    )

    sla = SLAMonitor().start_tracking("critical_etl")
    tracker = ThroughputTracker()
    start = time.time()

    try:
        # ── Bronze 읽기 + 품질 검증 ──
        bronze_df = spark.table(args.bronze_table)
        input_quality = validator.validate_input(bronze_df, ["transaction_id", "amount"])
        if not input_quality.is_valid:
            raise RuntimeError(str(input_quality.errors))

        # ── Silver: 정제·필터 ──
        silver_df = BronzeToSilverTransformer().transform(bronze_df)
        silver_count = _write_iceberg(
            spark,
            silver_df,
            args.silver_table,
            args.silver_location,
            cfg["tables"]["slvr"]["partition_cols"],
        )
        logger.info("Silver layer written: %s rows -> %s", silver_count, args.silver_table)

        # ── Gold: 일별 집계 리포트 ──
        gold_df = SilverToGoldReportTransformer().transform(spark.table(args.silver_table))
        gold_count = _write_iceberg(
            spark,
            gold_df,
            args.gold_table,
            args.gold_location,
            cfg["tables"]["gld"]["partition_cols"],
        )
        logger.info("Gold report written: %s rows -> %s", gold_count, args.gold_table)

        duration = time.time() - start
        result = {
            "bronze_table": args.bronze_table,
            "silver_table": args.silver_table,
            "gold_table": args.gold_table,
            "silver_count": silver_count,
            "gold_count": gold_count,
            "duration_seconds": duration,
        }
        sla.record_completion(duration, gold_count)
        tracker.record({"job": args.job, "layers": ["slvr", "gld"], **result})
        logger.info("Report ETL success: %s", result)
        return 0
    except Exception as exc:
        sla.record_failure(str(exc))
        logger.exception("Report ETL failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
