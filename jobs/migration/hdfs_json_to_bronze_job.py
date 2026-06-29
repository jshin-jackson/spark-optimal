#!/usr/bin/env python3
"""
Step 3: HDFS Raw JSON → Ozone Bronze Iceberg 테이블

[Medallion Bronze Layer]
  - 원본 JSON을 최대한 그대로 저장 (스키마 + 메타데이터 추가)
  - Iceberg 테이블: spark_catalog.sbi_financial.brnz_transactions
  - Ozone 경로: ofs://ozone1782570080/prod/data/brnz/transactions

[처리 내용]
  1. HDFS에서 JSONL 읽기
  2. ingest_ts, source_path, transaction_date 컬럼 추가
  3. transaction_date 기준 파티션하여 Iceberg 테이블에 저장

[실행]
  bash scripts/submit/spark_submit.sh migration \
    --py-file jobs/migration/hdfs_json_to_bronze_job.py \
    --project sbi_financial --job hdfs_json_to_bronze \
    --source-path hdfs://ns1/prod/data/brnz/transactions \
    --data-size-gb 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# spark_optimal 패키지 import를 위해 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F

from spark_optimal.monitoring.sla_monitor import SLAMonitor, ThroughputTracker
from spark_optimal.optimization.workload_classifier import JobMetadata, WorkloadClassifier
from spark_optimal.platform.factory.session_builder import SparkSessionBuilder
from spark_optimal.platform.medallion.financial_config import load_financial_medallion_config
from spark_optimal.platform.migration.hdfs_to_ozone import HDFSToOzoneMigrator, MigrationConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """커맨드라인 인자 — spark_submit.sh 가 전달하는 --project, --source-path 등."""
    cfg = load_financial_medallion_config()
    parser = argparse.ArgumentParser(description="HDFS JSON -> Bronze Iceberg")
    parser.add_argument("--project", default="sbi_financial")
    parser.add_argument("--job", default="hdfs_json_to_bronze")
    parser.add_argument("--source-path", default=cfg["hdfs_raw_path"])
    parser.add_argument("--target-table", default=cfg["tables"]["brnz"]["name"])
    parser.add_argument("--table-location", default=cfg["tables"]["brnz"]["location"])
    parser.add_argument("--data-size-gb", type=float, default=float(cfg["sdv"]["target_gb"]))
    parser.add_argument("--priority", default="high")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "create"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_financial_medallion_config()
    partition_cols = cfg["tables"]["brnz"]["partition_cols"]  # 예: ["transaction_date"]

    # 데이터 크기·패턴에 따라 workload 유형 결정 → executor 수 자동 계산
    classifier = WorkloadClassifier()
    workload = classifier.classify_job(
        JobMetadata(pattern="migration", source_path=args.source_path, data_size_gb=args.data_size_gb)
    )

    # Kerberos + Delegation Token + Iceberg 설정이 포함된 SparkSession 생성
    spark = SparkSessionBuilder(args.project, args.job).create_session(
        workload_type=workload,
        data_size_gb=args.data_size_gb,
        priority=args.priority,
    )

    sla = SLAMonitor().start_tracking("batch_migration")
    tracker = ThroughputTracker()

    try:
        migrator = HDFSToOzoneMigrator(
            spark,
            MigrationConfig(
                source_path=args.source_path,
                target_table=args.target_table,
                format="json",
                mode=args.mode,
                partition_cols=partition_cols,
                table_location=args.table_location,
            ),
        )

        # HDFS JSON 읽기
        source_df = migrator._read_source()

        # Bronze 메타데이터 컬럼 추가
        enriched = (
            source_df.withColumn("ingest_ts", F.current_timestamp())       # 적재 시각
            .withColumn("source_path", F.lit(args.source_path))             # 원본 HDFS 경로
            .withColumn("transaction_date", F.to_date(F.to_timestamp("transaction_ts")))  # 파티션 키
        )

        # Iceberg 테이블에 쓰기 (Ozone location)
        result = migrator.migrate_dataframe(enriched)
        result["table_location"] = args.table_location
        sla.record_completion(result["duration_seconds"], result["target_count"])
        tracker.record({"job": args.job, "layer": "brnz", **result})
        logger.info("Bronze ingest success: %s", result)
        return 0
    except Exception as exc:
        sla.record_failure(str(exc))
        logger.exception("Bronze ingest failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
