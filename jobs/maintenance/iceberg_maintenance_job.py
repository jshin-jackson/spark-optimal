#!/usr/bin/env python3
"""Entry point: Iceberg table maintenance."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_optimal.platform.factory.session_builder import SparkSessionBuilder
from spark_optimal.platform.maintenance.iceberg_maintenance import IcebergMaintenanceManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg maintenance")
    parser.add_argument("--project", default="spark_optimal")
    parser.add_argument("--job", default="iceberg_maintenance")
    parser.add_argument("--table", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--z-order", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    z_order = [c.strip() for c in args.z_order.split(",") if c.strip()]

    spark = SparkSessionBuilder(args.project, args.job).create_session(
        workload_type="etl_standard",
        data_size_gb=1.0,
    )
    manager = IcebergMaintenanceManager(spark)

    try:
        recommendation = manager.run_hybrid_maintenance(
            args.table,
            force=args.force,
            z_order_columns=z_order or None,
        )
        logger.info("Maintenance completed: %s", recommendation)
        return 0
    except Exception:
        logger.exception("Maintenance failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
