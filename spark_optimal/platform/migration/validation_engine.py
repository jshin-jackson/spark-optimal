"""Post-migration validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    source_count: int
    target_count: int
    checksum_match: bool
    passed: bool
    message: str = ""


class MigrationValidationEngine:
    def validate_row_counts(self, source_count: int, target_count: int) -> ValidationReport:
        passed = source_count == target_count
        return ValidationReport(
            source_count=source_count,
            target_count=target_count,
            checksum_match=passed,
            passed=passed,
            message="row counts match" if passed else f"mismatch source={source_count} target={target_count}",
        )

    def validate_table_readable(self, spark, table_name: str) -> bool:
        try:
            count = spark.table(table_name).count()
            logger.info("Table %s readable, rows=%s", table_name, count)
            return True
        except Exception as exc:
            logger.error("Table %s not readable: %s", table_name, exc)
            return False
