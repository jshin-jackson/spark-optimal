"""Ozone to Ozone ETL framework."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    quality_score: float = 1.0


@dataclass
class ETLConfig:
    source_table: str
    target_table: str
    sla_name: str = "critical_etl"
    validate_row_count: bool = True
    min_output_rows: int = 0


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df):
        pass


class DataQualityValidator:
    def validate_input(self, df, required_columns: Optional[List[str]] = None) -> ValidationResult:
        errors = []
        if required_columns:
            missing = [c for c in required_columns if c not in df.columns]
            if missing:
                errors.append(f"Missing columns: {missing}")
        return ValidationResult(is_valid=not errors, errors=errors)

    def validate_output(self, df, min_rows: int = 0) -> ValidationResult:
        count = df.count()
        if count < min_rows:
            return ValidationResult(
                is_valid=False,
                errors=[f"Output row count {count} below minimum {min_rows}"],
                quality_score=0.0,
            )
        return ValidationResult(is_valid=True, quality_score=1.0)


class OzoneETLPipeline:
    """Pattern 2: Ozone -> Spark -> Ozone."""

    def __init__(
        self,
        spark,
        config: ETLConfig,
        transformer: BaseTransformer,
        validator: Optional[DataQualityValidator] = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.transformer = transformer
        self.validator = validator or DataQualityValidator()

    def run(self) -> dict:
        start = time.time()
        source_df = self.spark.table(self.config.source_table)

        input_quality = self.validator.validate_input(source_df)
        if not input_quality.is_valid:
            raise DataQualityError(str(input_quality.errors))

        transformed = self.transformer.transform(source_df)
        output_quality = self.validator.validate_output(transformed, self.config.min_output_rows)
        if not output_quality.is_valid:
            raise DataQualityError(str(output_quality.errors))

        transformed.writeTo(self.config.target_table).createOrReplace()
        duration = time.time() - start

        result = {
            "source_table": self.config.source_table,
            "target_table": self.config.target_table,
            "output_count": transformed.count(),
            "duration_seconds": duration,
            "quality_score": output_quality.quality_score,
        }
        logger.info("ETL completed: %s", result)
        return result
