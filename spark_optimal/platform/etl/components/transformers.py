"""Enterprise transformer base with governance hooks."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from spark_optimal.governance.quality.validation_rules import ValidationRule
from spark_optimal.monitoring.quality_dashboard import QualityMetricsCollector
from spark_optimal.monitoring.sla_monitor import SLAMonitor
from spark_optimal.platform.etl.ozone_pipeline import DataQualityError, DataQualityValidator


@dataclass
class TransformationConfig:
    sla_name: str = "critical_etl"
    required_columns: List[str] = field(default_factory=list)
    quality_rules: List[ValidationRule] = field(default_factory=list)
    min_output_rows: int = 0


class EnterpriseTransformer(ABC):
    def __init__(
        self,
        sla_monitor: Optional[SLAMonitor] = None,
        quality_validator: Optional[DataQualityValidator] = None,
    ) -> None:
        self.sla_monitor = sla_monitor or SLAMonitor()
        self.quality_validator = quality_validator or DataQualityValidator()
        self.metrics = QualityMetricsCollector()

    @abstractmethod
    def transform(self, df):
        pass

    def execute_with_governance(self, df, config: TransformationConfig):
        sla_tracker = self.sla_monitor.start_tracking(config.sla_name)
        input_quality = self.quality_validator.validate_input(df, config.required_columns or None)
        if not input_quality.is_valid:
            raise DataQualityError(str(input_quality.errors))

        start = time.time()
        try:
            result = self.transform(df)
            output_quality = self.quality_validator.validate_output(result, config.min_output_rows)
            if not output_quality.is_valid:
                raise DataQualityError(str(output_quality.errors))

            duration = time.time() - start
            output_count = result.count()
            sla_tracker.record_completion(duration, output_count)
            self.metrics.record(config.sla_name, output_quality.quality_score, output_count)
            return result
        except Exception as exc:
            sla_tracker.record_failure(str(exc))
            raise
