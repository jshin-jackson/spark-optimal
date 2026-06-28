"""Data quality monitoring helpers."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from spark_optimal.governance.quality.validation_rules import RuleResult, ValidationRule, ValidationRuleRegistry

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    table_name: str
    row_count: int
    quality_score: float
    rule_results: List[RuleResult] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.rule_results)


class QualityMonitor:
    def __init__(self, metrics_path: Path | None = None) -> None:
        self.registry = ValidationRuleRegistry()
        self.metrics_path = metrics_path or Path("/tmp/spark-optimal-quality.jsonl")

    def evaluate(self, df, table_name: str, rules: List[ValidationRule]) -> QualityReport:
        results = self.registry.apply_all(df, rules)
        passed_count = sum(1 for r in results if r.passed)
        score = passed_count / max(len(results), 1)
        report = QualityReport(
            table_name=table_name,
            row_count=df.count(),
            quality_score=score,
            rule_results=results,
        )
        self._persist(report)
        if not report.passed:
            logger.warning("Quality check failed for %s: score=%.2f", table_name, score)
        return report

    def _persist(self, report: QualityReport) -> None:
        payload: Dict[str, Any] = {
            "table_name": report.table_name,
            "row_count": report.row_count,
            "quality_score": report.quality_score,
            "passed": report.passed,
            "timestamp": report.timestamp,
            "rules": [{"name": r.rule_name, "passed": r.passed, "message": r.message} for r in report.rule_results],
        }
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with self.metrics_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")
