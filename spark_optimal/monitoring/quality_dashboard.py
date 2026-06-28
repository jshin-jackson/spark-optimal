"""Data quality metrics aggregation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class QualityMetric:
    sla_name: str
    quality_score: float
    row_count: int


class QualityMetricsCollector:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or Path("/tmp/spark-optimal-quality-metrics.jsonl")
        self.metrics: List[QualityMetric] = []

    def record(self, sla_name: str, quality_score: float, row_count: int) -> None:
        metric = QualityMetric(sla_name=sla_name, quality_score=quality_score, row_count=row_count)
        self.metrics.append(metric)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"sla_name": sla_name, "quality_score": quality_score, "row_count": row_count}) + "\n")

    def average_score(self) -> float:
        if not self.metrics:
            return 1.0
        return sum(m.quality_score for m in self.metrics) / len(self.metrics)


class QualityDashboard:
    def __init__(self, collector: QualityMetricsCollector | None = None) -> None:
        self.collector = collector or QualityMetricsCollector()

    def summary(self) -> dict:
        return {
            "checks": len(self.collector.metrics),
            "average_quality_score": self.collector.average_score(),
            "latest": [
                {"sla": m.sla_name, "score": m.quality_score, "rows": m.row_count}
                for m in self.collector.metrics[-10:]
            ],
        }
