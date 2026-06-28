"""Spark job performance analysis and recommendations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from spark_optimal.optimization.performance_tuner import PerformanceTuner, TuningRecommendation


@dataclass
class PerformanceReport:
    workload_type: str
    data_size_gb: float
    recommendations: List[TuningRecommendation]

    @property
    def has_recommendations(self) -> bool:
        return bool(self.recommendations)


class PerformanceAnalyzer:
    def __init__(self) -> None:
        self.tuner = PerformanceTuner()

    def analyze(self, workload_type: str, data_size_gb: float, current_config: dict) -> PerformanceReport:
        recommendations = self.tuner.recommend(workload_type, data_size_gb, current_config)
        return PerformanceReport(workload_type, data_size_gb, recommendations)

    def format_report(self, report: PerformanceReport) -> str:
        lines = [f"Performance report: workload={report.workload_type} data={report.data_size_gb}GB"]
        for rec in report.recommendations:
            lines.append(f"  {rec.setting}: {rec.current} -> {rec.recommended} ({rec.reason})")
        if not report.recommendations:
            lines.append("  No tuning changes recommended.")
        return "\n".join(lines)
