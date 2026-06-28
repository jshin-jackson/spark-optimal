"""Runtime performance tuning recommendations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from spark_optimal.optimization.resource_manager import EnterpriseResourceManager, ResourcePlan


@dataclass
class TuningRecommendation:
    setting: str
    current: str
    recommended: str
    reason: str


class PerformanceTuner:
    def __init__(self) -> None:
        self.resource_manager = EnterpriseResourceManager()

    def recommend(self, workload_type: str, data_size_gb: float, current_config: Dict[str, str]) -> List[TuningRecommendation]:
        plan = self.resource_manager.calculate_optimal_resources(workload_type, data_size_gb)
        recommendations: List[TuningRecommendation] = []

        for key, recommended in plan.spark_config.items():
            current = current_config.get(key, "unset")
            if str(current) != str(recommended):
                recommendations.append(
                    TuningRecommendation(
                        setting=key,
                        current=str(current),
                        recommended=str(recommended),
                        reason=f"workload={workload_type}, data={data_size_gb}GB",
                    )
                )

        shuffle = current_config.get("spark.sql.shuffle.partitions", "200")
        if data_size_gb >= 100 and int(shuffle) < 400:
            recommendations.append(
                TuningRecommendation(
                    setting="spark.sql.shuffle.partitions",
                    current=str(shuffle),
                    recommended="400",
                    reason="large shuffle workload",
                )
            )
        return recommendations

    def apply_plan(self, workload_type: str, data_size_gb: float, priority: str = "medium") -> ResourcePlan:
        return self.resource_manager.calculate_optimal_resources(workload_type, data_size_gb, priority)
