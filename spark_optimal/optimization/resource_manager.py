"""Dynamic resource allocation for SBI YARN cluster."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Dict

from spark_optimal.config import load_cluster_config, load_resource_limits, load_resource_profiles


@dataclass
class ResourcePlan:
    workload_type: str
    spark_config: Dict[str, str]
    yarn_queue: str
    priority: str
    estimated_executors: int


def _parse_memory_gb(value: str) -> float:
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([gkmGKM])?b?$", value.strip())
    if not match:
        return 4.0
    amount = float(match.group(1))
    unit = (match.group(2) or "g").lower()
    if unit == "m":
        return amount / 1024
    if unit == "k":
        return amount / (1024 * 1024)
    return amount


class EnterpriseResourceManager:
    """Calculate Spark executor counts capped by environment cluster capacity."""

    def __init__(self) -> None:
        self.profiles = load_resource_profiles()
        self.cluster = load_cluster_config()
        self.limits = load_resource_limits()

    def _cluster_executor_cap(self, spark_config: Dict[str, str], profile_max: int) -> int:
        reserved = float(self.cluster.get("yarn_reserved_ratio", 0.15))
        total_vcores = int(self.cluster["total_vcores"])
        total_memory_gb = int(self.cluster.get("total_memory_gb") or self.cluster.get("total_memory_tb", 0) * 1024)

        cores = int(spark_config.get("spark.executor.cores", self.limits.get("max_executor_cores", 1)))
        mem_gb = _parse_memory_gb(spark_config.get("spark.executor.memory", self.limits.get("executor_memory", "4g")))
        overhead_gb = _parse_memory_gb(
            spark_config.get("spark.executor.memoryOverhead", self.limits.get("executor_memory_overhead", "1g"))
        )

        usable_vcores = max(1, int(total_vcores * (1 - reserved)))
        usable_memory_gb = max(1, int(total_memory_gb * (1 - reserved)))
        by_cores = max(1, usable_vcores // cores)
        by_memory = max(1, usable_memory_gb // max(1, int(mem_gb + overhead_gb)))

        abs_max = int(self.limits.get("max_executors_absolute", profile_max))
        return min(by_cores, by_memory, abs_max, profile_max)

    def calculate_optimal_resources(
        self,
        workload_type: str,
        data_size_gb: float,
        priority_level: str = "medium",
    ) -> ResourcePlan:
        profile = self.profiles.get(workload_type) or self.profiles["etl_standard"]
        target_per_executor = float(profile.get("target_data_per_executor_gb", 1))
        profile_max = int(profile.get("max_executors", 50))

        spark_config = dict(profile.get("spark_config", {}))
        cluster_cap = self._cluster_executor_cap(spark_config, profile_max)

        optimal_executors = max(1, min(math.ceil(data_size_gb / target_per_executor), cluster_cap))
        spark_config["spark.executor.instances"] = str(optimal_executors)
        spark_config["spark.dynamicAllocation.maxExecutors"] = str(cluster_cap)
        spark_config.setdefault("spark.dynamicAllocation.minExecutors", "1")

        if priority_level == "critical":
            spark_config["spark.dynamicAllocation.minExecutors"] = str(max(1, optimal_executors // 2))

        return ResourcePlan(
            workload_type=workload_type,
            spark_config=spark_config,
            yarn_queue=str(profile.get("yarn_queue", "default")),
            priority=str(profile.get("priority", priority_level)),
            estimated_executors=optimal_executors,
        )
