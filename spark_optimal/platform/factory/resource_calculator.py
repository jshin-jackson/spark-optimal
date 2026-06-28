"""Executor and memory calculator for SBI cluster."""

from __future__ import annotations

import math
from dataclasses import dataclass

from spark_optimal.config import load_cluster_config, load_resource_limits, load_resource_profiles


@dataclass
class ResourceEstimate:
    executors: int
    cores_per_executor: int
    memory_gib: int
    memory_overhead_gib: int
    estimated_parallelism: int


class ResourceCalculator:
    OVERHEAD_RATIO = 0.25

    def __init__(self) -> None:
        self.cluster = load_cluster_config()
        self.limits = load_resource_limits()
        profiles = load_resource_profiles()
        migration = profiles.get("migration", {})
        spark_cfg = migration.get("spark_config", {})
        self.default_cores = int(spark_cfg.get("spark.executor.cores", self.limits.get("max_executor_cores", 4)))
        mem = spark_cfg.get("spark.executor.memory", self.limits.get("executor_memory", "8g"))
        self.default_memory_gib = int(str(mem).lower().replace("g", ""))

    def _max_executors(self) -> int:
        limits_max = int(self.limits.get("max_executors_absolute", 200))
        profiles = load_resource_profiles()
        profile_max = int(profiles.get("migration", {}).get("max_executors", limits_max))
        reserved = float(self.cluster.get("yarn_reserved_ratio", 0.15))
        usable_vcores = int(int(self.cluster["total_vcores"]) * (1 - reserved))
        usable_memory_gb = int(
            int(self.cluster.get("total_memory_gb") or self.cluster.get("total_memory_tb", 0) * 1024) * (1 - reserved)
        )
        slot_gb = self.default_memory_gib + max(1, int(self.default_memory_gib * self.OVERHEAD_RATIO))
        by_cores = max(1, usable_vcores // self.default_cores)
        by_memory = max(1, usable_memory_gb // slot_gb)
        return min(by_cores, by_memory, limits_max, profile_max)

    def estimate(self, data_size_gb: float, target_gb_per_executor: float = 2.0, max_executors: int | None = None) -> ResourceEstimate:
        cap = max_executors if max_executors is not None else self._max_executors()
        executors = max(1, min(math.ceil(data_size_gb / target_gb_per_executor), cap))
        memory_gib = self.default_memory_gib
        overhead = max(1, int(memory_gib * self.OVERHEAD_RATIO))
        return ResourceEstimate(
            executors=executors,
            cores_per_executor=self.default_cores,
            memory_gib=memory_gib,
            memory_overhead_gib=overhead,
            estimated_parallelism=executors * self.default_cores,
        )

    def cluster_utilization(self, estimate: ResourceEstimate) -> float:
        used_cores = estimate.executors * estimate.cores_per_executor
        return used_cores / int(self.cluster["total_vcores"])
