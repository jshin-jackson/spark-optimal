"""YARN queue allocation based on department policies."""

from __future__ import annotations

from dataclasses import dataclass

from spark_optimal.config import load_department_policies


@dataclass
class QueueAllocation:
    queue_name: str
    priority_boost: float
    sla_max_latency_minutes: int


class YARNQueueManager:
    def __init__(self) -> None:
        self.policies = load_department_policies()

    def allocate_queue(self, department: str, workload_queue: str, priority: str) -> QueueAllocation:
        policy = self.policies.get(department, self.policies.get("data_warehouse", {}))
        queue_name = policy.get("yarn_queue", workload_queue)
        if priority == "critical" and department == "fraud_detection":
            queue_name = policy.get("yarn_queue", "fraud")
        return QueueAllocation(
            queue_name=queue_name,
            priority_boost=float(policy.get("priority_boost", 1.0)),
            sla_max_latency_minutes=int(policy.get("sla_max_latency_minutes", 60)),
        )
