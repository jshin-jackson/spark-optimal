"""SLA definitions for Spark workloads."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SLAProfile:
    name: str
    max_execution_seconds: int
    min_success_rate: float
    alert_threshold: float = 0.8


SLA_DEFINITIONS = {
    "critical_etl": SLAProfile(
        name="critical_etl",
        max_execution_seconds=30 * 60,
        min_success_rate=0.995,
        alert_threshold=0.8,
    ),
    "batch_migration": SLAProfile(
        name="batch_migration",
        max_execution_seconds=4 * 60 * 60,
        min_success_rate=0.99,
        alert_threshold=0.8,
    ),
    "analytical_workload": SLAProfile(
        name="analytical_workload",
        max_execution_seconds=2 * 60 * 60,
        min_success_rate=0.98,
        alert_threshold=0.8,
    ),
}
