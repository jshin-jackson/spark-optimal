"""Business-level SLA monitoring."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from spark_optimal.governance.quality.sla_definitions import SLA_DEFINITIONS, SLAProfile
from spark_optimal.monitoring.throughput_tracker import ThroughputTracker

logger = logging.getLogger(__name__)


@dataclass
class SLATracker:
    profile: SLAProfile
    started_at: float = field(default_factory=time.time)
    completed: bool = False
    failed: bool = False
    row_count: int = 0

    def record_completion(self, duration_seconds: float, row_count: int) -> None:
        self.completed = True
        self.row_count = row_count
        if duration_seconds > self.profile.max_execution_seconds:
            logger.warning(
                "SLA breach for %s: %.1fs > %ss",
                self.profile.name,
                duration_seconds,
                self.profile.max_execution_seconds,
            )

    def record_failure(self, error: str) -> None:
        self.failed = True
        logger.error("Job failed under SLA %s: %s", self.profile.name, error)

    def is_compliant(self, duration_seconds: float) -> bool:
        return (
            not self.failed
            and duration_seconds <= self.profile.max_execution_seconds
        )


class SLAMonitor:
    def start_tracking(self, sla_name: str) -> SLATracker:
        profile = SLA_DEFINITIONS[sla_name]
        return SLATracker(profile=profile)


__all__ = ["SLAMonitor", "SLATracker", "ThroughputTracker"]
