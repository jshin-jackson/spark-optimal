"""Migration progress tracking."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    batch_id: int
    row_count: int
    bytes_processed: int = 0
    duration_seconds: float = 0.0
    success: bool = True
    error: Optional[str] = None


class MigrationProgressTracker:
    def __init__(self, total_batches: int, total_bytes: int = 0, sla_max_seconds: int | None = None) -> None:
        self.metrics = {
            "total_batches": total_batches,
            "completed_batches": 0,
            "failed_batches": 0,
            "total_bytes": total_bytes,
            "processed_bytes": 0,
            "start_time": time.time(),
            "estimated_completion": None,
        }
        self.sla_max_seconds = sla_max_seconds

    def update_progress(self, result: BatchResult) -> None:
        if result.success:
            self.metrics["completed_batches"] += 1
            self.metrics["processed_bytes"] += result.bytes_processed
        else:
            self.metrics["failed_batches"] += 1

        elapsed = max(time.time() - self.metrics["start_time"], 1)
        rate = self.metrics["processed_bytes"] / elapsed
        remaining = self.metrics["total_bytes"] - self.metrics["processed_bytes"]
        if rate > 0 and self.metrics["total_bytes"] > 0:
            self.metrics["estimated_completion"] = time.time() + (remaining / rate)

        if self._is_sla_at_risk():
            logger.warning("Migration SLA at risk: elapsed=%.0fs", elapsed)

    def _is_sla_at_risk(self) -> bool:
        if not self.sla_max_seconds:
            return False
        elapsed = time.time() - self.metrics["start_time"]
        return elapsed > self.sla_max_seconds * 0.8

    def snapshot(self) -> dict:
        return dict(self.metrics)

    def write_checkpoint(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.snapshot(), indent=2), encoding="utf-8")
