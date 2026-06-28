"""Throughput metrics collection."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class ThroughputSample:
    job_name: str
    rows_per_second: float
    bytes_per_second: float
    timestamp: float = field(default_factory=time.time)


class ThroughputTracker:
    def __init__(self, metrics_path: Path | None = None) -> None:
        self.metrics_path = metrics_path or Path("/tmp/spark-optimal-throughput.jsonl")
        self.samples: List[ThroughputSample] = []

    def record(self, payload: Dict[str, Any]) -> None:
        duration = max(payload.get("duration_seconds", 1), 0.001)
        rows = payload.get("output_count", payload.get("target_count", 0))
        sample = ThroughputSample(
            job_name=payload.get("job", "unknown"),
            rows_per_second=rows / duration,
            bytes_per_second=payload.get("bytes_processed", 0) / duration,
        )
        self.samples.append(sample)
        entry = {"timestamp": sample.timestamp, **payload, "rows_per_second": sample.rows_per_second}
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with self.metrics_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry) + "\n")

    def latest(self) -> ThroughputSample | None:
        return self.samples[-1] if self.samples else None
