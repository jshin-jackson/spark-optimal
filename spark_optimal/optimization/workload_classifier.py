"""Automatic workload classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

WorkloadType = Literal["migration", "etl_heavy", "etl_standard", "analytical"]


@dataclass
class JobMetadata:
    pattern: str
    source_path: str = ""
    target_path: str = ""
    has_joins: bool = False
    has_aggregations: bool = False
    data_size_gb: float = 1.0
    department: str = "data_warehouse"


class WorkloadClassifier:
    def classify_job(self, metadata: JobMetadata) -> WorkloadType:
        if metadata.pattern == "migration":
            return "migration"
        if metadata.has_joins and metadata.has_aggregations:
            return "analytical"
        if metadata.has_joins or metadata.data_size_gb >= 500:
            return "etl_heavy"
        return "etl_standard"
