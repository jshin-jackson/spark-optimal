"""Rule-based predictive maintenance for Iceberg tables."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MaintenancePrediction:
    table_name: str
    priority: str
    requires_compaction: bool
    file_count: int
    urgency_score: float


class PredictiveMaintenanceEngine:
    HIGH_FILE_THRESHOLD = 100
    MEDIUM_FILE_THRESHOLD = 50

    def __init__(self, spark) -> None:
        self.spark = spark

    def predict_maintenance_needs(self, table_name: str) -> MaintenancePrediction:
        files_df = self.spark.sql(f"SELECT * FROM {table_name}.files")
        file_count = files_df.count()
        if file_count >= self.HIGH_FILE_THRESHOLD:
            priority, urgency = "HIGH", 0.9
        elif file_count >= self.MEDIUM_FILE_THRESHOLD:
            priority, urgency = "MEDIUM", 0.6
        else:
            priority, urgency = "LOW", 0.2
        return MaintenancePrediction(
            table_name=table_name,
            priority=priority,
            requires_compaction=file_count >= self.MEDIUM_FILE_THRESHOLD,
            file_count=file_count,
            urgency_score=urgency,
        )
