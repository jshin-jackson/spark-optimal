"""Parallel batch migration orchestration."""

from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, List

from spark_optimal.optimization.resource_manager import EnterpriseResourceManager
from spark_optimal.platform.migration.failure_recovery import FailureRecoveryEngine
from spark_optimal.platform.migration.progress_tracker import BatchResult, MigrationProgressTracker
from spark_optimal.platform.migration.validation_engine import MigrationValidationEngine

logger = logging.getLogger(__name__)
GB = 1024**3


@dataclass
class MigrationPlan:
    source_path: str
    target_table: str
    total_size_bytes: int
    batch_count: int
    recommended_parallelism: int


@dataclass
class ParallelMigrationConfig:
    source_path: str
    target_table: str
    total_size_gb: float
    batch_size_gb: float = 2.0
    parallelism: int = 4
    priority: str = "high"


class EnterpriseParallelMigrator:
    def __init__(self, migrate_batch_fn: Callable[[int], BatchResult]) -> None:
        self.migrate_batch_fn = migrate_batch_fn
        self.resource_manager = EnterpriseResourceManager()
        self.failure_recovery = FailureRecoveryEngine()
        self.validator = MigrationValidationEngine()

    def create_plan(self, config: ParallelMigrationConfig) -> MigrationPlan:
        total_bytes = int(config.total_size_gb * GB)
        batch_count = max(1, math.ceil(config.total_size_gb / config.batch_size_gb))
        return MigrationPlan(
            source_path=config.source_path,
            target_table=config.target_table,
            total_size_bytes=total_bytes,
            batch_count=batch_count,
            recommended_parallelism=min(config.parallelism, batch_count),
        )

    def migrate_large_dataset(self, config: ParallelMigrationConfig) -> dict:
        plan = self.create_plan(config)
        tracker = MigrationProgressTracker(plan.batch_count, plan.total_size_bytes)
        resource_plan = self.resource_manager.calculate_optimal_resources(
            "migration", config.total_size_gb, config.priority
        )

        completed: List[BatchResult] = []
        failed_ids: List[int] = []

        logger.info(
            "Starting parallel migration: batches=%s parallelism=%s executors=%s",
            plan.batch_count,
            plan.recommended_parallelism,
            resource_plan.estimated_executors,
        )

        with ThreadPoolExecutor(max_workers=plan.recommended_parallelism) as executor:
            futures = {executor.submit(self.migrate_batch_fn, i): i for i in range(plan.batch_count)}
            for future in as_completed(futures):
                batch_id = futures[future]
                try:
                    result = future.result()
                    completed.append(result)
                    tracker.update_progress(result)
                except Exception as exc:
                    self.failure_recovery.handle_failure(batch_id, exc)
                    failed_ids.append(batch_id)
                    tracker.update_progress(BatchResult(batch_id=batch_id, row_count=0, success=False, error=str(exc)))

        if failed_ids:
            retried = self.failure_recovery.retry_failed_batches(failed_ids, self.migrate_batch_fn)
            for result in retried:
                tracker.update_progress(result)

        total_rows = sum(r.row_count for r in completed if r.success)
        return {
            "source_path": config.source_path,
            "target_table": config.target_table,
            "batch_count": plan.batch_count,
            "total_rows": total_rows,
            "failed_batches": tracker.metrics["failed_batches"],
            "progress": tracker.snapshot(),
            "resource_plan": resource_plan.spark_config,
        }
