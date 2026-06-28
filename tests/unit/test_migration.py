"""Migration engine unit tests."""

from spark_optimal.platform.migration.failure_recovery import FailureRecoveryEngine
from spark_optimal.platform.migration.parallel_migrator import EnterpriseParallelMigrator, ParallelMigrationConfig
from spark_optimal.platform.migration.progress_tracker import BatchResult, MigrationProgressTracker
from spark_optimal.platform.migration.validation_engine import MigrationValidationEngine


def test_progress_tracker_sla_risk():
    tracker = MigrationProgressTracker(10, total_bytes=1000, sla_max_seconds=1)
    tracker.metrics["start_time"] -= 1
    assert tracker._is_sla_at_risk()


def test_validation_engine_row_counts():
    report = MigrationValidationEngine().validate_row_counts(100, 100)
    assert report.passed


def test_parallel_migrator_plan():
    def fake_migrate(batch_id: int) -> BatchResult:
        return BatchResult(batch_id=batch_id, row_count=1000, bytes_processed=1024**3)

    migrator = EnterpriseParallelMigrator(fake_migrate)
    plan = migrator.create_plan(
        ParallelMigrationConfig(
            source_path="hdfs://ns1/raw",
            target_table="spark_catalog.sbi.brnz",
            total_size_gb=4.0,
            batch_size_gb=2.0,
        )
    )
    assert plan.batch_count == 2


def test_failure_recovery_retry():
    engine = FailureRecoveryEngine()
    calls = {"n": 0}

    def flaky(batch_id: int) -> BatchResult:
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("transient")
        return BatchResult(batch_id=batch_id, row_count=1)

    results = engine.retry_failed_batches([0], flaky)
    assert results[0].success
