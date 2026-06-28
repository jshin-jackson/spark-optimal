"""Load test configuration validation."""

from spark_optimal.optimization.resource_manager import EnterpriseResourceManager


def test_large_migration_resource_plan():
    mgr = EnterpriseResourceManager()
    plan = mgr.calculate_optimal_resources("migration", data_size_gb=10000, priority_level="high")
    assert plan.estimated_executors <= 16
    assert plan.yarn_queue == "default"
