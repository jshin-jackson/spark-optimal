"""Unit tests for resource optimization."""

from spark_optimal.optimization.resource_manager import EnterpriseResourceManager
from spark_optimal.optimization.workload_classifier import JobMetadata, WorkloadClassifier
from spark_optimal.optimization.yarn_queue_manager import YARNQueueManager


def test_classify_migration():
    classifier = WorkloadClassifier()
    assert classifier.classify_job(JobMetadata(pattern="migration")) == "migration"


def test_classify_etl_heavy():
    classifier = WorkloadClassifier()
    metadata = JobMetadata(pattern="etl", has_joins=True, data_size_gb=100)
    assert classifier.classify_job(metadata) == "etl_heavy"


def test_resource_plan_caps_executors():
    manager = EnterpriseResourceManager()
    plan = manager.calculate_optimal_resources("migration", data_size_gb=1000, priority_level="high")
    assert plan.estimated_executors <= 16
    assert "spark.executor.instances" in plan.spark_config


def test_dev_cluster_executor_cap():
    manager = EnterpriseResourceManager()
    plan = manager.calculate_optimal_resources("migration", data_size_gb=10, priority_level="high")
    assert plan.estimated_executors == 5
    assert plan.spark_config["spark.dynamicAllocation.maxExecutors"] == "16"


def test_yarn_queue_allocation():
    manager = YARNQueueManager()
    allocation = manager.allocate_queue("fraud_detection", "etl", "critical")
    assert allocation.queue_name == "fraud"
    assert allocation.priority_boost >= 1.0
