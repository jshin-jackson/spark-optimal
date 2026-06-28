"""Monitoring module tests."""

from spark_optimal.monitoring.performance_analyzer import PerformanceAnalyzer
from spark_optimal.monitoring.quality_dashboard import QualityDashboard, QualityMetricsCollector
from spark_optimal.monitoring.throughput_tracker import ThroughputTracker


def test_throughput_tracker():
    tracker = ThroughputTracker()
    tracker.record({"job": "test", "duration_seconds": 10, "output_count": 1000})
    assert tracker.latest() is not None
    assert tracker.latest().rows_per_second == 100


def test_quality_dashboard():
    collector = QualityMetricsCollector()
    collector.record("critical_etl", 0.95, 500)
    summary = QualityDashboard(collector).summary()
    assert summary["checks"] == 1


def test_performance_analyzer():
    report = PerformanceAnalyzer().analyze("migration", 10.0, {"spark.executor.instances": "1"})
    assert report.workload_type == "migration"
