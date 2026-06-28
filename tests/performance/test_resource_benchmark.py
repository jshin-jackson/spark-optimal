"""Performance benchmark skeleton (no Spark cluster required)."""

import time

from spark_optimal.platform.factory.resource_calculator import ResourceCalculator


def test_resource_calculator_10gb_migration():
    calc = ResourceCalculator()
    estimate = calc.estimate(data_size_gb=10, max_executors=200)
    start = time.time()
    assert estimate.executors >= 1
    duration = time.time() - start
    assert duration < 1.0
