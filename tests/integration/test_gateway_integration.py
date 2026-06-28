"""Integration test placeholders (run on gateway with Spark)."""

import pytest

pytestmark = pytest.mark.integration


def test_integration_placeholder():
    pytest.skip("Run on CDP gateway: bash scripts/pipeline/run_financial_pipeline.sh")
