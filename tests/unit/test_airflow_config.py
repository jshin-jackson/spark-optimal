"""Airflow configuration tests."""

import os

from airflow.dags.common.sbi_config import SBIAirflowConfig


def test_sbi_airflow_config_defaults(monkeypatch):
    monkeypatch.delenv("SPARK_OPTIMAL_HOME", raising=False)
    cfg = SBIAirflowConfig.from_airflow()
    assert cfg.sbi_env in ("prod", "dev", "uat")
    assert cfg.target_gb == 10.0
    assert "hdfs://" in cfg.hdfs_raw


def test_bash_prefix_includes_env(monkeypatch):
    monkeypatch.setenv("SPARK_OPTIMAL_HOME", "/opt/spark-optimal")
    cfg = SBIAirflowConfig(
        project_home="/opt/spark-optimal",
        sbi_env="prod",
        target_gb=10.0,
        local_output="/opt/spark-optimal/data/output/financial",
        hdfs_raw="hdfs://ns1/prod/data/brnz/transactions",
        schedule_financial="0 2 * * *",
        schedule_maintenance="0 3 * * 0",
        email_on_failure=None,
    )
    prefix = cfg.bash_prefix()
    assert "SPARK_OPTIMAL_HOME" in prefix
    assert "SBI_ENV='prod'" in prefix
