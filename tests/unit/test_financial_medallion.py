"""Medallion financial pipeline configuration tests."""

import os

import pytest

from spark_optimal.platform.medallion.financial_config import load_financial_medallion_config


@pytest.fixture(autouse=True)
def _default_dev_env(monkeypatch):
    monkeypatch.setenv("SBI_ENV", "dev")


def test_financial_medallion_paths_dev():
    cfg = load_financial_medallion_config()
    assert cfg["tables"]["brnz"]["location"] == "ofs://ozone1782570080/dev/data/brnz/transactions"
    assert cfg["tables"]["slvr"]["location"] == "ofs://ozone1782570080/dev/data/slvr/transactions"
    assert cfg["tables"]["gld"]["location"] == "ofs://ozone1782570080/dev/data/gld/daily_transaction_report"
    assert cfg["hdfs_raw_path"] == "hdfs://ns1/dev/data/migration/upload"
    assert cfg["sdv"]["target_gb"] == 10


def test_financial_medallion_paths_prod(monkeypatch):
    monkeypatch.setenv("SBI_ENV", "prod")
    cfg = load_financial_medallion_config()
    assert cfg["tables"]["brnz"]["location"].startswith("ofs://ozone1782570080/prod/data/brnz")
    assert cfg["hdfs_raw_path"] == "hdfs://ns1/prod/data/migration/upload"
