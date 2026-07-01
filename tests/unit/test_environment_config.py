"""Environment-driven configuration tests."""

import pytest

from spark_optimal.config import load_cluster_config, load_environment_config, load_resource_limits


def test_dev_cluster_from_yaml():
    cfg = load_cluster_config("dev")
    assert cfg["total_vcores"] == 72
    assert cfg["total_memory_gb"] == 288
    assert cfg["node_managers"] == 9
    assert cfg["cores_per_node"] == 16
    assert cfg["memory_per_node_gb"] == 32


def test_uat_cluster_from_yaml():
    cfg = load_cluster_config("uat")
    assert cfg["total_vcores"] == 1085
    assert cfg["total_memory_gb"] == 20163
    assert cfg["node_managers"] == 40
    assert cfg["cores_per_node"] == 64
    assert cfg["memory_per_node_gb"] == 1229


def test_prod_cluster_from_yaml():
    cfg = load_cluster_config("prod")
    assert cfg["total_vcores"] == 26496
    assert cfg["node_managers"] == 214


def test_dev_medallion_paths_in_environment_yaml():
    env_cfg = load_environment_config("dev")
    assert env_cfg["medallion"]["hdfs_raw_path"].endswith("/dev/data/migration/upload")


def test_missing_cluster_raises(monkeypatch):
    monkeypatch.setattr(
        "spark_optimal.config.load_environment_config",
        lambda env=None: {"environment": "test"},
    )
    with pytest.raises(ValueError, match="Missing 'cluster'"):
        load_cluster_config("test")


def test_dev_resource_limits_from_yaml():
    limits = load_resource_limits("dev")
    assert limits["max_executors_absolute"] == 24
    assert limits["shuffle_partitions"] == 48
