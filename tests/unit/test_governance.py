"""Governance module tests."""

from spark_optimal.governance.quality.validation_rules import ValidationRule, ValidationRuleRegistry
from spark_optimal.governance.security.delegation_token_manager import DelegationTokenManager
from spark_optimal.governance.standards.data_standards import BRONZE_TRANSACTION_STANDARD
from spark_optimal.config import load_ozone_encryption_config
from spark_optimal.governance.security.delegation_config import build_kms_spark_config
from spark_optimal.governance.standards.security_standards import (
    FORBIDDEN_PERMISSION_METHODS,
    OZONE_ENCRYPTION_CHECKLIST,
    RANGER_AUTHORIZATION_CHECKLIST,
    REQUIRED_DELEGATION_KEYS,
)


def test_delegation_token_manager():
    mgr = DelegationTokenManager()
    cfg = mgr.build_token_config("hdfs://ns1", "ofs://ozone1782570080")
    assert "hdfs://ns1" in cfg.filesystems
    assert mgr.validate_config(cfg.spark_properties)


def test_bronze_standard_columns():
    assert "transaction_id" in BRONZE_TRANSACTION_STANDARD.required_columns


def test_security_standards_keys():
    assert "spark.yarn.access.hadoopFileSystems" in REQUIRED_DELEGATION_KEYS


def test_ranger_authorization_standards():
    assert any("Ranger" in item for item in RANGER_AUTHORIZATION_CHECKLIST)
    assert "chmod" in FORBIDDEN_PERMISSION_METHODS
    assert "hdfs dfs -chmod" in FORBIDDEN_PERMISSION_METHODS


def test_ozone_encryption_config():
    cfg = load_ozone_encryption_config()
    assert cfg["encryption_key"] == "ozone_encryption_key"
    assert cfg["kms_service"] == "cm_kms"
    assert cfg["enabled"] is True
    assert any("ozone_encryption_key" in item for item in OZONE_ENCRYPTION_CHECKLIST)


def test_kms_spark_config(monkeypatch):
    monkeypatch.setenv("KMS_PROVIDER_URI", "kms://https@kms-host:9494/kms")
    cfg = build_kms_spark_config()
    assert cfg["spark.hadoop.hadoop.security.key.provider.path"] == "kms://https@kms-host:9494/kms"
