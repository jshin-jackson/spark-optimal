"""Governance module tests."""

from spark_optimal.governance.quality.validation_rules import ValidationRule, ValidationRuleRegistry
from spark_optimal.governance.security.delegation_token_manager import DelegationTokenManager
from spark_optimal.governance.standards.data_standards import BRONZE_TRANSACTION_STANDARD
from spark_optimal.config import load_hdfs_encryption_config, load_ozone_encryption_config, load_ranger_iceberg_ozone_pairs
from spark_optimal.governance.security.ranger_pairs import (
    resolve_paired_policies,
    resolve_table_policies,
    verify_pairs_match_medallion,
)
from spark_optimal.governance.security.delegation_config import build_kms_spark_config
from spark_optimal.governance.standards.security_standards import (
    FORBIDDEN_PERMISSION_METHODS,
    HDFS_ENCRYPTION_CHECKLIST,
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
    assert any("cm_hive" in item for item in RANGER_AUTHORIZATION_CHECKLIST)
    assert any("Storage Handler" in item for item in RANGER_AUTHORIZATION_CHECKLIST)
    assert "chmod" in FORBIDDEN_PERMISSION_METHODS
    assert "hdfs dfs -chmod" in FORBIDDEN_PERMISSION_METHODS


def test_ozone_encryption_config():
    cfg = load_ozone_encryption_config()
    assert cfg["encryption_key"] == "ozone_encryption_key"
    assert cfg["kms_service"] == "cm_kms"
    assert cfg["enabled"] is True
    assert any("ozone_encryption_key" in item for item in OZONE_ENCRYPTION_CHECKLIST)


def test_hdfs_encryption_config():
    cfg = load_hdfs_encryption_config()
    assert cfg["encryption_key"] == "hdfs_encryption_key"
    assert cfg["kms_service"] == "cm_kms"
    assert cfg["enabled"] is True
    assert cfg["encryption_zones"]["financial_raw"]["path_by_environment"]["dev"] == (
        "/dev/raw/financial/transactions"
    )
    assert any("hdfs_encryption_key" in item for item in HDFS_ENCRYPTION_CHECKLIST)


def test_kms_spark_config(monkeypatch):
    monkeypatch.setenv("KMS_PROVIDER_URI", "kms://https@kms-host:9494/kms")
    cfg = build_kms_spark_config()
    assert cfg["spark.hadoop.hadoop.security.key.provider.path"] == "kms://https@kms-host:9494/kms"


def test_ranger_iceberg_ozone_pairs_config():
    cfg = load_ranger_iceberg_ozone_pairs()
    assert cfg["ranger_services"]["hadoop_sql"] == "cm_hive"
    assert cfg["ranger_services"]["ozone"] == "cm_ozone"
    assert "cloudera_documentation" in cfg
    assert "storage_handler" in cfg["cloudera_documentation"]
    tables = {t["table_name"] for t in cfg["tables"]}
    assert tables == {"brnz_transactions", "slvr_transactions", "gld_daily_report"}


def test_ranger_paired_policies_dev():
    pairs = resolve_paired_policies("dev")
    assert len(pairs) == 3
    brnz = next(p for p in pairs if p["table_name"] == "brnz_transactions")
    assert brnz["hive_policy"]["policy_name"] == "brnz_transactions"
    assert brnz["ozone_policy"]["policy_name"] == "brnz_transactions"
    assert brnz["url_policy"]["policy_name"] == "brnz_transactions-url"
    assert brnz["ozone_policy"]["path"] == "ofs://ozone1782570080/dev/data/brnz/transactions"
    assert brnz["hive_policy"]["ranger_service"] == "cm_hive"
    assert brnz["ozone_policy"]["ranger_service"] == "cm_ozone"


def test_ranger_pairs_match_medallion():
    assert verify_pairs_match_medallion("dev") == []


def test_ranger_table_policies_include_storage_handler():
    entries = resolve_table_policies("dev")
    brnz = next(e for e in entries if e["table_name"] == "brnz_transactions")
    types = {p["policy_type"] for p in brnz["policies"]}
    assert types == {"sql_table", "url", "volume_bucket_key"}
    assert brnz["optional_policies"][0]["policy_type"] == "storage_handler"
