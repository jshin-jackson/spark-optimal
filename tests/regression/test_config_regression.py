"""Regression tests for configuration stability."""

from spark_optimal.config import load_department_policies, load_resource_profiles
from spark_optimal.platform.factory.template_manager import TemplateManager


def test_resource_profiles_stable_keys():
    profiles = load_resource_profiles()
    assert "migration" in profiles
    assert "etl_standard" in profiles


def test_department_policies_fraud_queue():
    policies = load_department_policies()
    assert policies["fraud_detection"]["yarn_queue"] == "fraud"


def test_template_manager_merges_kerberos():
    tm = TemplateManager()
    import os
    os.environ.setdefault("PRINCIPAL", "systest@QE-INFRA-AD.CLOUDERA.COM")
    os.environ.setdefault("KEYTAB", "/opt/cloudera/systest.keytab")
    os.environ.setdefault("HDFS_URI", "hdfs://ns1")
    os.environ.setdefault("OFS_URI", "ofs://ozone1782570080")
    os.environ.setdefault("HMS_HOST", "ccycloud-1.jshin-sbi.root.comops.site")
    os.environ.setdefault("HMS_URIS", "thrift://ccycloud-1:9083")
    os.environ.setdefault("OZONE_OM_SERVICE_ID", "ozone1782570080")
    os.environ.setdefault("OZONE_OM_ADDRESS", "ccycloud-2:9862")
    os.environ.setdefault("HMS_PRINCIPAL", "hive/_HOST@QE-INFRA-AD.CLOUDERA.COM")
    config = tm.build_full_template("migration", "dev")
    assert config["spark.security.credentials.hadoopfs.enabled"] == "true"
