"""Unit tests for Kerberos manager (no kinit on dev laptop)."""

import os
from unittest.mock import MagicMock, patch

from spark_optimal.governance.security.delegation_config import build_kerberos_spark_config
from spark_optimal.governance.security.kerberos_manager import GatewayKerberosManager


def test_build_kerberos_spark_config():
    os.environ.setdefault("PRINCIPAL", "systest@QE-INFRA-AD.CLOUDERA.COM")
    os.environ.setdefault("KEYTAB", "/opt/cloudera/systest.keytab")
    os.environ.setdefault("HDFS_URI", "hdfs://ns1")
    os.environ.setdefault("OZONE_OM_SERVICE_ID", "ozone1782570080")
    os.environ.setdefault("OFS_URI", "ofs://ozone1782570080")
    os.environ.setdefault("HMS_HOST", "ccycloud-1.jshin-sbi.root.comops.site")
    os.environ.setdefault(
        "HMS_URIS",
        "thrift://ccycloud-1.jshin-sbi.root.comops.site:9083,thrift://ccycloud-6.jshin-sbi.root.comops.site:9083",
    )
    os.environ.setdefault("HMS_PRINCIPAL", "hive/_HOST@QE-INFRA-AD.CLOUDERA.COM")

    config = build_kerberos_spark_config()
    assert config["spark.security.credentials.hadoopfs.enabled"] == "true"
    assert "hdfs://ns1" in config["spark.yarn.access.hadoopFileSystems"]
    assert "ofs://ozone1782570080" in config["spark.yarn.access.hadoopFileSystems"]


@patch("spark_optimal.governance.security.kerberos_manager.subprocess.run")
def test_kinit_success(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
    manager = GatewayKerberosManager(keytab_path="/tmp/fake.keytab", principal="systest@TEST")
    with patch.object(manager, "check_auth_status", return_value=MagicMock(is_valid=False)):
        with patch("os.path.isfile", return_value=True):
            result = manager.perform_kinit(force=True)
    assert result.success is True
