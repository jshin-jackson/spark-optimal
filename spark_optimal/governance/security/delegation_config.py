"""Delegation token and Spark security configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from spark_optimal.config import load_environment_config


@dataclass
class SecurityContext:
    principal: str
    keytab_path: str
    hdfs_uri: str
    ofs_uri: str
    hms_principal: str


# Delegation tokens propagate Kerberos identity to Spark executors.
# Authorization on HDFS/Ozone/Hive is still enforced by Apache Ranger (not filesystem ACLs).
GATEWAY_SPARK_SECURITY_DEFAULTS: Dict[str, str] = {
    "spark.security.credentials.hadoopfs.enabled": "true",
    "spark.security.credentials.hive.enabled": "true",
    "spark.yarn.security.tokens.hadoopfs.enabled": "true",
    "spark.yarn.security.tokens.hive.enabled": "true",
    "spark.kerberos.renewal.credentials": "ccache",
    "spark.hadoop.fs.hdfs.impl.disable.cache": "true",
    "spark.hadoop.fs.ofs.impl.disable.cache": "true",
}


def _env_or_config(key: str, env_var: str, env_cfg: dict) -> str:
    value = os.environ.get(env_var)
    if value:
        return value
    if key in env_cfg:
        return str(env_cfg[key])
    raise KeyError(f"Missing {env_var} environment variable and '{key}' in environment config")


def build_security_context() -> SecurityContext:
    env_cfg = load_environment_config()
    return SecurityContext(
        principal=os.environ["PRINCIPAL"],
        keytab_path=os.environ["KEYTAB"],
        hdfs_uri=_env_or_config("hdfs_uri", "HDFS_URI", env_cfg),
        ofs_uri=_env_or_config("ofs_uri", "OFS_URI", env_cfg),
        hms_principal=os.environ.get("HMS_PRINCIPAL", "hive/_HOST@QE-INFRA-AD.CLOUDERA.COM"),
    )


def build_kerberos_spark_config(context: SecurityContext | None = None) -> Dict[str, str]:
    ctx = context or build_security_context()
    config = dict(GATEWAY_SPARK_SECURITY_DEFAULTS)
    config.update(
        {
            "spark.kerberos.principal": ctx.principal,
            "spark.kerberos.keytab": ctx.keytab_path,
            "spark.yarn.principal": ctx.principal,
            "spark.yarn.keytab": ctx.keytab_path,
            "spark.yarn.access.hadoopFileSystems": f"{ctx.hdfs_uri},{ctx.ofs_uri}",
            "spark.hadoop.hive.metastore.sasl.enabled": "true",
            "spark.hadoop.hive.metastore.kerberos.principal": ctx.hms_principal,
        }
    )
    return config


def build_iceberg_spark_config() -> Dict[str, str]:
    env_cfg = load_environment_config()
    hms_uris = os.environ.get("HMS_URIS") or env_cfg.get("hms_uris")
    if not hms_uris:
        hms_host = _env_or_config("hms_host", "HMS_HOST", env_cfg)
        hms_port = os.environ.get("HMS_PORT", env_cfg.get("hms_port", "9083"))
        hms_uris = f"thrift://{hms_host}:{hms_port}"
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.spark_catalog.uri": hms_uris,
        "spark.hadoop.hive.metastore.uris": hms_uris,
        "spark.sql.iceberg.merge-on-read.enabled": "true",
        "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    }


def build_ozone_spark_config() -> Dict[str, str]:
    env_cfg = load_environment_config()
    service_id = _env_or_config("ozone_om_service_id", "OZONE_OM_SERVICE_ID", env_cfg)
    om_address = _env_or_config("ozone_om_address", "OZONE_OM_ADDRESS", env_cfg)
    return {
        "spark.hadoop.fs.ofs.impl": "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem",
        "spark.hadoop.ozone.om.service.ids": service_id,
        f"spark.hadoop.ozone.om.address.{service_id}": om_address,
    }
