"""Delegation token configuration for Spark on YARN (HDFS, Ozone, Hive)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List

logger = logging.getLogger(__name__)


@dataclass
class DelegationTokenConfig:
    filesystems: List[str]
    spark_properties: Dict[str, str]


class DelegationTokenManager:
    """Build Spark properties that enable automatic delegation token collection."""

    REQUIRED_PROPERTIES = {
        "spark.security.credentials.hadoopfs.enabled": "true",
        "spark.security.credentials.hive.enabled": "true",
        "spark.yarn.security.tokens.hadoopfs.enabled": "true",
        "spark.yarn.security.tokens.hive.enabled": "true",
        "spark.kerberos.renewal.credentials": "ccache",
    }

    def build_token_config(self, hdfs_uri: str, ofs_uri: str) -> DelegationTokenConfig:
        filesystems = [hdfs_uri, ofs_uri]
        props = dict(self.REQUIRED_PROPERTIES)
        props["spark.yarn.access.hadoopFileSystems"] = ",".join(filesystems)
        logger.info("Delegation token filesystems: %s", props["spark.yarn.access.hadoopFileSystems"])
        return DelegationTokenConfig(filesystems=filesystems, spark_properties=props)

    def validate_config(self, spark_properties: Dict[str, str]) -> bool:
        for key in self.REQUIRED_PROPERTIES:
            if spark_properties.get(key) != "true" and key != "spark.kerberos.renewal.credentials":
                return False
        return "spark.yarn.access.hadoopFileSystems" in spark_properties
