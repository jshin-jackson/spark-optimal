"""Security standards for gateway Spark execution."""

from __future__ import annotations

GATEWAY_SECURITY_CHECKLIST = [
    "Run kinit with systest.keytab before spark-submit",
    "Verify spark.yarn.access.hadoopFileSystems includes HDFS and OFS",
    "Enable spark.security.credentials.hadoopfs.enabled=true",
    "Enable spark.security.credentials.hive.enabled=true",
    "Use YARN cluster deploy mode from gateway",
    "Do not override CM-managed Java --add-opens options",
]

REQUIRED_KERBEROS_KEYS = [
    "spark.kerberos.principal",
    "spark.kerberos.keytab",
    "spark.yarn.principal",
    "spark.yarn.keytab",
]

REQUIRED_DELEGATION_KEYS = [
    "spark.yarn.access.hadoopFileSystems",
    "spark.security.credentials.hadoopfs.enabled",
    "spark.security.credentials.hive.enabled",
]
