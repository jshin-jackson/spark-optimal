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

# Authorization: Apache Ranger only (HDFS, HMS, Spark, Ozone). No filesystem ACL bypass.
RANGER_AUTHORIZATION_CHECKLIST = [
    "HDFS access is granted only via Ranger HDFS policies (not chmod/chown/setfacl)",
    "Ozone volume/bucket/prefix access is granted only via Ranger Ozone policies",
    "Hive/Iceberg HMS access is granted only via Ranger Hive policies",
    "Spark jobs inherit systest identity; Ranger enforces access on executors",
    "On Permission denied with valid klist: update Ranger — see governance/configs/security/ranger.yaml",
    "Run scripts/security/security_check.sh before spark-submit to probe Ranger-backed paths",
]

FORBIDDEN_PERMISSION_METHODS = [
    "chmod",
    "chown",
    "setfacl",
    "hdfs dfs -chmod",
    "hdfs dfs -chown",
    "ozone native ACL outside Ranger",
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
