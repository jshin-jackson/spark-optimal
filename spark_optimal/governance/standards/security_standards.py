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

OZONE_ENCRYPTION_CHECKLIST = [
    "All Medallion Ozone data uses Ranger KMS key ozone_encryption_key (TDE)",
    "Create bucket with: ozone sh bucket create --volume {env} --bucket data --bucketkey ozone_encryption_key",
    "Ranger KMS: OM service user needs Get Metadata + Generate EEK on ozone_encryption_key",
    "Ranger KMS: systest needs Generate EEK + Decrypt EEK on ozone_encryption_key",
    "KMS_PROVIDER_URI set in env.conf (from hdfs getconf -confKey hadoop.security.key.provider.path)",
    "Run scripts/infrastructure/setup_ozone_encrypted_bucket.sh before first pipeline",
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
