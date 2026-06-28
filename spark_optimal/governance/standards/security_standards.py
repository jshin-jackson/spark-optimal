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
    "HDFS access is granted only via Ranger cm_hdfs policies (not chmod/chown/setfacl)",
    "Iceberg on Ozone (Cloudera CDP 7.3.1): cm_hive Storage Handler (iceberg, RW Storage) + SQL table + URL + cm_ozone per table",
    "SBI pair: cm_hive SQL policy {table} + cm_ozone policy {table} — same name as Iceberg table",
    "Also required: cm_hive URL policy {table}-url for ofs:// table location (Cloudera ozone-policy doc)",
    "RW Storage does NOT grant data access — SQL + URL + cm_ozone policies are still required",
    "systest must hold all policies to run Spark, Hive, and Impala without authz gaps",
    "Run scripts/security/print_ranger_iceberg_pairs.sh — see docs/operations/ranger-iceberg-ozone-pairs.md",
    "Run scripts/security/security_check.sh before spark-submit",
]

OZONE_ENCRYPTION_CHECKLIST = [
    "All Medallion Ozone data uses Ranger KMS key ozone_encryption_key (TDE)",
    "Create bucket with: ozone sh bucket create -k ozone_encryption_key {env}/data",
    "Ranger KMS: OM service user needs Get Metadata + Generate EEK on ozone_encryption_key",
    "Ranger KMS: systest needs Generate EEK + Decrypt EEK on ozone_encryption_key",
    "KMS_PROVIDER_URI set in env.conf (from hdfs getconf -confKey hadoop.security.key.provider.path)",
    "Run scripts/infrastructure/setup_ozone_encrypted_bucket.sh before first pipeline",
]

HDFS_ENCRYPTION_CHECKLIST = [
    "All HDFS raw ingest data uses Ranger KMS key hdfs_encryption_key (Encryption Zone)",
    "Create zone with: hdfs crypto -createZone -keyName hdfs_encryption_key -path /{env}/raw/financial/transactions",
    "Ranger KMS: hdfs (NameNode) service user needs Get Metadata + Generate EEK on hdfs_encryption_key",
    "Ranger KMS: systest needs Generate EEK + Decrypt EEK on hdfs_encryption_key",
    "Encryption Zone requires an empty directory — run setup before first upload",
    "Run scripts/infrastructure/setup_hdfs_encryption_zone.sh before upload_to_hdfs.sh",
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
