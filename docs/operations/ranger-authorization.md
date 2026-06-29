# Ranger Authorization (SBI spark-optimal)

## Policy

**HDFS, HMS (Hive Metastore), Spark, and Ozone** on the SBI CDP cluster use **Apache Ranger** for authorization.

| Layer | Tool | Role |
|-------|------|------|
| Authentication | Kerberos (`systest` keytab, kinit) | Proves identity |
| Authorization | **Ranger only** | Grants or denies access to paths, tables, Ozone resources |

**Do not use** filesystem ACLs or manual permission changes:

- `chmod`, `chown`, `setfacl`
- `hdfs dfs -chmod`, `hdfs dfs -chown`
- Ozone ACLs outside Ranger

If access fails with a valid Kerberos ticket, **update Ranger policies** — not POSIX/HDFS ACLs.

---

## Gateway principal

| Item | Value |
|------|-------|
| Principal | `systest@QE-INFRA-AD.CLOUDERA.COM` (or `${PRINCIPAL}` from `env.conf`) |
| Keytab | `/opt/cloudera/systest.keytab` |

Delegation tokens carry this identity to Spark executors; **Ranger still enforces access** on HDFS, Ozone, and Hive.

---

## Required Ranger resources (by service)

Resource patterns are defined in:

```
governance/configs/security/ranger.yaml
```

Environment-specific paths come from:

```
governance/configs/environments/{dev,uat,prod}.yaml  → medallion section
```

### HDFS (Ranger HDFS plugin)

| Purpose | Example (DEV) | Actions |
|---------|---------------|---------|
| Financial raw ingest | `hdfs://ns1/dev/data/migration/upload` | read, write, execute |
| Spark event log | `hdfs:///user/spark/applicationHistory` | read, write, execute |

### Ozone + Iceberg (Cloudera CDP 7.3.1)

Each Iceberg table on Ozone requires policies documented in [Ranger Iceberg–Ozone Pairs](ranger-iceberg-ozone-pairs.md) (Cloudera official):

| # | Service | Type | SBI policy name | Cloudera reference |
|---|---------|------|-----------------|-------------------|
| 1 | **cm_hive** (Hadoop SQL) | Storage Handler | edit `all - storage-type, storage-url` | [setup-ranger](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-setup-ranger.html) |
| 2 | **cm_hive** | SQL table | **`{env}_{table}_db_plcy`** | [database-access](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-setup-ranger-database-access.html) |
| 3 | **cm_hive** | URL (`ofs://…`) | **`{env}_{table}_uri_plcy`** | [ozone-policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) |
| 4 | **cm_ozone** | volume/bucket/key | **`{env}_data_{layer}_key_plcy`** | [ozone-policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) |

**SBI triple:** cm_hive **`dev_brnz_transactions_db_plcy`** + **`dev_brnz_transactions_uri_plcy`** + cm_ozone **`dev_data_brnz_key_plcy`**. RW Storage alone does not grant data access.

**Infrastructure (cm_ozone):** **`{env}_volume_plcy`**, **`{env}_data_bucket_plcy`** — volume/bucket creation; separate from per-layer key policies.

Print full checklist:

```bash
bash scripts/security/print_ranger_iceberg_pairs.sh
```

### Hive / HMS

Optional database policy **`{env}_sbi_financial_db_plcy`** on **cm_hive** supplements but does **not** replace per-table `_db_plcy` + `_uri_plcy` + cm_ozone `_key_plcy` policies above.

### Spark / Hive / Impala

All three engines use **Hadoop SQL** (`cm_hive`) + **cm_ozone** for Iceberg on Ozone. Spark does not bypass Ranger.

### Ranger KMS (HDFS + Ozone TDE)

HDFS and Ozone use **separate** Ranger KMS keys on service **`cm_kms`**:

| Key | Use | Principals (permissions on key) |
|-----|-----|--------------------------------|
| **`hdfs_encryption_key`** | HDFS Encryption Zones | **hdfs** (NN): Get Metadata, Generate EEK · **systest**: Generate EEK, Decrypt EEK |
| **`ozone_encryption_key`** | Ozone encrypted bucket | **OM** service user: Get Metadata, Generate EEK · **systest**: Generate EEK, Decrypt EEK |

HDFS Encryption Zone (empty directory required):

```bash
hdfs crypto -createZone -keyName hdfs_encryption_key -path /dev/data/migration/upload
```

Ozone bucket must be created with encryption at creation time:

```bash
ozone sh bucket create -k ozone_encryption_key dev/data
```

See [HDFS Encryption](hdfs-encryption.md), [Ozone Encryption](ozone-encryption.md), `hdfs_encryption.yaml`, and `ozone_encryption.yaml`.

---

## Pre-flight check (Gateway)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh
bash scripts/security/security_check.sh
```

`security_check.sh` verifies:

1. Valid Kerberos ticket
2. HDFS access to medallion raw path (Ranger)
3. Ozone/OFS access to Bronze prefix (Ranger)

Failures with an valid ticket → request Ranger policy update using `governance/configs/security/ranger.yaml`.

---

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `Permission denied` / `AccessControlException` on `hdfs dfs` | Ranger HDFS policy missing | Add policy for `${PRINCIPAL}` on path in `ranger.yaml` |
| Executor cannot read/write OFS | **cm_ozone** key policy missing | Add `{env}_data_{layer}_key_plcy` + cm_hive `_uri_plcy` |
| `CREATE TABLE` / HMS / Iceberg failure | **cm_hive** paired policy missing | Add cm_hive policy named = table on `sbi_financial.<table>` |
| Hive/Impala OK on one table, not another | Incomplete paired set | Register both cm_hive + cm_ozone for **each** table |
| `GSS initiate failed` | Kerberos (not Ranger) | `kinit_manager.sh` |
| Path does not exist | Not Ranger — run mkdir after write policy exists | `hdfs dfs -mkdir -p` or Spark create |

---

## Related docs

- [Ranger Iceberg–Ozone Paired Policies](ranger-iceberg-ozone-pairs.md)
- [Gateway Runbook](gateway-runbook.md)
- [Troubleshooting](../troubleshooting/common-issues.md)
- [Spark Job Standards](../standards/spark-job-standards.md)
- [DEV Jenkins Rebuild](../infrastructure/dev-jenkins-rebuild.md) — Ranger deployed with CDP
