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
| Financial raw ingest | `hdfs://ns1/dev/raw/financial/transactions` | read, write, execute |
| Spark event log | `hdfs:///user/spark/applicationHistory` | read, write, execute |

### Ozone (Ranger Ozone plugin)

| Purpose | Example (DEV) | Actions |
|---------|---------------|---------|
| Volume | `dev` | read, write, create |
| Bucket | `dev/data` | read, write, create, delete |
| Medallion prefixes | `ofs://ozone1782570080/dev/data/{brnz,slvr,gld}` | read, write, create, delete |

Volume/bucket **creation** (`ozone sh volume create`, `bucket create`) succeeds only if Ranger allows it for `systest`.

### Hive / HMS (Ranger Hive plugin)

| Purpose | Resource | Actions |
|---------|----------|---------|
| Iceberg catalog | database `sbi_financial` | create, select, update, alter, drop, … |
| Medallion tables | `brnz_transactions`, `slvr_transactions`, `gld_daily_report` | as required by ETL |

### Spark (Ranger Spark plugin)

Where the Spark Ranger plugin is enabled, align policies with Hive/HDFS/Ozone resources used by Spark SQL and Iceberg jobs. Spark does **not** bypass Ranger.

### Ranger KMS (Ozone TDE)

All Medallion **Ozone** data is encrypted at rest with Ranger KMS key **`ozone_encryption_key`** (service `cm_kms`).

| Principal | Permissions on `ozone_encryption_key` |
|-----------|--------------------------------------|
| Ozone Manager service user | Get Metadata, Generate EEK |
| `systest` (Gateway / Spark) | Generate EEK, Decrypt EEK |

Bucket must be created with encryption at creation time:

```bash
ozone sh bucket create --volume dev --bucket data --bucketkey ozone_encryption_key
```

See [Ozone Encryption](ozone-encryption.md) and `governance/configs/security/ozone_encryption.yaml`.

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
| Executor cannot read/write OFS | Ranger Ozone policy missing | Add Ozone policy for volume/bucket/prefix |
| `CREATE TABLE` / HMS / Iceberg failure | Ranger Hive policy missing | Grant `sbi_financial` DB + table privileges |
| `GSS initiate failed` | Kerberos (not Ranger) | `kinit_manager.sh` |
| Path does not exist | Not Ranger — run mkdir after write policy exists | `hdfs dfs -mkdir -p` or Spark create |

---

## Related docs

- [Gateway Runbook](gateway-runbook.md)
- [Troubleshooting](../troubleshooting/common-issues.md)
- [Spark Job Standards](../standards/spark-job-standards.md)
- [DEV Jenkins Rebuild](../infrastructure/dev-jenkins-rebuild.md) — Ranger deployed with CDP
