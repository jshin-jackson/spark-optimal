# HDFS Encryption (Ranger KMS)

All **spark-optimal HDFS raw ingest data** is encrypted at rest using **HDFS Encryption Zones (EZ)** and the dedicated Ranger KMS key:

| Property | Value |
|----------|-------|
| KMS service | `cm_kms` |
| Encryption key | **`hdfs_encryption_key`** |
| Cipher | AES/CTR/NoPadding, 256-bit |

Configuration inventory: `governance/configs/security/hdfs_encryption.yaml`

> Ozone Medallion data uses a **separate** key: **`ozone_encryption_key`**. See [Ozone Encryption](ozone-encryption.md).

---

## How it works

1. **Ranger KMS** stores the HDFS encryption key (`hdfs_encryption_key`).
2. **HDFS Encryption Zone** is created on an **empty** directory (e.g. `/dev/data/migration/upload`).
3. All files written under that path are **encrypted on write**, **decrypted on read** (transparent to `hdfs dfs` and Spark).
4. **Kerberos** identifies the client; **Ranger HDFS** authorizes paths; **Ranger KMS** authorizes key use.

```
Client (systest) → Kerberos auth → Ranger HDFS (path) → Ranger KMS (hdfs_encryption_key) → NameNode → encrypted blocks
```

---

## Prerequisites

| Item | Requirement |
|------|-------------|
| Ranger KMS | Running (`RANGER_KMS` in CDP stack) |
| Key | **`hdfs_encryption_key`** created in KMS UI (service `cm_kms`) |
| CM / core-site | `hadoop.security.key.provider.path` → KMS URI |
| Ranger KMS policy | **hdfs** (NameNode): Get Metadata, Generate EEK on **`hdfs_encryption_key`** |
| Ranger KMS policy | **systest**: Generate EEK, Decrypt EEK on **`hdfs_encryption_key`** |
| Ranger HDFS policy | raw path + parent dirs (see `ranger.yaml`) |
| HDFS admin | `hdfs crypto -createZone` may require superuser / crypto admin role |

---

## Create Encryption Zones (Gateway)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh

bash scripts/infrastructure/setup_hdfs_encryption_zone.sh
```

Manual equivalent (DEV financial raw):

```bash
# Verify key in Ranger KMS
hadoop key list | grep hdfs_encryption_key

# Empty directory required — create parents first (Ranger HDFS policy)
hdfs dfs -mkdir -p /dev/data/migration/upload

# Create Encryption Zone (HDFS admin may be required)
hdfs crypto -createZone -keyName hdfs_encryption_key -path /dev/data/migration/upload

# Verify
hdfs crypto -getZone /dev/data/migration/upload
```

Optional Spark event log zone:

```bash
hdfs dfs -mkdir -p /user/spark/applicationHistory
hdfs crypto -createZone -keyName hdfs_encryption_key -path /user/spark/applicationHistory
```

> **Important:** You **cannot** add an Encryption Zone to a directory that already contains files. Move data out, create the zone, then re-upload.

---

## Spark / Iceberg

`conf/{env}/spark-defaults.conf` sets:

```
spark.hadoop.hadoop.security.key.provider.path=${KMS_PROVIDER_URI}
```

`KMS_PROVIDER_URI` in `env.conf` must be set **after** `HADOOP_CONF_DIR=/etc/hadoop/conf`:

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
hdfs getconf -confKey hadoop.security.key.provider.path
# → add to config/env.conf:
# export KMS_PROVIDER_URI="kms://https@<kms-host>:9494/kms"
```

Spark executors inherit this so reads from encrypted HDFS paths work in cluster mode.

---

## Verification

```bash
bash scripts/security/security_check.sh
# checks hdfs_encryption_key + hdfs_encryption_key EZ + ozone_encryption_key + Ozone bucket

hdfs crypto -getZone /dev/data/migration/upload
hadoop key list | grep hdfs_encryption_key
```

---

## Troubleshooting

| Symptom | Cause | Action |
|---------|-------|--------|
| `createZone` permission denied | Not HDFS superuser / crypto admin | Request HDFS admin or run setup as admin |
| `Directory is not empty` | Files exist before EZ creation | Move data, create zone, re-upload |
| `hadoop key list` missing key | `KMS_PROVIDER_URI` unset or wrong | Set after `HADOOP_CONF_DIR`; verify CM core-site |
| Spark read HDFS fails, KMS in trace | systest lacks Decrypt/Generate EEK | Ranger KMS policy for **`hdfs_encryption_key`** |
| NN cannot write encrypted blocks | hdfs user lacks KMS ACL | Grant hdfs Get Metadata + Generate EEK on **`hdfs_encryption_key`** |
| EZ shows wrong key | Zone created with old/shared key | Recreate EZ with **`hdfs_encryption_key`** |

See also: [Ranger Authorization](ranger-authorization.md) · [Ozone Encryption](ozone-encryption.md)
