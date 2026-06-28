# Ozone Encryption (Ranger KMS)

All **spark-optimal Medallion data on Ozone** is encrypted at rest using **Transparent Data Encryption (TDE)** and the Ranger KMS key:

| Property | Value |
|----------|-------|
| KMS service | `cm_kms` |
| Encryption key | **`ozone_encryption_key`** |
| Cipher | AES/CTR/NoPadding, 256-bit |

Configuration inventory: `governance/configs/security/ozone_encryption.yaml`

---

## How it works

1. **Ranger KMS** stores the encryption key (`ozone_encryption_key`).
2. **Ozone bucket** `/{env}/data` is created with `--bucketkey ozone_encryption_key`.
3. All objects under `ofs://.../{env}/data/brnz|slvr|gld/` are **encrypted on write**, **decrypted on read** (transparent to Spark/Iceberg).
4. **Kerberos** identifies the client; **Ranger Ozone** authorizes paths; **Ranger KMS** authorizes key use.

```
Client (systest) → Kerberos auth → Ranger Ozone (path) → Ranger KMS (key) → Ozone OM → encrypted blocks
```

---

## Prerequisites

| Item | Requirement |
|------|-------------|
| Ranger KMS | Running (`RANGER_KMS` in CDP stack) |
| Key | `ozone_encryption_key` created in KMS UI (service `cm_kms`) |
| CM / core-site | `hadoop.security.key.provider.path` → KMS URI |
| Ranger KMS policy | **OM** service user: Get Metadata, Generate EEK |
| Ranger KMS policy | **systest**: Generate EEK, Decrypt EEK on `ozone_encryption_key` |
| Ranger Ozone policy | volume/bucket/prefix access (see `ranger.yaml`) |

---

## Create encrypted Medallion bucket (Gateway)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh

bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh
```

Manual equivalent:

```bash
# Verify key in Ranger KMS
hadoop key list | grep ozone_encryption_key

# Volume (Ranger Ozone policy required)
ozone sh volume create dev   # skip if exists

# Encrypted bucket — MUST use -k/--bucketkey at creation (CDP 7.3: volume/bucket URI)
ozone sh bucket create -k ozone_encryption_key dev/data
```

> **Important:** Encryption is **bucket-level**. An existing unencrypted `data` bucket must be **recreated** with `--bucketkey` (after backup/migration). You cannot attach a key to an already-created plain bucket.

---

## Spark / Iceberg

`conf/{env}/spark-defaults.conf` sets:

```
spark.hadoop.hadoop.security.key.provider.path=${KMS_PROVIDER_URI}
```

`KMS_PROVIDER_URI` in `env.conf` should match CM (`hdfs getconf -confKey hadoop.security.key.provider.path`).  
Spark executors inherit this so Iceberg writes to encrypted Ozone buckets work in cluster mode.

---

## Verification

```bash
bash scripts/security/security_check.sh
# includes KMS key + encrypted bucket checks

hadoop key list
ozone sh bucket info dev/data
```

---

## Troubleshooting

| Symptom | Cause | Action |
|---------|-------|--------|
| `Cannot create bucket` / KMS error | Missing Ranger KMS policy for OM | Grant OM Get Metadata + Generate EEK |
| Spark write to Ozone fails, KMS in stack trace | systest lacks Decrypt/Generate EEK | Ranger KMS policy for `ozone_encryption_key` |
| Data readable without encryption | Bucket created without `--bucketkey` | Recreate bucket with `ozone_encryption_key` |
| `hadoop key list` missing key | Key not in `cm_kms` | Create in Ranger KMS UI |

See also: [Ranger Authorization](ranger-authorization.md)
