# Ranger Policies — Iceberg Tables on Ozone (Cloudera CDP 7.3.1 + SBI naming)

Iceberg tables under `ofs://ozone1782570080/{env}/data/{brnz,slvr,gld}/` require **multiple Ranger policies** across **Hadoop SQL** (`cm_hive`) and **Ozone** (`cm_ozone`). Spark, Hive, and Impala all use this model.

Configuration inventory: `governance/configs/security/ranger_iceberg_ozone_pairs.yaml`

---

## Cloudera official documentation (7.3.1)

| Topic | URL |
|-------|-----|
| Iceberg Ranger introduction | [iceberg-ranger-introduction](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-ranger-introduction.html) |
| Storage Handler policy | [iceberg-setup-ranger](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-setup-ranger.html) |
| SQL table query policy | [iceberg-setup-ranger-database-access](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-setup-ranger-database-access.html) |
| Iceberg files on Ozone | [iceberg-ozone-policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) |

---

## Cloudera policy model (summary)

From [Accessing Iceberg tables](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-ranger-introduction.html):

> You need to set up **two Hadoop SQL policies** to query Iceberg tables:
> 1. One to authorize users to access the **Iceberg files** (Storage Handler)
> 2. One to authorize users to **query Iceberg tables** (SQL table policy)

For **Ozone**, [Accessing Iceberg files in Ozone](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) adds:

> You must set up a **Hadoop SQL access policy** and **Ozone file system access policy**.

Therefore each table on Ozone needs **four policy types** (three in `cm_hive`, one in `cm_ozone`):

| # | Ranger service | Policy type | Purpose | Cloudera doc |
|---|----------------|-------------|---------|--------------|
| 1 | **cm_hive** (Hadoop SQL) | **Storage Handler** | `storage-type=iceberg`, **RW Storage** — CREATE/ALTER table **location** only | [setup-ranger](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-setup-ranger.html) |
| 2 | **cm_hive** | **SQL table** | `database` / `table` / `columns` — Select, Create, Alter, … | [database-access](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-setup-ranger-database-access.html) |
| 3 | **cm_hive** | **URL** | `ofs://volume/bucket/key` — Read/Write on Ozone table location | [ozone-policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) |
| 4 | **cm_ozone** | **volume, bucket, key** | OFS file access for table data | [ozone-policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html) |

> **Important:** RW Storage does **not** grant table **data** access. SQL table + URL + cm_ozone policies are all required.

In Ranger UI, **Hadoop SQL** appears as the preloaded service **`cm_hive`** (covers Hive, Impala, Hue, Spark SQL).

---

## SBI naming convention (production DLH pattern)

Policy names must be **unique within a service**. SBI DLH production uses:

| Policy type | Service | Pattern | PROD example | DEV example (`brnz_transactions`) |
|-------------|---------|---------|--------------|-----------------------------------|
| SQL table | cm_hive | **`{env}_{table}_db_plcy`** | `prd_gld_bidetl_db_plcy` | `dev_brnz_transactions_db_plcy` |
| URL | cm_hive | **`{env}_{table}_uri_plcy`** | `prd_gld_bidetl_uri_plcy` | `dev_brnz_transactions_uri_plcy` |
| Storage Handler | cm_hive | cluster default | edit `all - storage-type, storage-url` | same |
| Ozone volume | cm_ozone | **`{env}_volume_plcy`** | `prod_volume_plcy` | `dev_volume_plcy` |
| Ozone bucket | cm_ozone | **`{env}_data_bucket_plcy`** | `prod_data_bucket_plcy` | `dev_data_bucket_plcy` |
| Ozone key/layer | cm_ozone | **`{env}_data_{layer}_key_plcy`** | `prod_data_brnz_key_plcy` | `dev_data_brnz_key_plcy` |

**SBI triple per table:** cm_hive **`{env}_{table}_db_plcy`** + **`{env}_{table}_uri_plcy`** + cm_ozone **`{env}_data_{layer}_key_plcy`**

**Roles (PROD):** `SBI_ETLAdmin_Role`, `SBI_ETLUsers_RW_Role`, `SBI_ETLTester_RO_Role`  
**DEV Gateway:** `systest` user directly (or assign `SBI_ETLUsers_RW_Role`)

---

## Medallion tables (DEV)

| Table | cm_hive SQL | cm_hive URL | cm_ozone key | Ozone path |
|-------|-------------|-------------|--------------|------------|
| `brnz_transactions` | `dev_brnz_transactions_db_plcy` | `dev_brnz_transactions_uri_plcy` | `dev_data_brnz_key_plcy` | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| `slvr_transactions` | `dev_slvr_transactions_db_plcy` | `dev_slvr_transactions_uri_plcy` | `dev_data_slvr_key_plcy` | `ofs://.../dev/data/slvr/transactions` |
| `gld_daily_report` | `dev_gld_daily_report_db_plcy` | `dev_gld_daily_report_uri_plcy` | `dev_data_gld_key_plcy` | `ofs://.../dev/data/gld/daily_transaction_report` |

**Infrastructure (cm_ozone, once per env):** `dev_volume_plcy`, `dev_data_bucket_plcy`

Principal: **`systest@...`** for Spark (Gateway), Hive, and Impala in DEV.

---

## Setup procedure (Cloudera-aligned + SBI naming)

### 1. Enable Ozone in Ranger (once)

Cloudera Manager → **Ozone** → Configuration → search **`ranger_service`** → enable → restart Ozone.

### 2. cm_ozone infrastructure (once per env)

Ranger → **cm_ozone** → Add New Policy:

| Policy Name | volume | bucket | key | Permissions |
|-------------|--------|--------|-----|-------------|
| `{env}_volume_plcy` | `{env}` | `*` | `*` | Read, Write, Create |
| `{env}_data_bucket_plcy` | `{env}` | `data` | `*` | Read, Write, Create, Delete |

### 3. Cluster Storage Handler (once)

Ranger → **cm_hive** (Hadoop SQL) → edit default **`all - storage-type, storage-url`**:

| Field | Value |
|-------|-------|
| storage-type | `iceberg` |
| storage-url | `*` (Include) |
| Permission | **RW Storage** |
| User / Role | `systest` or `SBI_ETLUsers_RW_Role` |

### 4. Per table — cm_hive SQL policy `{env}_{table}_db_plcy`

Ranger → **cm_hive** → Add New Policy:

| Field | Example |
|-------|---------|
| Policy Name | `dev_brnz_transactions_db_plcy` |
| database | `sbi_financial` |
| table | `brnz_transactions` |
| columns | `*` |
| Permissions | Select, Create, Alter, All (as needed) |

### 5. Per table — cm_hive URL policy `{env}_{table}_uri_plcy`

Ranger → **cm_hive** → Add New Policy → resource **URL**:

| Field | Example |
|-------|---------|
| Policy Name | `dev_brnz_transactions_uri_plcy` |
| URL | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| Permissions | Read, Write |

### 6. Per table — cm_ozone key policy `{env}_data_{layer}_key_plcy`

Ranger → **cm_ozone** → Add New Policy:

| Field | Example |
|-------|---------|
| Policy Name | `dev_data_brnz_key_plcy` |
| volume | `dev` |
| bucket | `data` |
| key | `brnz/transactions` |
| Permissions | Read, Write, Create, Delete |

See [Accessing Iceberg files in Ozone](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html).

---

## Print inventory (Gateway)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/print_ranger_iceberg_pairs.sh
```

---

## Authorization flow

```
systest → Kerberos
  → cm_hive Storage Handler (iceberg, RW Storage)              — CREATE/ALTER location
  → cm_hive {env}_{table}_db_plcy (sbi_financial.table)        — SELECT/INSERT/ALTER metadata
  → cm_hive {env}_{table}_uri_plcy (ofs://.../table path)      — location Read/Write
  → cm_ozone {env}_data_{layer}_key_plcy (volume/bucket/key)   — OFS data files
  → Spark / Hive / Impala job succeeds
```

---

## Troubleshooting

| Symptom | Missing policy | Action |
|---------|----------------|--------|
| `PERMISSION_DENIED ... CREATE ... volume` | `{env}_volume_plcy` | Add cm_ozone volume policy with Create |
| CREATE TABLE fails on location | Storage Handler or URL | Add iceberg RW Storage + `_uri_plcy` |
| SELECT fails, files OK | SQL table policy | Add cm_hive `{env}_{table}_db_plcy` |
| Spark driver OK, executor OFS fail | cm_ozone or URL | Add `{env}_data_{layer}_key_plcy` + `_uri_plcy` |
| Impala/Hive differ from Spark | Incomplete Hadoop SQL set | All cm_hive policy types for same table |

See also: [Ranger Authorization](ranger-authorization.md)
