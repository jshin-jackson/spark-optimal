# Ranger Policies — Iceberg Tables on Ozone (Cloudera CDP 7.3.1)

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

## SBI naming convention

Policy names must be **unique within a service**. SBI uses:

| Policy type | Service | Policy name | Example (`brnz_transactions`) |
|-------------|---------|-------------|--------------------------------|
| SQL table | cm_hive | **`{table_name}`** | `brnz_transactions` |
| URL | cm_hive | **`{table_name}-url`** | `brnz_transactions-url` |
| Storage Handler | cm_hive | cluster default **or** `{table_name}-storage` | edit `all - storage-type, storage-url` |
| Ozone FS | cm_ozone | **`{table_name}`** | `brnz_transactions` |

**SBI pair:** cm_hive SQL policy **`aaa`** + cm_ozone policy **`aaa`** (same table name).  
Cloudera additionally requires Storage Handler + URL policies (see table above).

---

## Medallion tables (DEV)

| Table | cm_hive SQL | cm_hive URL | cm_ozone | Ozone path |
|-------|-------------|-------------|----------|------------|
| `brnz_transactions` | `brnz_transactions` | `brnz_transactions-url` | `brnz_transactions` | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| `slvr_transactions` | `slvr_transactions` | `slvr_transactions-url` | `slvr_transactions` | `ofs://.../dev/data/slvr/transactions` |
| `gld_daily_report` | `gld_daily_report` | `gld_daily_report-url` | `gld_daily_report` | `ofs://.../dev/data/gld/daily_transaction_report` |

Principal: **`systest@...`** for Spark (Gateway), Hive, and Impala.

---

## Setup procedure (Cloudera-aligned)

### 1. Enable Ozone in Ranger (once)

Cloudera Manager → **Ozone** → Configuration → search **`ranger_service`** → enable → restart Ozone.

### 2. Cluster Storage Handler (once)

Ranger → **cm_hive** (Hadoop SQL) → edit default **`all - storage-type, storage-url`**:

| Field | Value |
|-------|-------|
| storage-type | `iceberg` |
| storage-url | `*` (Include) |
| Permission | **RW Storage** |
| User | `systest` |

See [Editing a storage handler policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-setup-ranger.html).

### 3. Per table — cm_hive SQL policy `{table}`

Ranger → **cm_hive** → Add New Policy:

| Field | Example |
|-------|---------|
| Policy Name | `brnz_transactions` |
| database | `sbi_financial` |
| table | `brnz_transactions` |
| columns | `*` |
| Permissions | Select, Create, Alter, All (as needed) |

See [Creating a SQL policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-setup-ranger-database-access.html).

### 4. Per table — cm_hive URL policy `{table}-url`

Ranger → **cm_hive** → Add New Policy → resource **URL**:

| Field | Example |
|-------|---------|
| Policy Name | `brnz_transactions-url` |
| URL | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| Permissions | Read, Write |

Alternatively edit default **`all - url`** and add `systest` (broader — Cloudera ozone doc).

### 5. Per table — cm_ozone policy `{table}`

Ranger → **cm_ozone** → Add New Policy (or edit **`all - volume, bucket, key`** for dev):

| Field | Example |
|-------|---------|
| Policy Name | `brnz_transactions` |
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
  → cm_hive Storage Handler (iceberg, RW Storage)     — CREATE/ALTER location
  → cm_hive SQL table policy (sbi_financial.table)    — SELECT/INSERT/ALTER metadata
  → cm_hive URL policy (ofs://.../table path)         — location Read/Write
  → cm_ozone policy (volume/bucket/key)               — OFS data files
  → Spark / Hive / Impala job succeeds
```

---

## Troubleshooting

| Symptom | Missing policy | Action |
|---------|----------------|--------|
| CREATE TABLE fails on location | Storage Handler or URL | Add iceberg RW Storage + ofs URL policy |
| SELECT fails, files OK | SQL table policy | Add cm_hive `{table}` on `sbi_financial.{table}` |
| Spark driver OK, executor OFS fail | cm_ozone or URL | Add cm_ozone `{table}` + cm_hive `{table}-url` |
| Impala/Hive differ from Spark | Incomplete Hadoop SQL set | All three cm_hive policy types for same table |

See also: [Ranger Authorization](ranger-authorization.md)
