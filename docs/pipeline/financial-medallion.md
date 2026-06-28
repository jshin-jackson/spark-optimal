# Financial Medallion Pipeline

SDV synthetic financial JSON → HDFS → Ozone Iceberg (Bronze/Silver/Gold).

## Flow

```mermaid
graph LR
    sdv[Step1 SDV JSON 10GB] --> local[Local JSONL]
    local --> hdfs[Step2 HDFS Raw]
    hdfs --> brnz[Step3 Bronze Iceberg]
    brnz --> slvr[Step4 Silver Iceberg]
    slvr --> gld[Step4 Gold Report]
```

| Step | Action | Location |
|------|--------|----------|
| 1 | SDV generate ~10GB JSON | `data/output/financial/*.jsonl` |
| 2 | Upload to HDFS | `hdfs://ns1/prod/raw/financial/transactions` |
| 3 | Spark ingest JSON | `ofs://ozone1782570080/prod/data/brnz/transactions` |
| 4 | ETL to report tables | `slvr/` + `gld/` |

## Iceberg Tables

| Layer | Table | Ozone Path |
|-------|-------|------------|
| Bronze | `spark_catalog.sbi_financial.brnz_transactions` | `.../brnz/transactions` |
| Silver | `spark_catalog.sbi_financial.slvr_transactions` | `.../slvr/transactions` |
| Gold | `spark_catalog.sbi_financial.gld_daily_report` | `.../gld/daily_transaction_report` |

## Ranger (required)

All HDFS, Ozone, and Hive/Iceberg access must be granted via **Apache Ranger** for `systest@...`.  
Do not use filesystem ACLs. See [Ranger Authorization](../operations/ranger-authorization.md) and `governance/configs/security/ranger.yaml`.

## Run (Gateway Node)

### Option A: Airflow (recommended)

```bash
source config/env.conf
export SBI_ENV=prod

bash scripts/airflow/deploy_dags.sh
bash scripts/airflow/import_variables.sh
airflow dags trigger sbi_financial_medallion
```

DAG: `sbi_financial_medallion` — daily 02:00 (configurable via Airflow Variable `schedule_financial_medallion`)

See [Airflow Runbook](../operations/airflow-runbook.md) (CDP Gateway)  
Local install: [Airflow Local Install](../operations/airflow-local-install.md)

### Option B: Manual one-shot

```bash
source config/env.conf
export SBI_ENV=prod

bash scripts/pipeline/run_financial_pipeline.sh
```

### Option C: Step-by-step

```bash
python3 data_gen/generate_financial_json.py --target-gb 10
bash scripts/data/upload_to_hdfs.sh
bash scripts/submit/spark_submit.sh migration \
  --py-file jobs/migration/hdfs_json_to_bronze_job.py \
  --project sbi_financial --job hdfs_json_to_bronze
bash scripts/submit/spark_submit.sh etl \
  --py-file jobs/etl/bronze_to_report_job.py \
  --project sbi_financial --job bronze_to_report
```

## Gold Report Schema

Daily aggregates by `report_date`, `merchant_category`, `channel`, `currency`:

- `transaction_count`
- `total_amount`
- `avg_amount`
- `unique_customers`

Query example (Hue / beeline):

```sql
SELECT report_date, merchant_category, channel,
       transaction_count, total_amount
FROM sbi_financial.gld_daily_report
WHERE report_date >= date_sub(current_date(), 7)
ORDER BY total_amount DESC;
```
