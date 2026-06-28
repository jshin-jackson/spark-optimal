# Airflow Deployment (SBI Spark Optimal)

## DAGs

| DAG ID | Schedule | Description |
|--------|----------|-------------|
| `sbi_financial_medallion` | `0 2 * * *` (daily 02:00) | SDV → HDFS → Bronze → Silver → Gold |
| `sbi_iceberg_maintenance` | `0 3 * * 0` (Sun 03:00) | Iceberg compaction + snapshot expiry |
| `sbi_security_precheck` | Manual | kinit + security check |

## Flow (sbi_financial_medallion)

```mermaid
graph LR
    start[start] --> sec[security_precheck]
    sec --> gen[generate_sdv_json]
    gen --> upload[upload_to_hdfs]
    upload --> bronze[hdfs_json_to_bronze]
    bronze --> gold[bronze_to_silver_gold]
    gold --> metrics[collect_metrics]
    metrics --> endNode[end]
```

## Setup on CDP Gateway / Airflow Host

```bash
# 1. Deploy project to gateway
export SPARK_OPTIMAL_HOME=/opt/spark-optimal

# 2. Configure environment
cp config/env.template.conf config/env.conf
# edit env.conf

# 3. Deploy DAGs
export AIRFLOW_DAGS_DIR=/usr/lib/airflow/dags/spark-optimal
bash scripts/airflow/deploy_dags.sh

# 4. Import Airflow Variables (edit variables.template.json first)
bash scripts/airflow/import_variables.sh

# 5. Verify DAGs
airflow dags list | grep sbi_
airflow dags test sbi_financial_medallion 2026-06-27
```

## Airflow Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `spark_optimal_home` | `/opt/spark-optimal` | Project root on gateway |
| `sbi_env` | `prod` | DEV / UAT / PROD |
| `financial_target_gb` | `10` | SDV generation size |
| `hdfs_financial_raw` | `hdfs://ns1/prod/raw/...` | HDFS raw path |
| `schedule_financial_medallion` | `0 2 * * *` | Cron schedule |
| `schedule_iceberg_maintenance` | `0 3 * * 0` | Weekly maintenance |
| `sbi_alert_email` | — | Failure notification email |

## Kerberos

Each Spark task runs `kinit_manager.sh` via `spark_submit.sh`. The first task `security_precheck` validates Kerberos before data generation.

## Manual Run

Airflow UI → `sbi_financial_medallion` → Trigger DAG

Or CLI:
```bash
airflow dags trigger sbi_financial_medallion
```

## Monitoring

After each run, check task `collect_metrics` output or:
```bash
bash scripts/monitoring/collect_metrics.sh
```

Metrics files:
- `/tmp/spark-optimal-throughput.jsonl`
- `/tmp/spark-optimal-quality.jsonl`
