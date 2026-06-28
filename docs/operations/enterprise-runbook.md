# Enterprise Operations Runbook

## Daily Checklist

1. Verify Kerberos + Ranger path access: `bash scripts/security/security_check.sh`
2. Review metrics: `bash scripts/monitoring/collect_metrics.sh`
3. Check YARN queue utilization for `migration`, `etl`, `fraud` queues

## Job Submission

```bash
source config/env.conf
export SBI_ENV=prod
bash scripts/deployment/package_python.sh
bash scripts/submit/spark_submit.sh migration --project sbi_financial --job <name> ...
```

## Financial Medallion Pipeline

Full pipeline: `bash scripts/pipeline/run_financial_pipeline.sh`

## Maintenance Window

```bash
bash scripts/maintenance/run_iceberg_maintenance.sh
```

## Escalation

| Severity | Condition | Action |
|----------|-----------|--------|
| P1 | PROD SLA breach > 30 min | Kill stuck jobs, re-run with reduced parallelism |
| P2 | Data quality score < 0.95 | Block downstream; investigate bronze layer |
| P3 | Iceberg file count > 200 | Schedule compaction |

See [troubleshooting/common-issues.md](../troubleshooting/common-issues.md).
