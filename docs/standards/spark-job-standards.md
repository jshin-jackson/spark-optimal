# Spark Job Standards (SBI)

## Naming

- App name: `{project}_{job}` via `SparkSessionBuilder(project, job)`
- YARN queue: from workload profile + department policy

## Security (Gateway)

1. `kinit -kt /opt/cloudera/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM`
2. Use `scripts/submit/spark_submit.sh` — never raw spark-submit without delegation token settings
3. Deploy mode: **cluster** from gateway

## Resource Sizing

| Workload | Profile | Sizing Input |
|----------|---------|--------------|
| Migration | `migration` | `--data-size-gb` |
| ETL | `etl_standard` / `etl_heavy` | data size + join flag |
| Analytics | `analytical` | compute-heavy flag |

Use `PerformanceAnalyzer` before production runs for tuning recommendations.

## Medallion Layout

```
ofs://ozone1782570080/prod/data/brnz/   # raw ingest
ofs://ozone1782570080/prod/data/slvr/   # cleansed
ofs://ozone1782570080/prod/data/gld/    # report-ready
```

## Iceberg Maintenance

- Compaction when file count > 100
- Snapshot expiry retain_last=30
- Run `scripts/maintenance/run_iceberg_maintenance.sh` on schedule
