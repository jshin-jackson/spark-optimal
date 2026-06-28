# Troubleshooting Guide

## Kerberos / SASL

| Symptom | Fix |
|---------|-----|
| `GSS initiate failed` | `bash scripts/security/kinit_manager.sh --force` |
| `Invalid token` | Renew ticket: `bash scripts/security/token_renewal.sh` |
| Executor cannot read OFS | Verify `spark.yarn.access.hadoopFileSystems=hdfs://ns1,ofs://ozone1782570080` |

## Spark / YARN

| Symptom | Fix |
|---------|-----|
| App stuck in ACCEPTED | Check YARN queue capacity; reduce executors |
| OOM / spill | Increase `spark.executor.memoryOverhead`; raise shuffle partitions |
| Stuck application | `bash scripts/emergency/kill_stuck_jobs.sh sbi_financial --kill` |

## Iceberg

| Symptom | Fix |
|---------|-----|
| Slow queries, many small files | Run compaction via maintenance job |
| HMS connection failure | Verify `HMS_URIS` and Kerberos principal |
| Row count mismatch after migration | Re-run with validation; check JSON multiline=false |

## Data Quality

- Check `/tmp/spark-optimal-quality.jsonl`
- Run `bash scripts/monitoring/collect_metrics.sh`
