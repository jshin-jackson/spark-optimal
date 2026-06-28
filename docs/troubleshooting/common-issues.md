# Troubleshooting Guide

## Ranger / Authorization

SBI: HDFS, HMS, Spark, Ozone access is **Ranger-only**. Do not use `chmod`, `chown`, or `setfacl`.

| Symptom | Fix |
|---------|-----|
| `Permission denied` on `hdfs dfs` (valid `klist`) | Add Ranger **HDFS** policy for `${PRINCIPAL}` on path in `governance/configs/security/ranger.yaml` |
| Executor OFS / Iceberg write failure | Ranger **Ozone** + **Hive** policies for medallion paths and `sbi_financial` |
| `security_check.sh` Ranger probe fails | Platform team: Ranger policy request per [ranger-authorization.md](../operations/ranger-authorization.md) |

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
