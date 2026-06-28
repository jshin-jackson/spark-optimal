#!/usr/bin/env bash
# Collect SLA and throughput metrics from gateway logs.
set -euo pipefail

echo "=== SLA / Throughput metrics ==="
for f in /tmp/spark-optimal-throughput.jsonl /tmp/spark-optimal-quality.jsonl /tmp/spark-optimal-quality-metrics.jsonl; do
  if [[ -f "$f" ]]; then
    echo "--- $f (last 5) ---"
    tail -5 "$f"
  else
    echo "--- $f not found ---"
  fi
done
