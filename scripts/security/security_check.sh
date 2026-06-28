#!/usr/bin/env bash
# Verify Kerberos ticket and Hadoop delegation token readiness on gateway node.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

echo "=== Kerberos ticket ==="
if klist -s; then
  klist
else
  echo "ERROR: No valid Kerberos ticket. Run scripts/security/kinit_manager.sh" >&2
  exit 1
fi

echo
echo "=== Environment ==="
echo "PRINCIPAL=${PRINCIPAL:-unset}"
echo "KEYTAB=${KEYTAB:-unset}"
echo "HDFS_URI=${HDFS_URI:-unset}"
echo "OFS_URI=${OFS_URI:-unset}"
echo "HMS_HOST=${HMS_HOST:-unset}"
echo "HMS_HOSTS=${HMS_HOSTS:-unset}"
echo "HMS_URIS=${HMS_URIS:-unset}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR:-unset}"

echo
echo "=== Hadoop filesystem access (delegation token test) ==="
if command -v hdfs >/dev/null 2>&1; then
  hdfs dfs -ls "${HDFS_URI:-/}" 2>/dev/null | head -5 || echo "WARN: hdfs dfs check failed"
else
  echo "hdfs CLI not found; skipping HDFS check"
fi

echo
echo "Security check completed."
