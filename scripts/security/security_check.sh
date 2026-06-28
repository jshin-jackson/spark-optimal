#!/usr/bin/env bash
# Verify Kerberos ticket and Ranger-backed access (HDFS, Ozone) on gateway node.
#
# Authentication: Kerberos (kinit / systest keytab)
# Authorization:  Apache Ranger ONLY — do not use chmod/chown/setfacl for access control.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

RANGER_DOC="governance/configs/security/ranger.yaml"
RANGER_MD="docs/operations/ranger-authorization.md"

echo "=== Kerberos ticket (authentication) ==="
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
echo "SBI_ENV=${SBI_ENV:-unset}"
echo "HDFS_URI=${HDFS_URI:-unset}"
echo "OFS_URI=${OFS_URI:-unset}"
echo "HDFS_FINANCIAL_RAW=${HDFS_FINANCIAL_RAW:-unset}"
echo "OZONE_MEDALLION_BRNZ=${OZONE_MEDALLION_BRNZ:-unset}"
echo "HMS_HOST=${HMS_HOST:-unset}"
echo "HMS_HOSTS=${HMS_HOSTS:-unset}"
echo "HMS_URIS=${HMS_URIS:-unset}"
echo "FIN_DB=${FIN_DB:-unset}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR:-unset}"

_ranger_fail() {
  local service="$1"
  local path="$2"
  local detail="$3"
  echo "ERROR: Ranger authorization check failed for ${service}" >&2
  echo "       Path: ${path}" >&2
  echo "       Principal: ${PRINCIPAL:-unset} (Kerberos ticket is valid)" >&2
  echo "       Detail: ${detail}" >&2
  echo "       SBI policy: HDFS/HMS/Spark/Ozone access is Ranger-only." >&2
  echo "       Do NOT use chmod/chown/setfacl — request a Ranger policy update." >&2
  echo "       Inventory: ${RANGER_DOC}" >&2
  echo "       Guide:     ${RANGER_MD}" >&2
  return 1
}

_check_ranger_path() {
  local service="$1"
  local path="$2"

  if [[ -z "${path}" || "${path}" == "unset" ]]; then
    echo "SKIP ${service}: path unset"
    return 0
  fi
  if ! command -v hdfs >/dev/null 2>&1; then
    echo "SKIP ${service}: hdfs CLI not found"
    return 0
  fi

  local output
  output="$(hdfs dfs -ls "${path}" 2>&1)" || true

  if echo "${output}" | grep -qiE 'Permission denied|AccessControlException|authorization failed|not authorized'; then
    _ranger_fail "${service}" "${path}" "${output}"
    return 1
  fi
  if echo "${output}" | grep -qiE 'No such file or directory|does not exist|FileNotFoundException'; then
    echo "WARN ${service}: path not found (OK if Ranger write policy exists for parent create): ${path}"
    return 0
  fi

  echo "OK  ${service}: ${path}"
  echo "${output}" | head -3
  return 0
}

echo
echo "=== Ranger authorization probes (HDFS / Ozone via hdfs dfs) ==="
echo "Note: Hive/Iceberg authz is enforced by Ranger at Spark runtime (see ${RANGER_DOC})."

fail=0
_check_ranger_path "HDFS raw (Ranger)" "${HDFS_FINANCIAL_RAW:-}" || fail=1
_check_ranger_path "Ozone Bronze prefix (Ranger)" "${OZONE_MEDALLION_BRNZ:-}" || fail=1

if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi

echo
echo "Security check completed (Kerberos OK, Ranger path probes OK or warn-only)."
