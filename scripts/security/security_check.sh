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
RANGER_PAIRS_DOC="docs/operations/ranger-iceberg-ozone-pairs.md"
RANGER_MD="docs/operations/ranger-authorization.md"
HDFS_ENC_DOC="docs/operations/hdfs-encryption.md"
OZONE_ENC_DOC="docs/operations/ozone-encryption.md"
HDFS_KEY="${HDFS_ENCRYPTION_KEY:-hdfs_encryption_key}"
OZONE_KEY="${OZONE_ENCRYPTION_KEY:-ozone_encryption_key}"
OZONE_VOL="${OZONE_VOLUME:-${SBI_ENV:-dev}}"
OZONE_BKT="${OZONE_BUCKET:-data}"
HDFS_RAW="${HDFS_FINANCIAL_RAW:-}"

_uri_to_hdfs_path() {
  local uri="$1"
  if [[ "${uri}" =~ ^hdfs://[^/]+(/.*)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "${uri}" =~ ^hdfs://[^/]+$ ]]; then
    echo "/"
  else
    echo "${uri}"
  fi
}

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
echo "HDFS_FINANCIAL_RAW=${HDFS_RAW:-unset}"
echo "OZONE_MEDALLION_BRNZ=${OZONE_MEDALLION_BRNZ:-unset}"
echo "HMS_HOST=${HMS_HOST:-unset}"
echo "HMS_HOSTS=${HMS_HOSTS:-unset}"
echo "HMS_URIS=${HMS_URIS:-unset}"
echo "FIN_DB=${FIN_DB:-unset}"
echo "SPARK_CONF_DIR=${SPARK_CONF_DIR:-unset}"
echo "HDFS_ENCRYPTION_KEY=${HDFS_KEY}"
echo "OZONE_ENCRYPTION_KEY=${OZONE_KEY}"
echo "KMS_PROVIDER_URI=${KMS_PROVIDER_URI:-unset}"
echo "OZONE_VOLUME=${OZONE_VOL} OZONE_BUCKET=${OZONE_BKT}"

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

_check_hdfs_encryption_zone() {
  local uri="$1"
  local expected_key="$2"
  if [[ -z "${uri}" ]]; then
    echo "SKIP HDFS EZ: HDFS_FINANCIAL_RAW unset"
    return 0
  fi
  if ! command -v hdfs >/dev/null 2>&1; then
    echo "SKIP HDFS EZ: hdfs CLI not found"
    return 0
  fi

  local path zone_key
  path="$(_uri_to_hdfs_path "${uri}")"
  zone_key="$(hdfs crypto -getZone "${path}" 2>/dev/null | awk '/Key Name/ {print $NF; exit}' || true)"

  if [[ -z "${zone_key}" ]]; then
    echo "WARN HDFS Encryption Zone not found for ${path}" >&2
    echo "      Run scripts/infrastructure/setup_hdfs_encryption_zone.sh" >&2
    echo "      See ${HDFS_ENC_DOC}" >&2
    return 1
  fi
  if [[ "${zone_key}" == "${expected_key}" ]]; then
    echo "OK  HDFS Encryption Zone: ${path} (key=${expected_key})"
  else
    echo "WARN HDFS Encryption Zone ${path} uses key '${zone_key}' (expected ${expected_key})" >&2
    return 1
  fi
  return 0
}

_check_kms_key() {
  local key_name="$1"
  local doc="$2"
  if ! command -v hadoop >/dev/null 2>&1; then
    echo "SKIP hadoop key list (${key_name}): hadoop CLI not found"
    return 0
  fi
  if hadoop key list 2>/dev/null | grep -q "${key_name}"; then
    echo "OK  Ranger KMS key listed: ${key_name}"
    return 0
  fi
  echo "ERROR: encryption key '${key_name}' not found (hadoop key list)" >&2
  echo "       Create in Ranger KMS (cm_kms). See ${doc}" >&2
  return 1
}

echo
echo "=== Ranger authorization probes (HDFS / Ozone via hdfs dfs) ==="
echo "Note: Hive/Iceberg authz is enforced by Ranger at Spark runtime (see ${RANGER_DOC})."

fail=0
_check_ranger_path "HDFS raw (Ranger)" "${HDFS_RAW}" || fail=1
_check_ranger_path "Ozone Bronze table data (Ranger cm_ozone)" "${OZONE_MEDALLION_BRNZ:-}/transactions" || fail=1

echo
echo "=== Iceberg on Ozone — Ranger policies (Cloudera CDP 7.3.1) ==="
echo "Per table: cm_hive Storage Handler + SQL {table} + URL {table}-url + cm_ozone {table}"
echo "SBI pair: cm_hive SQL + cm_ozone share the same policy name (= table name)"
echo "Inventory: governance/configs/security/ranger_iceberg_ozone_pairs.yaml"
echo "Print:     bash scripts/security/print_ranger_iceberg_pairs.sh"
echo "Guide:     ${RANGER_PAIRS_DOC}"
if command -v python3 >/dev/null 2>&1; then
  PAIR_ERR="$(python3 - <<'PY' 2>&1 || true
import os
os.environ.setdefault("SBI_ENV", os.environ.get("SBI_ENV", "dev"))
from spark_optimal.governance.security.ranger_pairs import verify_pairs_match_medallion
errs = verify_pairs_match_medallion()
if errs:
    for e in errs:
        print(e)
    raise SystemExit(1)
print("OK  paired Ozone paths match medallion table locations")
PY
)"
  if echo "${PAIR_ERR}" | grep -q "^OK"; then
    echo "${PAIR_ERR}"
  else
    echo "WARN paired-policy config check:" >&2
    echo "${PAIR_ERR}" >&2
  fi
else
  echo "SKIP paired-policy config check: python3 not found"
fi

echo
echo "=== Ranger KMS TDE (cm_kms) ==="
if [[ -z "${KMS_PROVIDER_URI:-}" ]]; then
  echo "WARN KMS_PROVIDER_URI unset — set HADOOP_CONF_DIR then hdfs getconf in env.conf" >&2
  fail=1
else
  echo "OK  KMS_PROVIDER_URI=${KMS_PROVIDER_URI}"
fi

_check_kms_key "${HDFS_KEY}" "${HDFS_ENC_DOC}" || fail=1
_check_kms_key "${OZONE_KEY}" "${OZONE_ENC_DOC}" || fail=1

echo
echo "=== HDFS Encryption Zone ==="
_check_hdfs_encryption_zone "${HDFS_RAW}" "${HDFS_KEY}" || fail=1

echo
echo "=== Ozone encrypted bucket ==="
if command -v ozone >/dev/null 2>&1; then
  if ozone sh bucket info --volume "${OZONE_VOL}" --bucket "${OZONE_BKT}" >/dev/null 2>&1; then
    echo "OK  Ozone bucket exists: ${OZONE_VOL}/${OZONE_BKT}"
    echo "    Medallion data must use bucket created with --bucketkey ${OZONE_KEY}"
  else
    echo "WARN Ozone bucket ${OZONE_VOL}/${OZONE_BKT} not found — run scripts/infrastructure/setup_ozone_encrypted_bucket.sh"
  fi
else
  echo "SKIP ozone bucket info: ozone CLI not found"
fi

if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi

echo
echo "Security check completed (Kerberos OK, Ranger + HDFS/Ozone encryption checks OK or warn-only)."
