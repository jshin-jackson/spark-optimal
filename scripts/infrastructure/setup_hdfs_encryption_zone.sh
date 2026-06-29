#!/usr/bin/env bash
# Create HDFS Encryption Zones for spark-optimal raw paths (Ranger KMS TDE).
#
# Financial raw JSON and (optionally) Spark event log use Ranger KMS key:
#   hdfs_encryption_key (service cm_kms)
#
# Encryption Zone MUST be created on an empty directory. Run before first upload.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

ENV_NAME="${SBI_ENV:-dev}"
KEY="${HDFS_ENCRYPTION_KEY:-hdfs_encryption_key}"
KMS_SERVICE="${RANGER_KMS_SERVICE:-cm_kms}"
HDFS_RAW_URI="${HDFS_FINANCIAL_RAW:-hdfs://ns1/${ENV_NAME}/data/migration/upload}"
SPARK_HISTORY_URI="${HDFS_SPARK_HISTORY:-hdfs:///user/spark/applicationHistory}"
SETUP_SPARK_HISTORY="${SETUP_HDFS_SPARK_HISTORY:-1}"

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

_get_zone_key() {
  local path="$1"
  hdfs crypto -getZone "${path}" 2>/dev/null | awk '/Key Name/ {print $NF; exit}'
}

_ensure_encryption_zone() {
  local label="$1"
  local uri="$2"
  local required="${3:-1}"
  local path
  path="$(_uri_to_hdfs_path "${uri}")"

  echo "=== ${label} ==="
  echo "    URI:  ${uri}"
  echo "    Path: ${path}"

  local existing_key
  existing_key="$(_get_zone_key "${path}" || true)"
  if [[ -n "${existing_key}" ]]; then
    if [[ "${existing_key}" == "${KEY}" ]]; then
      echo "OK  Encryption Zone exists with key: ${KEY}"
    else
      echo "WARN: Encryption Zone exists but key is '${existing_key}' (expected ${KEY})" >&2
    fi
    return 0
  fi

  echo "    Creating parent directory (Ranger HDFS policy required)..."
  hdfs dfs -mkdir -p "${path}" || {
    echo "ERROR: hdfs dfs -mkdir failed for ${path}" >&2
    return 1
  }

  local file_count
  file_count="$(hdfs dfs -count "${path}" 2>/dev/null | awk '{print $2}' || echo "0")"
  if [[ "${file_count}" != "0" ]]; then
    echo "ERROR: ${path} is not empty (${file_count} files). EZ requires an empty directory." >&2
    echo "       Move data out, create EZ, then re-upload. See docs/operations/hdfs-encryption.md" >&2
    return 1
  fi

  echo "    Creating Encryption Zone with key ${KEY}..."
  if hdfs crypto -createZone -keyName "${KEY}" -path "${path}"; then
    echo "OK  Encryption Zone created: ${path} (key=${KEY})"
    return 0
  fi

  echo "ERROR: hdfs crypto -createZone failed for ${path}" >&2
  echo "       Requires HDFS admin / superuser or Ranger HDFS crypto policy." >&2
  echo "       Verify Ranger KMS ACL for hdfs (NameNode) + ${KEY}. See docs/operations/hdfs-encryption.md" >&2
  if [[ "${required}" == "1" ]]; then
    return 1
  fi
  echo "WARN: optional zone skipped"
  return 0
}

echo "=== HDFS Encryption Zone setup ==="
echo "SBI_ENV=${ENV_NAME} key=${KEY} kms=${KMS_SERVICE}"

bash "${PROJECT_ROOT}/scripts/security/kinit_manager.sh"

if ! command -v hdfs >/dev/null 2>&1; then
  echo "ERROR: hdfs CLI not found" >&2
  exit 1
fi

echo
echo "=== Ranger KMS encryption key ==="
if command -v hadoop >/dev/null 2>&1; then
  if hadoop key list 2>/dev/null | grep -q "${KEY}"; then
    echo "OK  hadoop key list contains: ${KEY}"
  else
    echo "ERROR: encryption key '${KEY}' not found in KMS (service ${KMS_SERVICE})" >&2
    echo "       Create in Ranger KMS UI, then grant hdfs + systest permissions." >&2
    echo "       See docs/operations/hdfs-encryption.md" >&2
    exit 1
  fi
else
  echo "WARN: hadoop CLI not found; skipping key list check"
fi

fail=0
_ensure_encryption_zone "Financial raw (required)" "${HDFS_RAW_URI}" 1 || fail=1

if [[ "${SETUP_SPARK_HISTORY}" == "1" ]]; then
  echo
  _ensure_encryption_zone "Spark event log (optional)" "${SPARK_HISTORY_URI}" 0 || fail=1
fi

if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi

echo
echo "Encrypted HDFS paths:"
echo "  ${HDFS_RAW_URI}"
if [[ "${SETUP_SPARK_HISTORY}" == "1" ]]; then
  echo "  ${SPARK_HISTORY_URI}"
fi
echo
echo "Setup complete."
