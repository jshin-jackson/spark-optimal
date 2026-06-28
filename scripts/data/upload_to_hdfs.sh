#!/usr/bin/env bash
# Step 2: Upload generated financial JSONL files to HDFS.
# Target path MUST be inside an HDFS Encryption Zone (Ranger KMS key hdfs_encryption_key).
# Run first: bash scripts/infrastructure/setup_hdfs_encryption_zone.sh
# HDFS write requires Ranger HDFS policy for ${PRINCIPAL} on ${HDFS_TARGET} — not chmod/chown.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

LOCAL_DIR="${1:-${PROJECT_ROOT}/data/output/financial}"
HDFS_TARGET="${2:-${HDFS_FINANCIAL_RAW:-${HDFS_URI}/${SBI_ENV:-dev}/raw/financial/transactions}}"

if [[ ! -d "${LOCAL_DIR}" ]]; then
  echo "ERROR: local directory not found: ${LOCAL_DIR}" >&2
  echo "Run Step 1 first: python data_gen/generate_financial_json.py" >&2
  exit 1
fi

bash "${PROJECT_ROOT}/scripts/security/kinit_manager.sh"

echo "Uploading ${LOCAL_DIR} -> ${HDFS_TARGET}"
if ! hdfs dfs -mkdir -p "${HDFS_TARGET}"; then
  echo "ERROR: HDFS mkdir failed. With valid kinit, this is usually a Ranger HDFS policy gap." >&2
  echo "       See governance/configs/security/ranger.yaml" >&2
  exit 1
fi
if ! hdfs dfs -put -f "${LOCAL_DIR}"/*.jsonl "${HDFS_TARGET}/"; then
  echo "ERROR: HDFS put failed. Check Ranger write policy for ${HDFS_TARGET}" >&2
  exit 1
fi

echo "Upload complete."
hdfs dfs -du -h "${HDFS_TARGET}" | tail -5
hdfs dfs -ls "${HDFS_TARGET}" | head -10
