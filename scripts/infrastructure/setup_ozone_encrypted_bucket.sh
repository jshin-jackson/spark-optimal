#!/usr/bin/env bash
# Create Ozone volume + encrypted bucket for Medallion (Ranger KMS TDE).
#
# All Ozone Medallion data uses encryption key: ozone_encryption_key (Ranger KMS / cm_kms).
# Bucket encryption MUST be set at creation via --bucketkey.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

ENV_NAME="${SBI_ENV:-dev}"
VOLUME="${OZONE_VOLUME:-${ENV_NAME}}"
BUCKET="${OZONE_BUCKET:-data}"
KEY="${OZONE_ENCRYPTION_KEY:-ozone_encryption_key}"
KMS_SERVICE="${RANGER_KMS_SERVICE:-cm_kms}"

echo "=== Ozone encrypted bucket setup ==="
echo "SBI_ENV=${ENV_NAME} volume=${VOLUME} bucket=${BUCKET} key=${KEY} kms=${KMS_SERVICE}"

bash "${PROJECT_ROOT}/scripts/security/kinit_manager.sh"

if ! command -v ozone >/dev/null 2>&1; then
  echo "ERROR: ozone CLI not found" >&2
  exit 1
fi

echo
echo "=== Ranger KMS encryption key ==="
if command -v hadoop >/dev/null 2>&1; then
  if hadoop key list 2>/dev/null | grep -q "${KEY}"; then
    echo "OK  hadoop key list contains: ${KEY}"
  else
    echo "ERROR: encryption key '${KEY}' not found in KMS (service ${KMS_SERVICE})" >&2
    echo "       Create in Ranger KMS UI, then grant OM + systest permissions." >&2
    echo "       See docs/operations/ozone-encryption.md" >&2
    exit 1
  fi
else
  echo "WARN: hadoop CLI not found; skipping key list check"
fi

echo
echo "=== Ozone volume ==="
if ozone sh volume info "${VOLUME}" >/dev/null 2>&1; then
  echo "OK  volume exists: ${VOLUME}"
else
  echo "Creating volume: ${VOLUME} (Ranger Ozone policy required)"
  ozone sh volume create "${VOLUME}"
fi

echo
echo "=== Ozone encrypted bucket ==="
if ozone sh bucket info --volume "${VOLUME}" --bucket "${BUCKET}" >/dev/null 2>&1; then
  echo "OK  bucket exists: ${VOLUME}/${BUCKET}"
  if ozone sh bucket info --volume "${VOLUME}" --bucket "${BUCKET}" 2>&1 | grep -qi "encryption"; then
    echo "    (bucket info mentions encryption — verify key is ${KEY})"
  else
    echo "WARN: bucket exists but encryption status unclear." >&2
    echo "      If bucket was created WITHOUT --bucketkey, recreate it with:" >&2
    echo "      ozone sh bucket delete --volume ${VOLUME} --bucket ${BUCKET}" >&2
    echo "      ozone sh bucket create --volume ${VOLUME} --bucket ${BUCKET} --bucketkey ${KEY}" >&2
  fi
else
  echo "Creating encrypted bucket: ${VOLUME}/${BUCKET} with key ${KEY}"
  ozone sh bucket create --volume "${VOLUME}" --bucket "${BUCKET}" --bucketkey "${KEY}"
  echo "OK  encrypted bucket created"
fi

echo
echo "Medallion prefixes (encrypted at rest):"
echo "  ${OZONE_MEDALLION_BRNZ:-${OFS_URI}/${ENV_NAME}/data/brnz}"
echo "  ${OZONE_MEDALLION_SLVR:-${OFS_URI}/${ENV_NAME}/data/slvr}"
echo "  ${OZONE_MEDALLION_GLD:-${OFS_URI}/${ENV_NAME}/data/gld}"
echo
echo "Setup complete."
