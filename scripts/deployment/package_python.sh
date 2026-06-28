#!/usr/bin/env bash
# Package Python modules for YARN cluster-mode distribution.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DIST_DIR="${PROJECT_ROOT}/dist"
ZIP_FILE="${DIST_DIR}/spark_optimal.zip"

mkdir -p "${DIST_DIR}"
rm -f "${ZIP_FILE}"

(
  cd "${PROJECT_ROOT}"
  zip -r "${ZIP_FILE}" spark_optimal -x "*.pyc" -x "*__pycache__*"
)

echo "Created ${ZIP_FILE}"
