#!/usr/bin/env bash
# Import Airflow Variables for SBI Spark Optimal.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VAR_FILE="${1:-${PROJECT_ROOT}/airflow/config/variables.template.json}"

if ! command -v airflow >/dev/null 2>&1; then
  echo "ERROR: airflow CLI not found" >&2
  exit 1
fi

airflow variables import "${VAR_FILE}"
echo "Imported variables from ${VAR_FILE}"
airflow variables list | grep -E "sbi_|spark_optimal|financial_|schedule_" || true
