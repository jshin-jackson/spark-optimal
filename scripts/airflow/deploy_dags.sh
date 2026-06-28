#!/usr/bin/env bash
# Deploy SBI Airflow DAGs to CDP Airflow dags folder.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Override: export AIRFLOW_DAGS_DIR=/usr/lib/airflow/dags
AIRFLOW_DAGS_DIR="${AIRFLOW_DAGS_DIR:-/usr/lib/airflow/dags/spark-optimal}"

echo "Deploying DAGs to ${AIRFLOW_DAGS_DIR}"
mkdir -p "${AIRFLOW_DAGS_DIR}"

rsync -av --delete \
  "${PROJECT_ROOT}/airflow/dags/" \
  "${AIRFLOW_DAGS_DIR}/"

echo "DAG deployment complete."
echo "Set Airflow Variables from template:"
echo "  airflow variables import ${PROJECT_ROOT}/airflow/config/variables.template.json"
