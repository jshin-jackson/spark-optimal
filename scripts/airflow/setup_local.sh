#!/usr/bin/env bash
# Local Airflow setup for SBI Spark Optimal (dev laptop).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv-airflow"
AIRFLOW_HOME="${AIRFLOW_HOME:-${PROJECT_ROOT}/.airflow-local}"

echo "Project root : ${PROJECT_ROOT}"
echo "AIRFLOW_HOME : ${AIRFLOW_HOME}"
echo "Virtual env  : ${VENV_DIR}"

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

pip install --upgrade pip
pip install -r "${PROJECT_ROOT}/requirements/base.txt"
pip install -r "${PROJECT_ROOT}/requirements/airflow.txt"

mkdir -p "${AIRFLOW_HOME}"
export AIRFLOW_HOME

# Point Airflow to project DAGs (symlink for live reload during dev)
DAG_LINK="${AIRFLOW_HOME}/dags/spark-optimal"
mkdir -p "${AIRFLOW_HOME}/dags"
ln -sfn "${PROJECT_ROOT}/airflow/dags" "${DAG_LINK}"

# Minimal local config
CFG="${AIRFLOW_HOME}/airflow.cfg"
if [[ ! -f "${CFG}" ]]; then
  airflow config list >/dev/null 2>&1 || true
fi

airflow db migrate

VAR_FILE="${PROJECT_ROOT}/airflow/config/variables.local.json"
# Replace placeholder home path with actual project root
sed "s|/Users/jackson/Cloudera/Customer/SBI/spark-optimal|${PROJECT_ROOT}|g" \
  "${VAR_FILE}" > "${AIRFLOW_HOME}/variables.local.json"
airflow variables import "${AIRFLOW_HOME}/variables.local.json" || true

cat <<EOF

=== Local Airflow setup complete ===

1) Activate venv:
   source ${VENV_DIR}/bin/activate
   export AIRFLOW_HOME=${AIRFLOW_HOME}

2) Start Airflow (all-in-one, recommended for local):
   airflow standalone

   UI: http://localhost:8080
   Default login is printed in the terminal output.

3) Or start scheduler + webserver separately:
   airflow webserver --port 8080 &
   airflow scheduler

4) Verify DAGs:
   airflow dags list | grep sbi_

5) Dry-run a task (no cluster required for parse test):
   airflow dags test sbi_security_precheck 2026-01-01

Note: Spark/HDFS tasks require CDP gateway + Kerberos.
      Use local Airflow for DAG development and scheduling validation.

EOF
