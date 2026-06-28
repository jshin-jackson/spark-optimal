#!/usr/bin/env bash
# Run Iceberg maintenance on configured financial tables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/config/env.conf" 2>/dev/null || source "${PROJECT_ROOT}/config/env.template.conf"

bash "${PROJECT_ROOT}/scripts/submit/spark_submit.sh" maintenance \
  --py-file "${PROJECT_ROOT}/jobs/maintenance/iceberg_maintenance_job.py" \
  --table spark_catalog.sbi_financial.brnz_transactions \
  --force

bash "${PROJECT_ROOT}/scripts/submit/spark_submit.sh" maintenance \
  --py-file "${PROJECT_ROOT}/jobs/maintenance/iceberg_maintenance_job.py" \
  --table spark_catalog.sbi_financial.slvr_transactions \
  --force
