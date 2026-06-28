#!/usr/bin/env bash
# Verify ICEBERG_JAR and SPARK_OZONE_JARS exist after sourcing env.conf.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
else
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.template.conf"
fi

fail=0
check_file() {
  local path="$1"
  local label="$2"
  if [[ -f "${path}" ]]; then
    echo "OK  ${label}: ${path}"
  else
    echo "MISSING ${label}: ${path}" >&2
    fail=1
  fi
}

check_file "${ICEBERG_JAR:?ICEBERG_JAR not set}" "ICEBERG_JAR"
IFS=':' read -r -a ozone_jars <<< "${SPARK_OZONE_JARS:?SPARK_OZONE_JARS not set}"
for jar in "${ozone_jars[@]}"; do
  check_file "${jar}" "SPARK_OZONE_JARS"
done

if [[ "${fail}" -ne 0 ]]; then
  echo >&2
  echo "Fix paths in config/env.conf — list available JARs:" >&2
  echo "  ls /opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime*.jar" >&2
  echo "  ls /opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3*.jar" >&2
  exit 1
fi

echo "All Spark dependency JARs found."
