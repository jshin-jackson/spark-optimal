#!/usr/bin/env bash
# Emergency: list and optionally kill stuck YARN Spark applications.
set -euo pipefail

APP_FILTER="${1:-sbi_financial}"

echo "Stuck / running Spark apps matching: ${APP_FILTER}"
yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep -i "${APP_FILTER}" || true

if [[ "${2:-}" == "--kill" ]]; then
  yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null \
    | awk -v f="${APP_FILTER}" '$1 ~ /^application_/ && index(tolower($0), tolower(f)) {print $1}' \
    | while read -r app_id; do
        echo "Killing ${app_id}"
        yarn application -kill "${app_id}"
      done
fi
