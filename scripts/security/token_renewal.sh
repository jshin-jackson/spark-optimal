#!/usr/bin/env bash
# Re-authenticate before long-running Spark jobs (delegation token refresh via new kinit).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "${SCRIPT_DIR}/kinit_manager.sh" --force
echo "Kerberos ticket renewed. Spark will collect fresh delegation tokens on next submit."
