#!/usr/bin/env bash
# Print DEV Jenkins / CloudCat build parameters for copy-paste into Jenkins.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_YAML="${PROJECT_ROOT}/governance/configs/infrastructure/dev_jenkins_build.yaml"

if [[ ! -f "${BUILD_YAML}" ]]; then
  echo "ERROR: not found: ${BUILD_YAML}" >&2
  exit 1
fi

python3 << PY
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML required (pip install pyyaml)", file=sys.stderr)
    sys.exit(1)

path = Path("${BUILD_YAML}")
data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
j = data.get("jenkins", {})

labels = [
    ("CLUSTER_SHORTNAME", j.get("cluster_shortname", "")),
    ("CM_VERSION", j.get("cm_version", "")),
    ("CDH", j.get("cdh_version", "")),
    ("CLOUDCAT_OS", j.get("cloudcat_os", "")),
    ("CLOUDCAT_BUDGET", j.get("cloudcat_budget", "")),
    ("DB", j.get("database", "")),
    ("KERBEROS", j.get("kerberos", "")),
    ("JAVA_VERSION", j.get("java_version", "")),
    ("OPTIONAL_ARGS", j.get("optional_args", "").replace("\n", " ").strip()),
]

print("# DEV Jenkins / CloudCat parameters")
print(f"# Source: {path.relative_to(Path('${PROJECT_ROOT}').resolve())}")
print()
for name, value in labels:
    print(f"{name}={value}")

hosts = j.get("cluster_hosts") or []
if hosts:
    print()
    print("# CLUSTER_HOSTS")
    for h in hosts:
        print(f"#   {h}")
PY
