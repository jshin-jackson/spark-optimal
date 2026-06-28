#!/usr/bin/env bash
# Print Cloudera-aligned Ranger policies for Iceberg tables on Ozone.
#
# CDP 7.3.1 requires (see Cloudera docs linked in ranger_iceberg_ozone_pairs.yaml):
#   cm_hive (Hadoop SQL): Storage Handler + SQL table + URL policies
#   cm_ozone: volume/bucket/key policy per table
#
# SBI pair: cm_hive SQL policy {table} + cm_ozone policy {table} (same name)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

ENV_NAME="${SBI_ENV:-dev}"
PAIRS_DOC="docs/operations/ranger-iceberg-ozone-pairs.md"
PAIRS_YAML="governance/configs/security/ranger_iceberg_ozone_pairs.yaml"

echo "=== Iceberg on Ozone — Ranger policies (Cloudera CDP 7.3.1) ==="
echo "SBI_ENV=${ENV_NAME}  principal=${PRINCIPAL:-systest@QE-INFRA-AD.CLOUDERA.COM}"
echo "Inventory: ${PAIRS_YAML}"
echo "Guide:     ${PAIRS_DOC}"
echo

python3 - <<PY
from spark_optimal.governance.security.ranger_pairs import (
    load_ranger_iceberg_ozone_pairs,
    resolve_table_policies,
    verify_pairs_match_medallion,
)

env = "${ENV_NAME}"
cfg = load_ranger_iceberg_ozone_pairs()
hs_svc = cfg["ranger_services"]["hadoop_sql"]
oz_svc = cfg["ranger_services"]["ozone"]
docs = cfg.get("cloudera_documentation", {})

print("=== Cloudera documentation ===")
for key, meta in docs.items():
    print(f"  [{key}] {meta.get('url', '')}")
print()

sh = cfg.get("cluster_policies", {}).get("storage_handler", {})
print("=== Cluster — Hadoop SQL Storage Handler (required once) ===")
print(f"  Service: {hs_svc} (Hadoop SQL)")
print(f"  Policy:  edit default '{sh.get('policy_name')}'")
print(f"  storage-type: {sh.get('storage_type')}  storage-url: {sh.get('storage_url')}  perm: {sh.get('permissions')}")
print(f"  → RW Storage only (CREATE/ALTER location). Does NOT grant data access.")
print()

db = cfg.get("database_policy", {})
print("=== Optional — Hadoop SQL database policy ===")
print(f"  {hs_svc} policy '{db.get('policy_name')}' database={db.get('database')}")
print()

print("=== cm_ozone infrastructure ===")
for pol in cfg.get("infrastructure_ozone", {}).get("policies", []):
    name = pol["policy_name"].replace("{env}", env)
    vol = str(pol.get("volume", "")).replace("{env}", env)
    bkt = pol.get("bucket", "—")
    print(f"  {oz_svc} '{name}' volume={vol} bucket={bkt}")
print()

errors = verify_pairs_match_medallion(env)
if errors:
    for err in errors:
        print(f"ERROR: {err}")
    raise SystemExit(1)

print("=== Per-table policies (register ALL for Spark / Hive / Impala) ===")
print(f"{'Table':<22} {'Type':<18} {'Service':<10} {'Policy name':<28} Resource")
print("-" * 110)
for entry in resolve_table_policies(env):
    t = entry["table_name"]
    for pol in entry["policies"]:
        ptype = pol["policy_type"]
        name = pol["policy_name"]
        svc = pol["ranger_service"]
        if ptype == "sql_table":
            res = f"{pol['database']}.{pol['table']} cols={pol['columns']}"
        elif ptype == "url":
            res = pol["url"]
        else:
            res = f"vol={pol['volume']} bkt={pol['bucket']} key={pol['key']}"
        print(f"{t:<22} {ptype:<18} {svc:<10} {name:<28} {res}")
    opt = entry["optional_policies"][0]
    print(
        f"{t:<22} {'storage_handler':<18} {opt['ranger_service']:<10} "
        f"{opt['policy_name']:<28} iceberg/{opt['storage_url']} (optional)"
    )
    print()

print("SBI pair (same name): cm_hive SQL '{table}' + cm_ozone '{table}'")
print("Also required: cm_hive '{table}-url' + cluster Storage Handler (iceberg, RW Storage)")
print("Principal:", cfg["principal"])
PY
