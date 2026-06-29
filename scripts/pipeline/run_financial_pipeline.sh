#!/usr/bin/env bash
# =============================================================================
# 금융 Medallion 파이프라인 — End-to-End 실행 스크립트 (Step 1~4)
# =============================================================================
#
# [목적]
#   SDV JSON 생성 → HDFS 업로드 → Bronze → Silver → Gold 전체 파이프라인을
#   한 번에 실행합니다. Airflow 없이 수동 테스트할 때 사용합니다.
#
# [4단계 흐름]
#   Step 1: SDV로 ~10GB 금융 JSON 생성 (로컬 디스크)
#   Step 2: 생성된 JSONL을 HDFS raw 경로에 업로드
#   Step 3: Spark — HDFS JSON → Ozone Bronze Iceberg (brnz_transactions)
#   Step 4: Spark — Bronze → Silver → Gold 일별 리포트 (gld_daily_report)
#
# [사용법 — Gateway Node]
#   cp config/env.template.conf config/env.conf   # 최초 1회
#   bash scripts/pipeline/run_financial_pipeline.sh
#
# [환경 변수로 단계 건너뛰기]
#   SKIP_GENERATE=true bash scripts/pipeline/run_financial_pipeline.sh  # Step 1 생략
#   SKIP_UPLOAD=true   bash scripts/pipeline/run_financial_pipeline.sh  # Step 2 생략
#   TARGET_GB=0.1      bash scripts/pipeline/run_financial_pipeline.sh  # 100MB 테스트
#
# [Airflow 스케줄 실행]
#   bash scripts/airflow/deploy_dags.sh
#   airflow dags trigger sbi_financial_medallion
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Kerberos, HDFS, Ozone 주소 등 로드
if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

export SBI_ENV="${SBI_ENV:-prod}"
export SPARK_CONF_DIR="${PROJECT_ROOT}/conf/${SBI_ENV}"

# 기본값: 10GB SDV, 로컬 출력 경로, HDFS raw 경로
TARGET_GB="${TARGET_GB:-10}"
LOCAL_OUTPUT="${LOCAL_OUTPUT:-${PROJECT_ROOT}/data/output/financial}"
HDFS_RAW="${HDFS_FINANCIAL_RAW:-${HDFS_URI}/${SBI_ENV}/data/brnz/transactions}"
SKIP_GENERATE="${SKIP_GENERATE:-false}"
SKIP_UPLOAD="${SKIP_UPLOAD:-false}"

# ── Step 1: SDV 금융 JSON 생성 ──
echo "=== Step 1: SDV financial JSON generation (~${TARGET_GB} GB) ==="
if [[ "${SKIP_GENERATE}" != "true" ]]; then
  pip install -q -r "${PROJECT_ROOT}/requirements/financial.txt"
  python3 "${PROJECT_ROOT}/data_gen/generate_financial_json.py" \
    --output-dir "${LOCAL_OUTPUT}" \
    --target-gb "${TARGET_GB}"
else
  echo "Skipped (SKIP_GENERATE=true)"
fi

# ── Step 2: HDFS 업로드 ──
echo "=== Step 2: Upload to HDFS ==="
if [[ "${SKIP_UPLOAD}" != "true" ]]; then
  bash "${PROJECT_ROOT}/scripts/data/upload_to_hdfs.sh" "${LOCAL_OUTPUT}" "${HDFS_RAW}"
else
  echo "Skipped (SKIP_UPLOAD=true)"
fi

# ── Step 3: HDFS → Ozone Bronze (Iceberg) ──
echo "=== Step 3: HDFS JSON -> Ozone Bronze (brnz) ==="
bash "${PROJECT_ROOT}/scripts/submit/spark_submit.sh" migration \
  --py-file "${PROJECT_ROOT}/jobs/migration/hdfs_json_to_bronze_job.py" \
  --project sbi_financial \
  --job hdfs_json_to_bronze \
  --source-path "${HDFS_RAW}" \
  --data-size-gb "${TARGET_GB}"

# ── Step 4: Bronze → Silver → Gold ──
echo "=== Step 4: Bronze -> Silver -> Gold report ==="
bash "${PROJECT_ROOT}/scripts/submit/spark_submit.sh" etl \
  --py-file "${PROJECT_ROOT}/jobs/etl/bronze_to_report_job.py" \
  --project sbi_financial \
  --job bronze_to_report \
  --data-size-gb "${TARGET_GB}"

echo "=== Pipeline complete ==="
echo "Bronze: spark_catalog.sbi_financial.brnz_transactions @ ${OZONE_MEDALLION_BRNZ:-${OFS_URI}/${SBI_ENV}/data/brnz}/transactions"
echo "Silver: spark_catalog.sbi_financial.slvr_transactions @ ${OZONE_MEDALLION_SLVR:-${OFS_URI}/${SBI_ENV}/data/slvr}/transactions"
echo "Gold:   spark_catalog.sbi_financial.gld_daily_report   @ ${OZONE_MEDALLION_GLD:-${OFS_URI}/${SBI_ENV}/data/gld}/daily_transaction_report"
