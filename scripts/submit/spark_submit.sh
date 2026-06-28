#!/usr/bin/env bash
# =============================================================================
# Spark Submit 표준 래퍼 (Gateway Node 전용)
# =============================================================================
#
# [목적]
#   SBI 표준에 맞게 spark-submit을 실행합니다.
#   Kerberos(kinit), Delegation Token, Python zip 배포를 자동 처리합니다.
#
# [사용법]
#   bash scripts/submit/spark_submit.sh <migration|etl|maintenance> [옵션...]
#
#   # Migration 예시
#   bash scripts/submit/spark_submit.sh migration \
#     --py-file jobs/migration/hdfs_json_to_bronze_job.py \
#     --project sbi_financial --job my_job \
#     --source-path hdfs://ns1/raw --data-size-gb 10
#
# [Job 유형]
#   migration   → HDFS/Ozone → Iceberg (기본: hdfs_to_ozone_job.py)
#   etl         → Ozone → Ozone (기본: ozone_to_ozone_job.py)
#   maintenance → Iceberg compaction (기본: iceberg_maintenance_job.py)
#
#   --py-file 로 다른 job 파일 지정 가능 (금융 파이프라인 등)
#
# [주의]
#   - Gateway Node에서만 실행
#   - config/env.conf 가 source 되어 있어야 함
#   - cluster deploy mode 사용 (driver가 YARN에서 실행)
# =============================================================================

set -euo pipefail  # 오류 시 즉시 종료, 미정의 변수 사용 금지

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# SBI_ENV: dev | uat | prod — conf/{env}/spark-defaults.conf 선택
ENV_NAME="${SBI_ENV:-dev}"
JOB_TYPE="${1:-}"
shift || true

if [[ -z "${JOB_TYPE}" ]]; then
  echo "Usage: $0 <migration|etl|maintenance> [spark-submit args...]" >&2
  exit 1
fi

# ── 환경 변수 로드 (Kerberos, HDFS, Ozone 주소 등) ──
if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

export SPARK_OPTIMAL_HOME="${PROJECT_ROOT}"
export SPARK_CONF_DIR="${PROJECT_ROOT}/conf/${ENV_NAME}"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

# ── 보안 사전 작업 (순서 중요) ──
# 1) kinit: Kerberos 티켓 발급
bash "${PROJECT_ROOT}/scripts/security/kinit_manager.sh"
# 2) 보안 점검: 티켓·환경 변수 확인
bash "${PROJECT_ROOT}/scripts/security/security_check.sh"
# 3) Python zip: cluster mode executor에 코드 배포
bash "${PROJECT_ROOT}/scripts/deployment/package_python.sh"

PY_FILES="${PROJECT_ROOT}/dist/spark_optimal.zip"

# ── Job 유형별 기본 Python 파일 ──
case "${JOB_TYPE}" in
  migration)
    DEFAULT_JOB="${PROJECT_ROOT}/jobs/migration/hdfs_to_ozone_job.py"
    ;;
  etl)
    DEFAULT_JOB="${PROJECT_ROOT}/jobs/etl/ozone_to_ozone_job.py"
    ;;
  maintenance)
    DEFAULT_JOB="${PROJECT_ROOT}/jobs/maintenance/iceberg_maintenance_job.py"
    ;;
  *)
    echo "Unknown job type: ${JOB_TYPE}" >&2
    exit 1
    ;;
esac

PY_FILE="${DEFAULT_JOB}"
EXTRA_ARGS=()

# --py-file 옵션으로 다른 job 파일 지정 가능
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--py-file" ]]; then
    PY_FILE="$2"
    shift 2
  else
    EXTRA_ARGS+=("$1")
    shift
  fi
done

# Delegation Token: HDFS + Ozone 모두 접근 가능하도록 URI 나열
HADOOP_FILESYSTEMS="${HDFS_URI},${OFS_URI}"

# ── spark-submit 실행 ──
# cluster mode: driver가 YARN container에서 실행 (Gateway 부하 감소)
# --principal/--keytab: executor Kerberos 인증
# spark.security.credentials.*: Delegation Token 자동 수집·전파
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --principal "${PRINCIPAL}" \
  --keytab "${KEYTAB}" \
  --conf "spark.yarn.access.hadoopFileSystems=${HADOOP_FILESYSTEMS}" \
  --conf "spark.security.credentials.hadoopfs.enabled=true" \
  --conf "spark.security.credentials.hive.enabled=true" \
  --conf "spark.yarn.security.tokens.hadoopfs.enabled=true" \
  --conf "spark.yarn.security.tokens.hive.enabled=true" \
  --conf "spark.kerberos.renewal.credentials=ccache" \
  --py-files "${PY_FILES}" \
  "${PY_FILE}" \
  "${EXTRA_ARGS[@]}"
