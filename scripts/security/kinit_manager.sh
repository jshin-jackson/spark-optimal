#!/usr/bin/env bash
# =============================================================================
# Kerberos kinit 관리 (Gateway Node)
# =============================================================================
#
# [목적]
#   systest.keytab 으로 Kerberos 티켓을 발급합니다 (인증 / authentication).
#   Spark/HDFS/Ozone/Hive 접근 전 반드시 실행해야 합니다.
#
# [Ranger]
#   kinit은 "누구인지"만 증명합니다. HDFS/HMS/Spark/Ozone 권한은 Ranger 정책으로만 부여됩니다.
#   Permission denied 시 chmod/chown 하지 말고 Ranger 정책을 요청하세요.
#
# [사용법]
#   bash scripts/security/kinit_manager.sh          # 티켓 없을 때만 kinit
#   bash scripts/security/kinit_manager.sh --force  # 항상 새로 kinit
#
# [kinit이란?]
#   Kerberos 인증 서버(KDC)에 "나는 systest 사용자입니다"라고 증명하는 과정.
#   성공하면 ticket cache(~/.krb5cc_*)에 티켓이 저장됩니다.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# env.conf 에서 KEYTAB, PRINCIPAL 로드
if [[ -f "${PROJECT_ROOT}/config/env.conf" ]]; then
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/config/env.conf"
fi

KEYTAB="${KEYTAB:-/opt/cloudera/systest.keytab}"
PRINCIPAL="${PRINCIPAL:-systest@QE-INFRA-AD.CLOUDERA.COM}"
FORCE="${1:-}"

if [[ ! -f "${KEYTAB}" ]]; then
  echo "ERROR: keytab not found: ${KEYTAB}" >&2
  echo "  → config/env.conf 의 KEYTAB 경로를 확인하세요." >&2
  exit 1
fi

# --force 또는 기존 티켓 없을 때 kinit 실행
if [[ "${FORCE}" == "--force" ]] || ! klist -s 2>/dev/null; then
  echo "Running kinit for ${PRINCIPAL}"
  kinit -kt "${KEYTAB}" "${PRINCIPAL}"
else
  echo "Valid Kerberos ticket already present for $(klist | awk '/Default principal/ {print $3}')"
fi

# 발급된 티켓 정보 출력 (만료 시간 확인용)
klist
