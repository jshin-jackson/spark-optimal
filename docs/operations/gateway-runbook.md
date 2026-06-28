# Gateway Node 운영 Runbook

## 사전 조건

- Gateway node에 `systest.keytab` 배치
- `config/env.conf` 환경 변수 설정 (DEV/UAT/PROD)
- CDP parcel JAR 경로 확인
- **Ranger:** HDFS / HMS / Spark / Ozone 권한은 Ranger 정책만 사용 — `governance/configs/security/ranger.yaml`

## 실행 순서

1. `export SBI_ENV=dev|uat|prod`
2. `source config/env.conf`
3. `bash scripts/security/kinit_manager.sh` — Kerberos **인증**
4. `bash scripts/security/security_check.sh` — Kerberos + Ranger **권한** probe
5. `bash scripts/deployment/package_python.sh`
6. `bash scripts/submit/spark_submit.sh <migration|etl|maintenance> ...`

## Ranger (Authorization)

SBI 정책: **chmod/chown/setfacl 금지**. 접근 거부 시 Ranger UI에서 `systest` principal에 정책 추가.

| Service | Ranger plugin | spark-optimal resources |
|---------|---------------|-------------------------|
| HDFS | hdfs | `{env}/raw/financial/transactions`, Spark history |
| Ozone | ozone | `{env}/data/brnz`, `slvr`, `gld` |
| Hive | hive | DB `sbi_financial`, Iceberg tables |
| Spark | spark | Align with Hive/HDFS/Ozone when plugin enabled |

상세: [ranger-authorization.md](ranger-authorization.md)

## Delegation Token

Spark가 cluster mode로 제출되면 driver/executor가 다음 토큰을 자동 수집합니다:

- HDFS delegation token (`spark.security.credentials.hadoopfs.enabled=true`)
- Hive/HMS token (`spark.security.credentials.hive.enabled=true`)
- Ozone OFS (`spark.yarn.access.hadoopFileSystems`에 OFS URI 포함)

토큰은 **Kerberos 신원**을 executor에 전달합니다. **Ranger**가 HDFS/Ozone/Hive 접근을 허용/거부합니다.

장시간 작업 전에는 `bash scripts/security/token_renewal.sh`로 kinit 갱신 후 재제출합니다.

## 장애 대응

| symptom | 조치 |
|----------|------|
| `Permission denied` / `AccessControlException` (klist OK) | **Ranger** HDFS/Ozone/Hive 정책 — `ranger.yaml` 참고, chmod 금지 |
| `Invalid token` / `SASL` 오류 | kinit 재실행, security_check 확인 |
| Executor Ozone 접근 실패 | `spark.yarn.access.hadoopFileSystems`에 `ofs://ozone1782570080` 확인, OM 주소 확인 |
| HMS 연결 실패 | HMS principal, Auto-TLS truststore 확인 |
| OOM / spill | workload profile 조정, data-size-gb 재산정 |
