# Airflow Local Installation Manual (SBI Spark Optimal)

로컬 개발 환경(macOS / Linux)에서 Airflow를 설치하고 SBI DAG를 개발·검증하는 방법입니다.

> **주의**: Spark/HDFS/Ozone task는 CDP Gateway + Kerberos 환경에서만 실제 실행됩니다.  
> 로컬 Airflow는 **DAG 개발, 스케줄 검증, UI 테스트** 용도로 사용합니다.

---

## 1. 사전 요구사항

| 항목 | 버전 |
|------|------|
| Python | 3.9 ~ 3.11 (CDP 7.3.1 Spark 환경과 동일 권장: 3.9+) |
| pip | 최신 |
| disk | 2GB+ (Airflow DB + logs) |

macOS:
```bash
# Homebrew Python (optional)
brew install python@3.11
```

RHEL / Gateway (Linux):
```bash
sudo yum install python3 python3-pip python3-devel
```

---

## 2. 빠른 설치 (자동 스크립트)

프로젝트 루트에서:

```bash
cd /path/to/spark-optimal
bash scripts/airflow/setup_local.sh
```

스크립트가 수행하는 작업:
1. `.venv-airflow` 가상환경 생성
2. `requirements/airflow.txt` 설치
3. `AIRFLOW_HOME=.airflow-local` 초기화
4. DAG symlink: `.airflow-local/dags/spark-optimal` → `airflow/dags/`
5. DB migrate + local Variables import

---

## 3. 수동 설치 (단계별)

### Step 1: 가상환경

```bash
cd /path/to/spark-optimal
python3 -m venv .venv-airflow
source .venv-airflow/bin/activate
pip install --upgrade pip
pip install -r requirements/base.txt
pip install -r requirements/airflow.txt
```

### Step 2: AIRFLOW_HOME 설정

```bash
export AIRFLOW_HOME=/path/to/spark-optimal/.airflow-local
mkdir -p "$AIRFLOW_HOME/dags"

# DAG 폴더 연결
ln -sfn /path/to/spark-optimal/airflow/dags "$AIRFLOW_HOME/dags/spark-optimal"
```

`~/.bashrc` 또는 `~/.zshrc`에 추가 (선택):
```bash
export AIRFLOW_HOME=/path/to/spark-optimal/.airflow-local
```

### Step 3: DB 초기화

Airflow 2.6+:
```bash
airflow db migrate
```

### Step 4: 관리자 계정 (standalone 사용 시 생략 가능)

```bash
airflow users create \
  --username admin \
  --password admin \
  --firstname SBI \
  --lastname Admin \
  --role Admin \
  --email admin@local.dev
```

### Step 5: Variables import (로컬용)

`airflow/config/variables.local.json`에서 경로를 본인 환경에 맞게 수정:

```json
{
  "spark_optimal_home": "/path/to/spark-optimal",
  "sbi_env": "dev",
  "financial_target_gb": "1",
  "schedule_financial_medallion": "@once"
}
```

```bash
airflow variables import airflow/config/variables.local.json
```

| Variable | 로컬 권장값 | 설명 |
|----------|-------------|------|
| `spark_optimal_home` | 프로젝트 절대경로 | DAG bash script base |
| `sbi_env` | `dev` | conf/dev/spark-defaults.conf |
| `financial_target_gb` | `1` | 로컬 테스트용 작은 크기 |
| `schedule_financial_medallion` | `@once` | 로컬: 수동 실행만 |

---

## 4. Airflow 실행

### 방법 A: standalone (로컬 개발 권장)

Scheduler + Webserver + Admin 계정을 한 번에 시작:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME=/path/to/spark-optimal/.airflow-local
airflow standalone
```

- UI: http://localhost:8080
- 터미널에 admin 비밀번호가 출력됩니다.

### 방법 B: webserver + scheduler 분리

터미널 1:
```bash
airflow webserver --port 8080
```

터미널 2:
```bash
airflow scheduler
```

---

## 5. DAG 확인

```bash
# DAG 목록
airflow dags list | grep sbi_

# Import error 확인
airflow dags list-import-errors

# DAG 구조 확인
airflow tasks list sbi_financial_medallion
```

예상 DAG:

| DAG ID | 로컬 스케줄 | 용도 |
|--------|-------------|------|
| `sbi_financial_medallion` | `@once` | 전체 medallion 파이프라인 |
| `sbi_iceberg_maintenance` | `@weekly` | Iceberg 유지보수 |
| `sbi_security_precheck` | Manual | kinit 점검 |

---

## 6. 로컬 테스트

### DAG parse / task dry-run

```bash
# 단일 DAG run test (task 실제 실행 — gateway 없으면 bash task 실패 가능)
airflow dags test sbi_financial_medallion 2026-06-27
```

### UI에서 Trigger

1. http://localhost:8080 접속
2. `sbi_financial_medallion` 선택
3. **Trigger DAG** 클릭

### 로컬에서 가능한 task

| Task | 로컬 실행 |
|------|-----------|
| `generate_sdv_json` | 가능 (SDV pip install 필요) |
| `security_precheck` | 불가 (kinit/keytab 없음) |
| `upload_to_hdfs` | 불가 (HDFS 없음) |
| Spark tasks | 불가 (YARN cluster 필요) |

로컬 SDV 생성만 테스트:
```bash
pip install -r requirements/financial.txt
python3 data_gen/generate_financial_json.py --target-gb 0.1 --output-dir data/output/financial
```

---

## 7. Gateway(CDP) 환경으로 전환

로컬 개발 후 Gateway/Airflow 서버에 배포:

```bash
# variables.template.json 경로를 gateway 기준으로 수정
vi airflow/config/variables.template.json

export AIRFLOW_DAGS_DIR=/usr/lib/airflow/dags/spark-optimal
bash scripts/airflow/deploy_dags.sh
bash scripts/airflow/import_variables.sh
```

Gateway runbook: [airflow-runbook.md](./airflow-runbook.md)

---

## 8. 디렉터리 구조 (로컬)

```
spark-optimal/
├── .venv-airflow/          # Airflow 전용 venv (gitignore)
├── .airflow-local/         # AIRFLOW_HOME (gitignore)
│   ├── airflow.cfg
│   ├── airflow.db            # SQLite metadata DB
│   ├── logs/
│   └── dags/
│       └── spark-optimal -> ../../airflow/dags
├── airflow/
│   ├── dags/               # DAG 소스
│   └── config/
│       ├── variables.local.json
│       └── variables.template.json
└── scripts/airflow/
    ├── setup_local.sh
    ├── deploy_dags.sh
    └── import_variables.sh
```

`.gitignore`에 추가 권장:
```
.venv-airflow/
.airflow-local/
```

---

## 9. 트러블슈팅

### DAG가 UI에 안 보임

```bash
airflow dags list-import-errors
# PYTHONPATH 확인
export PYTHONPATH=/path/to/spark-optimal/airflow/dags:$PYTHONPATH
```

### `ModuleNotFoundError: common.sbi_config`

DAG folder가 `airflow/dags` 전체여야 합니다 (symlink 확인):
```bash
ls -la $AIRFLOW_HOME/dags/spark-optimal/common/
```

### Port 8080 사용 중

```bash
airflow webserver --port 8081
```

### DB reset (로컬만)

```bash
rm -f $AIRFLOW_HOME/airflow.db
airflow db migrate
```

### macOS: `fork()` 경고

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

---

## 10. 참고 링크

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [SBI Airflow Runbook (CDP Gateway)](./airflow-runbook.md)
- [Financial Medallion Pipeline](../pipeline/financial-medallion.md)
