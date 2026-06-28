# SBI Spark Optimal

> **SBI 은행 데이터 플랫폼을 위한 Spark 베스트 프랙티스 프로젝트**  
> Cloudera CDP 7.3.1 · Spark 3.5 · Apache Iceberg · Apache Ozone · Kerberos

**Repository:** https://github.com/jshin-jackson/spark-optimal

---

## 이 문서는 누구를 위한 것인가요?

- Spark / Hadoop / Ozone을 **처음 접하는 분**
- SBI 클러스터에서 **데이터 파이프라인을 실행·운영**해야 하는 분
- **HDFS → Ozone 마이그레이션** 또는 **Ozone ETL** 작업을 표준화하려는 분

어려운 용어는 아래 [용어 설명](#15-용어-설명-초보자용)에서 풀어서 설명합니다.

---

## 초보자 30분 가이드 — 이 순서만 따라하세요

Gateway Node에 SSH 접속한 뒤, **아래 순서대로** 진행하면 됩니다.  
8절 이후(Airflow, 설정 상세 등)는 **첫 파이프라인 성공 후** 보면 됩니다.

| 순서 | 절 | 할 일 | 예상 시간 |
|------|-----|--------|-----------|
| ① | [5절](#5-사전-준비-prerequisites) | keytab·Python 등 사전 준비 확인 | 5분 |
| ② | [6절](#6-처음-시작하기--5단계) | clone → env.conf → JAR 검증 → kinit → zip → SDV 테스트 | 15분 |
| ③ | [7절](#7-금융-데이터-파이프라인-실습-예제) | Ozone volume/bucket 확인 → 소량 파이프라인 실행 | 10분 |
| 나중 | [8~11절](#8-airflow로-스케줄링하기) | Airflow, 커스텀 Job, 환경·YAML 설정 | — |
| 막히면 | [14절 FAQ](#14-문제-해결-faq) · [15절 용어](#15-용어-설명-초보자용) | 오류 해결·용어 확인 | — |

**한 번에 복사해서 실행 (Gateway, DEV 기준):**

```bash
# ① 프로젝트 + 환경
git clone https://github.com/jshin-jackson/spark-optimal.git
cd spark-optimal
cp config/env.template.conf config/env.conf
export SBI_ENV=dev
source config/env.conf
bash scripts/deployment/verify_jars.sh

# ② Kerberos + 패키징
bash scripts/security/kinit_manager.sh
bash scripts/deployment/package_python.sh
pip install -r requirements/financial.txt

# ③ Ozone 준비 (volume/bucket 없으면 생성)
ozone sh volume list
ozone sh bucket list --volume dev
# ozone sh volume create dev
# ozone sh bucket create --volume dev data

# ④ 소량 파이프라인 (약 100MB — 첫 실행 권장)
TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh
```

> **읽기만 먼저:** [1~3절](#1-프로젝트가-하는-일)(개념) → [4절](#4-폴더-구조-설명)(폴더 맵)은 실행과 병행해도 됩니다.  
> **Kerberos 상세:** 실행 전 [12절](#12-보안-kerberos--반드시-읽기)을 한 번 훑어 보세요.

---

## 목차

**초보자:** [30분 가이드](#초보자-30분-가이드--이-순서만-따라하세요) → 5 → 6 → 7 → (성공 후) 8~

1. [프로젝트가 하는 일](#1-프로젝트가-하는-일)
2. [전체 아키텍처 한눈에 보기](#2-전체-아키텍처-한눈에-보기)
3. [핵심 개념: Medallion Architecture](#3-핵심-개념-medallion-architecture)
4. [폴더 구조 설명](#4-폴더-구조-설명)
5. [사전 준비 (Prerequisites)](#5-사전-준비-prerequisites)
6. [처음 시작하기 — 5단계](#6-처음-시작하기--5단계)
7. [금융 데이터 파이프라인 (실습 예제)](#7-금융-데이터-파이프라인-실습-예제)
8. [Airflow로 스케줄링하기](#8-airflow로-스케줄링하기)
9. [Spark Job 직접 실행하기](#9-spark-job-직접-실행하기)
10. [환경별 설정 (DEV / UAT / PROD)](#10-환경별-설정-dev--uat--prod)
11. [설정 파일 가이드 (환경 값은 코드가 아닌 YAML)](#11-설정-파일-가이드-환경-값은-코드가-아닌-yaml)
12. [보안 (Kerberos) — 반드시 읽기](#12-보안-kerberos--반드시-읽기)
13. [SBI 클러스터 접속 정보](#13-sbi-클러스터-접속-정보)
14. [문제 해결 (FAQ)](#14-문제-해결-faq)
15. [용어 설명 (초보자용)](#15-용어-설명-초보자용)
16. [DEV Jenkins / CloudCat 클러스터 재빌드](#16-dev-jenkins--cloudcat-클러스터-재빌드)
17. [추가 문서](#17-추가-문서)

---

## 1. 프로젝트가 하는 일

이 프로젝트는 SBI 은행의 **대용량 데이터 처리 작업을 표준화**합니다.

| 할 일 | 설명 | 예시 |
|-------|------|------|
| **Migration** | HDFS(기존 저장소) 데이터를 Ozone(신규 저장소)으로 옮김 | JSON/Parquet → Iceberg 테이블 |
| **ETL** | Ozone 데이터를 읽어 가공 후 다시 Ozone에 저장 | Bronze → Silver → Gold |
| **Maintenance** | Iceberg 테이블 성능 유지 (파일 정리, 스냅샷 만료) | Compaction |
| **스케줄링** | Airflow로 위 작업을 정해진 시간에 자동 실행 | 매일 02:00 |

**왜 필요한가?**

- Spark 설정, Kerberos 인증, 리소스 할당을 **매번 수동으로 하지 않아도 됨**
- DEV / UAT / PROD 환경마다 **동일한 방식**으로 실행
- 환경별 YAML·conf 파일에 따라 **executor 수·메모리를 자동 계산** (코드 수정 불필요)

---

## 2. 전체 아키텍처 한눈에 보기

```
[Gateway Node]  ← 여기서 모든 명령 실행 (kinit + spark-submit)
      │
      ├── 1. SDV로 테스트 JSON 생성 (약 10GB)
      │
      ├── 2. HDFS에 Raw 데이터 업로드
      │         hdfs://ns1/{SBI_ENV}/raw/financial/transactions
      │
      ├── 3. Spark: HDFS JSON → Ozone Bronze (Iceberg)
      │         ofs://ozone1782570080/{SBI_ENV}/data/brnz/transactions
      │
      ├── 4. Spark: Bronze → Silver → Gold (리포트용)
      │         slvr/ ... gld/daily_transaction_report
      │
      └── [Airflow] 위 1~4단계를 스케줄에 따라 자동 실행
```

`{SBI_ENV}` = `dev` | `uat` | `prod` — 경로는 환경마다 자동으로 달라집니다.

**실행 위치**: Spark Job은 반드시 **Gateway Node**에서 실행합니다.  
Gateway Node = 클러스터에 접속하기 위한 전용 서버 (노트북/PC가 아님).

---

## 3. 핵심 개념: Medallion Architecture

데이터를 **3단계**로 나누어 관리하는 방식입니다 (은행·빅테크에서 널리 사용).

| 층 (Layer) | 이름 | 역할 | Ozone 경로 패턴 |
|------------|------|------|-----------------|
| **Bronze** | brnz | 원본 그대로 저장 (Raw) | `ofs://ozone1782570080/{env}/data/brnz/` |
| **Silver** | slvr | 정제·중복 제거·품질 검증 | `ofs://ozone1782570080/{env}/data/slvr/` |
| **Gold** | gld | 리포트·대시보드용 집계 | `ofs://ozone1782570080/{env}/data/gld/` |

예) DEV: `.../dev/data/brnz/` · PROD: `.../prod/data/brnz/`

**비유**: Bronze = 원본 사진, Silver = 보정된 사진, Gold = 앨범 표지용 요약본

---

## 4. 폴더 구조 설명

```
spark-optimal/
│
├── config/
│   └── env.template.conf      ← ★ Gateway 셸 변수 (복사 → env.conf)
│
├── conf/
│   ├── dev/spark-defaults.conf   ← DEV Spark 기본값 (32 vCore 클러스터용)
│   ├── uat/spark-defaults.conf
│   └── prod/spark-defaults.conf
│
├── governance/configs/        ← ★ 환경별 YAML (클러스터·경로·리소스)
│   ├── environments/
│   │   ├── dev.yaml           ← DEV 클러스터 용량, Medallion 경로
│   │   ├── uat.yaml
│   │   └── prod.yaml
│   ├── workloads/
│   │   ├── resource_profiles.yaml      ← PROD/UAT 워크로드 프로파일
│   │   └── resource_profiles_dev.yaml  ← DEV 전용 overlay
│   ├── medallion/
│   │   └── financial.yaml     ← 테이블 스키마 (경로는 environments/*.yaml)
│   └── infrastructure/
│       └── dev_jenkins_build.yaml  ← DEV Jenkins/CloudCat 재빌드 파라미터
│
├── data_gen/                  ← 테스트용 가짜 금융 JSON 생성 (SDV)
│   ├── financial_schema.py
│   └── generate_financial_json.py
│
├── jobs/                      ← Spark Job 진입점 (실제 실행되는 Python 파일)
│   ├── migration/             ← HDFS → Ozone
│   ├── etl/                   ← Ozone → Ozone
│   └── maintenance/           ← Iceberg 유지보수
│
├── scripts/                   ← 쉘 스크립트 (운영·배포·보안)
│   ├── security/              ← kinit, 보안 점검
│   ├── submit/                ← spark-submit 래퍼
│   ├── deployment/            ← JAR 검증, Python zip 패키징
│   │   ├── verify_jars.sh     ← env.conf Iceberg/Ozone JAR 존재 확인
│   │   └── package_python.sh
│   ├── pipeline/              ← 전체 파이프라인 한 번에 실행
│   ├── infrastructure/        ← Jenkins/CloudCat 파라미터 출력
│   └── airflow/               ← Airflow DAG 배포
│
├── airflow/dags/              ← Airflow DAG 정의 (스케줄 작업)
│
├── spark_optimal/             ← 공통 Python 라이브러리
│   ├── governance/            ← 보안, 품질, SLA
│   ├── optimization/          ← 리소스 자동 계산
│   ├── platform/              ← SparkSession, Migration, ETL
│   └── monitoring/            ← SLA·처리량 모니터링
│
├── docs/                      ← 상세 매뉴얼
│   └── infrastructure/
│       └── dev-jenkins-rebuild.md  ← DEV Jenkins/CloudCat 재빌드
└── tests/                     ← 자동 테스트 (33개)
```

**처음 볼 파일 (실행 순서)**

| 순서 | 파일 | 언제 |
|------|------|------|
| 1 | `config/env.template.conf` | 6절 Step 2 — env.conf 만들 때 |
| 2 | `scripts/deployment/verify_jars.sh` | 6절 Step 2 — JAR 확인 |
| 3 | `scripts/security/kinit_manager.sh` | 6절 Step 3 — Kerberos |
| 4 | `scripts/deployment/package_python.sh` | 6절 Step 4 — zip 빌드 |
| 5 | `scripts/pipeline/run_financial_pipeline.sh` | 7절 — 파이프라인 실행 |
| 6 | `governance/configs/environments/dev.yaml` | 경로·리소스 확인 시 |
| 7 | `conf/dev/spark-defaults.conf` | Spark 설정 확인 시 |

**나중에 볼 파일:** `dev_jenkins_build.yaml`(클러스터 재빌드), `airflow/dags/sbi_financial_medallion_dag.py`(스케줄링)

---

## 5. 사전 준비 (Prerequisites)

### Gateway Node에서 필요한 것

| 항목 | 설명 |
|------|------|
| Cloudera CDP 7.3.1 | Hadoop / Spark / Ozone / Hive가 설치된 클러스터 |
| **DEV 환경** | Cloudera **내부 Jenkins + CloudCat** 으로 프로비저닝 ([재빌드 가이드](#16-dev-jenkins--cloudcat-클러스터-재빌드)) |
| Gateway Node SSH 접속 | 터미널로 접속 가능해야 함 |
| `systest.keytab` | Kerberos 인증용 keytab 파일 (`/opt/cloudera/systest.keytab`) |
| Python 3.9+ | CDP 7.3.1 기본 Python (DEV Jenkins 빌드: **3.11** 설치 옵션) |
| `spark-submit`, `hdfs`, `kinit` | PATH에 있어야 함 |

### 로컬 PC에서 가능한 것 (클러스터 없이)

- Airflow UI로 DAG 구조 확인
- SDV 테스트 데이터 소량 생성 (`--target-gb 0.1`)
- Python 단위 테스트 (`pytest`)

---

## 6. 처음 시작하기 — 5단계

Gateway Node에 SSH 접속한 뒤 아래 순서대로 진행하세요.  
**완료 후 다음:** [7절 금융 파이프라인](#7-금융-데이터-파이프라인-실습-예제)으로 이어갑니다.

### Step 1: 프로젝트 배치

```bash
# GitHub에서 clone (최초 1회)
git clone https://github.com/jshin-jackson/spark-optimal.git
cd spark-optimal

# 이미 clone 한 경우 — template·JAR 경로 갱신 반영
git pull

# 운영 경로 예: /opt/spark-optimal (환경에 맞게 조정)
# cd /opt/spark-optimal
```

### Step 2: 환경 설정 파일 만들기

```bash
# 템플릿을 복사
cp config/env.template.conf config/env.conf

# 편집기로 열어서 본인 환경 값 확인 (대부분 이미 SBI 값으로 채워져 있음)
vi config/env.conf

# 설정 적용 — 처음에는 DEV 권장
export SBI_ENV=dev          # dev | uat | prod
source config/env.conf
```

> **Tip:** `env.template.conf`의 Medallion 경로는 `${SBI_ENV}`를 사용합니다.  
> `SBI_ENV=dev`이면 HDFS/Ozone 경로에 자동으로 `/dev/`가 붙습니다.

JAR 경로가 실제 CDP parcel과 일치하는지 확인합니다 (파이프라인 실행 전 필수):

```bash
bash scripts/deployment/verify_jars.sh
# → All Spark dependency JARs found. 가 나와야 함
```

parcel 업그레이드 후 JAR suffix가 바뀌면 `env.conf`의 `ICEBERG_JAR`, `SPARK_OZONE_JARS`를 수정한 뒤 다시 실행하세요.

### Step 3: Kerberos 로그인 (kinit)

```bash
# systest.keytab 으로 인증 — Spark 실행 전 반드시 필요
bash scripts/security/kinit_manager.sh

# 정상 여부 확인
bash scripts/security/security_check.sh
```

**kinit이란?**  
Kerberos(클러스터 보안 시스템)에 "나는 systest 사용자입니다"라고 증명하는 과정입니다.  
keytab = 비밀번호 없이 자동 인증하는 열쇠 파일.

### Step 4: Python 패키지 빌드

```bash
# Spark cluster mode에서 executor가 사용할 zip 파일 생성
bash scripts/deployment/package_python.sh
# → dist/spark_optimal.zip 생성됨
```

### Step 5: 테스트 실행

```bash
# 소량(0.1GB) SDV 데이터 생성 — 클러스터 없이 Gateway에서 가능
pip install -r requirements/financial.txt
python3 data_gen/generate_financial_json.py --target-gb 0.1
```

여기까지 성공하면 **환경 설정(6절)이 완료**된 것입니다.

**다음 단계 → [7절](#7-금융-데이터-파이프라인-실습-예제)**  
Ozone volume/bucket을 확인한 뒤 `TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh` 로 첫 파이프라인을 실행하세요.

---

## 7. 금융 데이터 파이프라인 (실습 예제)

SBI 프로젝트의 **대표 예제**: SDV 가짜 금융 JSON 10GB → HDFS → Ozone Bronze/Silver/Gold

> **선행 조건:** [6절 5단계](#6-처음-시작하기--5단계)를 모두 마쳤을 것.  
> 새 터미널을 열었다면 `export SBI_ENV=dev && source config/env.conf` 후 `bash scripts/security/kinit_manager.sh`를 다시 실행하세요 (Kerberos 티켓 만료 시에도 동일).

### 최초 실행 전: Ozone volume / bucket 준비

Medallion 경로는 `ofs://ozone1782570080/{env}/data/...` 형태입니다.  
**Ozone 루트(`ofs://ozone1782570080/`)가 비어 있어도 정상**입니다 — 프로젝트 데이터는 volume·bucket 아래에 있습니다.

DEV 기준 (`governance/configs/environments/dev.yaml`):

| 항목 | 값 |
|------|-----|
| Ozone service ID | `ozone1782570080` |
| Volume | `dev` |
| Bucket | `data` |
| Medallion prefix | `ofs://ozone1782570080/dev/data/brnz|slvr|gld/` |

```bash
# volume / bucket 존재 확인 (Ozone CLI 또는 CM Ozone UI)
ozone sh volume list
ozone sh bucket list --volume dev

# 없으면 생성 (권한·정책은 클러스터 운영 정책에 따름)
ozone sh volume create dev
ozone sh bucket create --volume dev data
```

Jenkins/CloudCat로 DEV 클러스터를 **재빌드**한 경우 Ozone OM service ID·HMS 호스트가 바뀔 수 있습니다.  
CM 값과 `env.conf`, `governance/configs/environments/dev.yaml`의 `ofs_uri`, `hms_*`를 맞춘 뒤 파이프라인을 실행하세요.

### 파이프라인 흐름 (DEV 예시)

```
단계 A  SDV로 JSON 10GB 생성     →  data/output/financial/*.jsonl
단계 B  HDFS 업로드              →  hdfs://ns1/dev/raw/financial/transactions
단계 C  Spark: HDFS → Bronze     →  Iceberg brnz_transactions
단계 D  Spark: Bronze → Gold     →  Iceberg gld_daily_report (리포트용)
```

### 한 번에 실행 (수동)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh   # 티켓 없거나 만료됐으면 필수

bash scripts/pipeline/run_financial_pipeline.sh
```

소량 테스트(100MB, **첫 실행 권장**):

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh

TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh
```

### 파이프라인 단계별 실행 (문제 발생 시 디버깅용)

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh

# 단계 A: 데이터 생성
python3 data_gen/generate_financial_json.py --target-gb 10

# 단계 B: HDFS 업로드 (HDFS_FINANCIAL_RAW 는 env.conf 에서 자동 설정)
bash scripts/data/upload_to_hdfs.sh

# 단계 C: Bronze 적재 (경로·리소스는 governance/configs/environments/dev.yaml 기준)
bash scripts/submit/spark_submit.sh migration \
  --py-file jobs/migration/hdfs_json_to_bronze_job.py \
  --project sbi_financial \
  --job hdfs_json_to_bronze \
  --source-path "${HDFS_FINANCIAL_RAW}" \
  --data-size-gb 10

# 단계 D: Silver + Gold 리포트
bash scripts/submit/spark_submit.sh etl \
  --py-file jobs/etl/bronze_to_report_job.py \
  --project sbi_financial \
  --job bronze_to_report \
  --data-size-gb 10
```

### 결과 확인 (Hue / Beeline SQL)

```sql
-- Gold 리포트 테이블 조회 예시
SELECT report_date, merchant_category, channel,
       transaction_count, total_amount
FROM sbi_financial.gld_daily_report
WHERE report_date >= date_sub(current_date(), 7)
ORDER BY total_amount DESC;
```

상세: [docs/pipeline/financial-medallion.md](docs/pipeline/financial-medallion.md)

> **다음 단계:** 파이프라인이 성공했다면 [8절 Airflow](#8-airflow로-스케줄링하기)로 자동 스케줄을 설정하세요.  
> 환경·설정 상세는 [10~11절](#10-환경별-설정-dev--uat--prod), Kerberos는 [12절](#12-보안-kerberos--반드시-읽기), 오류는 [14절 FAQ](#14-문제-해결-faq)를 참고하세요.

---

## 8. Airflow로 스케줄링하기

> **다음 단계 (운영):** [7절](#7-금융-데이터-파이프라인-실습-예제) 수동 파이프라인이 성공한 뒤 진행하세요.

**Airflow** = 작업을 정해진 시간에 자동 실행해 주는 스케줄러 (cron의 고급 버전)

### Gateway (운영) 환경

```bash
# DAG 배포
export AIRFLOW_DAGS_DIR=/usr/lib/airflow/dags/spark-optimal
bash scripts/airflow/deploy_dags.sh

# Variables 등록 (경로·스케줄·sbi_env 등)
bash scripts/airflow/import_variables.sh
```

Airflow Variable `sbi_env`를 `dev` / `uat` / `prod`로 설정하면,  
`hdfs_financial_raw` 등이 없을 때 `governance/configs/environments/{sbi_env}.yaml`의 Medallion 경로를 자동 사용합니다.

```bash
# 수동 실행 테스트
airflow dags trigger sbi_financial_medallion
```

| DAG 이름 | 스케줄 | 설명 |
|----------|--------|------|
| `sbi_financial_medallion` | 매일 02:00 | 금융 파이프라인 전체 |
| `sbi_iceberg_maintenance` | 매주 일요일 03:00 | Iceberg compaction |
| `sbi_security_precheck` | 수동 | kinit 점검 |

상세: [docs/operations/airflow-runbook.md](docs/operations/airflow-runbook.md)

### 로컬 PC (개발·학습용)

```bash
# 로컬 Airflow 설치 (macOS / Linux)
bash scripts/airflow/setup_local.sh

source .venv-airflow/bin/activate
export AIRFLOW_HOME=/path/to/spark-optimal/.airflow-local
airflow standalone    # → http://localhost:8080
```

로컬에서는 Spark/HDFS task는 실행되지 않습니다. DAG 구조 확인·UI 테스트용입니다.

상세: [docs/operations/airflow-local-install.md](docs/operations/airflow-local-install.md)

---

## 9. Spark Job 직접 실행하기

> **다음 단계 (고급):** [7절](#7-금융-데이터-파이프라인-실습-예제) 예제 파이프라인 이후, 다른 프로젝트 Job을 직접 제출할 때 참고하세요.

### Migration (HDFS → Ozone)

```bash
bash scripts/submit/spark_submit.sh migration \
  --project sbi_edw \
  --job customer_migration \
  --source-path hdfs://ns1/legacy/customers \
  --target-table spark_catalog.edw.customers \
  --format parquet \
  --data-size-gb 500
```

### ETL (Ozone → Ozone)

```bash
bash scripts/submit/spark_submit.sh etl \
  --project sbi_edw \
  --job customer_curated \
  --source-table spark_catalog.edw.customers_raw \
  --target-table spark_catalog.edw.customers_curated \
  --dedup-keys customer_id \
  --data-size-gb 100
```

### `spark_submit.sh`가 자동으로 해 주는 일

1. `kinit` (Kerberos 인증)
2. 보안 점검
3. Python zip 패키징
4. Delegation Token 설정 포함 `spark-submit` 실행

---

## 10. 환경별 설정 (DEV / UAT / PROD)

> **참고 (설정 상세):** [6~7절](#6-처음-시작하기--5단계) 실행 후 DEV/UAT/PROD 전환·튜닝 시 참고하세요.

| 환경 | 변수 | Spark 설정 | 환경 YAML |
|------|------|------------|-----------|
| 개발 | `export SBI_ENV=dev` | `conf/dev/spark-defaults.conf` | `governance/configs/environments/dev.yaml` |
| 검증 | `export SBI_ENV=uat` | `conf/uat/spark-defaults.conf` | `governance/configs/environments/uat.yaml` |
| 운영 | `export SBI_ENV=prod` | `conf/prod/spark-defaults.conf` | `governance/configs/environments/prod.yaml` |

**규칙:** PROD 작업 전 반드시 DEV → UAT 순으로 검증하세요.

**`SPARK_CONF_DIR` vs CM `spark-defaults`:**  
`spark_submit.sh`는 `SPARK_CONF_DIR=conf/{SBI_ENV}`를 설정합니다. Job 제출 시 **프로젝트의 `conf/dev/spark-defaults.conf`가 CM의 `/etc/spark3/conf.cloudera.../spark-defaults.conf`를 대체**합니다 (병합되지 않음).  
CM에서 Iceberg가 활성화되어 있어도, cluster mode executor에는 `env.conf`의 `ICEBERG_JAR`·`SPARK_OZONE_JARS`와 `verify_jars.sh` 검증이 필요합니다.

### DEV 클러스터 Spark 설정 (현재 기준)

| 항목 | 값 |
|------|-----|
| 총 vCore | 32 |
| 총 Memory | 256 GB |
| NodeManager | 9대 |
| executor cores | 1 |
| executor memory | 4g (+ 1g overhead) |
| maxExecutors | 16 |
| shuffle partitions | 32 |

10GB 파이프라인 실행 시 executor **5개** 정도로 자동 계산됩니다 (2GB/executor 기준).

DEV 설정 변경 시 수정할 파일:

1. `governance/configs/environments/dev.yaml` → `cluster`, `resource_limits`, `medallion`
2. `conf/dev/spark-defaults.conf` → Spark executor/driver 기본값
3. `governance/configs/workloads/resource_profiles_dev.yaml` → migration/etl 워크로드별 overlay

DEV 클러스터 **신규 프로비저닝·재빌드**는 Jenkins 파라미터를 사용합니다 → [16절](#16-dev-jenkins--cloudcat-클러스터-재빌드)

---

## 11. 설정 파일 가이드 (환경 값은 코드가 아닌 YAML)

환경마다 달라지는 값은 **Python 코드에 하드코딩하지 않습니다.**  
아래 설정 파일만 수정하면 DEV/UAT/PROD 전환이 가능합니다.

### 설정 우선순위

```
Airflow Variable  →  환경 변수 (env.conf)  →  environments/{env}.yaml  →  conf/{env}/spark-defaults.conf
```

### 파일별 역할

| 파일 | 담당 내용 |
|------|-----------|
| `governance/configs/environments/{env}.yaml` | HDFS/Ozone URI, HMS, **클러스터 용량**, Spark 상한, **Medallion 경로** |
| `conf/{env}/spark-defaults.conf` | Spark executor/driver, dynamic allocation, shuffle partitions |
| `governance/configs/workloads/resource_profiles.yaml` | migration/etl 워크로드 프로파일 (기본) |
| `governance/configs/workloads/resource_profiles_{env}.yaml` | 환경별 overlay (DEV는 `resource_profiles_dev.yaml`) |
| `governance/configs/medallion/financial.yaml` | Iceberg 테이블명·파티션·SDV (경로 제외) |
| `config/env.conf` | Gateway Kerberos, JAR 경로, `${SBI_ENV}` 기반 Medallion 경로 |
| `scripts/deployment/verify_jars.sh` | `ICEBERG_JAR`·`SPARK_OZONE_JARS` 파일 존재 검증 |
| `scripts/deployment/package_python.sh` | Spark cluster mode용 `dist/spark_optimal.zip` 생성 |
| `governance/configs/infrastructure/dev_jenkins_build.yaml` | DEV Jenkins/CloudCat 재빌드 파라미터 (GBN, OPTIONAL_ARGS) |

### environments/dev.yaml 예시

```yaml
cluster:
  total_vcores: 32
  total_memory_gb: 256
  node_managers: 9

resource_limits:
  max_executors_absolute: 16
  shuffle_partitions: 32

medallion:
  hdfs_raw_path: hdfs://ns1/dev/raw/financial/transactions
  brnz_base: ofs://ozone1782570080/dev/data/brnz
  slvr_base: ofs://ozone1782570080/dev/data/slvr
  gld_base: ofs://ozone1782570080/dev/data/gld
```

### 환경 전환 체크리스트

```bash
export SBI_ENV=dev          # 또는 uat / prod
source config/env.conf      # ${SBI_ENV} 경로 반영
echo $SPARK_CONF_DIR          # .../conf/dev 확인
echo $HDFS_FINANCIAL_RAW      # .../dev/raw/... 확인
bash scripts/deployment/verify_jars.sh   # CDP parcel JAR 경로 일치 확인
```

**CDP parcel / GBN 업그레이드 후:** `ls /opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime*.jar`로 실제 파일명 확인 → `env.conf` 수정 → `verify_jars.sh` 재실행 (자동 JAR 선택 스크립트는 제공하지 않음).

---

## 12. 보안 (Kerberos) — 반드시 읽기

> **초보자:** [6절 Step 3](#step-3-kerberos-로그인-kinit)에서 kinit을 실행합니다. 아래는 **원리·체크리스트·Delegation Token** 상세입니다. 첫 실행 전 한 번 훑어 보세요.

SBI 클러스터는 **Kerberos + Auto-TLS**로 보호됩니다.

### 매 배치 실행 전 체크리스트

- [ ] `bash scripts/security/kinit_manager.sh` 실행
- [ ] `bash scripts/security/security_check.sh` 통과
- [ ] `spark_submit.sh` 사용 (직접 spark-submit 금지 — Delegation Token 누락 위험)
- [ ] cluster deploy mode 사용 (local mode 금지)

### Delegation Token이란?

Gateway에서 kinit한 인증 정보를 **Spark executor(워커)**까지 전달하는 토큰입니다.  
이 설정 없으면 executor가 HDFS/Ozone에 접근하지 못해 Job이 실패합니다.

필수 Spark 설정 (자동 적용됨):

```
spark.yarn.access.hadoopFileSystems=hdfs://ns1,ofs://ozone1782570080
spark.security.credentials.hadoopfs.enabled=true
spark.security.credentials.hive.enabled=true
```

---

## 13. SBI 클러스터 접속 정보

| 구성요소 | 값 |
|----------|-----|
| HDFS Nameservice | `ns1` → `hdfs://ns1` |
| Ozone Service ID | `ozone1782570080` → `ofs://ozone1782570080` |
| Ozone OM | `ccycloud-2.jshin-sbi.root.comops.site:9862`, `ccycloud-8.jshin-sbi.root.comops.site:9862` |
| Hive Metastore | `ccycloud-1.jshin-sbi.root.comops.site:9083`, `ccycloud-6.jshin-sbi.root.comops.site:9083` |
| Kerberos Principal | `systest@QE-INFRA-AD.CLOUDERA.COM` |
| Keytab | `/opt/cloudera/systest.keytab` |

### 클러스터 규모

**DEV** (Cloudera 내부 Jenkins/CloudCat — `jshin-sbi`)

| 항목 | 값 |
|------|-----|
| vCore | 32 |
| Memory | 256 GB |
| NodeManager | 9대 |
| Cluster shortname | `ccycloud-{1..10}.jshin-sbi` |
| Kerberos | AD KERBEROS |
| Java | 17.0.11-openjdk |

Jenkins/CloudCat 재빌드 파라미터·절차 → **[16절 DEV Jenkins / CloudCat 클러스터 재빌드](#16-dev-jenkins--cloudcat-클러스터-재빌드)**

**PROD** (운영 클러스터)

| 항목 | 값 |
|------|-----|
| 총 Memory | 120.67 TB |
| 총 vCore | 26,496 |
| NodeManager | 214대 (각 128 vCore, 1.5 TiB) |

PROD/UAT 클러스터 용량은 `governance/configs/environments/prod.yaml`의 `cluster` 섹션에서 관리합니다.

---

## 14. 문제 해결 (FAQ)

### Q. `kinit: Keytab contains no suitable keys for systest@...`

keytab 경로 또는 principal이 틀렸습니다.

```bash
echo $KEYTAB $PRINCIPAL
kinit -kt /opt/cloudera/systest.keytab systest@QE-INFRA-AD.CLOUDERA.COM
```

### Q. `hdfs dfs -ls ofs://ozone1782570080/` 결과가 비어 있음

**정상입니다.** Ozone 루트에는 volume이 바로 보이지 않을 수 있습니다.  
Medallion 데이터는 `ofs://ozone1782570080/dev/data/...` 아래에 저장됩니다.

```bash
ozone sh volume list
ozone sh bucket list --volume dev
```

volume `dev`, bucket `data`가 없으면 [7절 Ozone 준비](#최초-실행-전-ozone-volume--bucket-준비)를 먼저 진행하세요.

### Q. Spark Job은 시작됐는데 executor에서 Ozone 접근 실패

`spark.yarn.access.hadoopFileSystems`에 OFS URI가 포함되어 있는지 확인:

```bash
grep access.hadoopFileSystems conf/${SBI_ENV}/spark-defaults.conf
# hdfs://ns1,ofs://ozone1782570080 이 있어야 함
```

### Q. DEV에서 executor가 너무 많이 잡혀 YARN 리소스 부족

`governance/configs/environments/dev.yaml`의 `resource_limits.max_executors_absolute` 값을 낮추세요.  
현재 DEV 기본값은 **16**입니다.

### Q. Medallion 경로가 prod로 잡힘

```bash
echo $SBI_ENV $HDFS_FINANCIAL_RAW
# SBI_ENV=dev 이고 .../dev/raw/... 이어야 함
export SBI_ENV=dev && source config/env.conf
```

### Q. Airflow DAG가 UI에 안 보임

```bash
airflow dags list-import-errors
ls -la $AIRFLOW_HOME/dags/spark-optimal/
```

### Q. DEV 클러스터를 처음부터 다시 올려야 함

[Jenkins / CloudCat 재빌드](#16-dev-jenkins--cloudcat-클러스터-재빌드) 절을 따르세요.

```bash
bash scripts/infrastructure/print_dev_jenkins_params.sh
# → Jenkins Job에 파라미터 입력 후 빌드
# → 완료 후 dev.yaml cluster 섹션을 yarn node -list 기준으로 갱신
```

### Q. SDV 설치 오류

```bash
pip install -r requirements/financial.txt
# Python 3.9+ 필요
```

### Q. Spark Job 실패 — Iceberg/Ozone JAR not found

parcel 빌드마다 JAR 파일명 suffix가 다릅니다 (예: `700-158` vs `600-325`).

```bash
ls /opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime*.jar
ls /opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3*.jar

# env.conf 의 ICEBERG_JAR, SPARK_OZONE_JARS 를 실제 파일명으로 수정
cp config/env.template.conf config/env.conf   # template 갱신 후 재복사
vi config/env.conf

bash scripts/deployment/verify_jars.sh
```

더 많은 해결 방법: [docs/troubleshooting/common-issues.md](docs/troubleshooting/common-issues.md)

---

## 15. 용어 설명 (초보자용)

| 용어 | 쉬운 설명 |
|------|-----------|
| **Spark** | 대용량 데이터를 빠르게 처리하는 분산 컴퓨팅 엔진 |
| **PySpark** | Python으로 Spark를 사용하는 API |
| **HDFS** | Hadoop 분산 파일 시스템 (기존 데이터 저장소) |
| **Ozone** | HDFS를 대체하는 차세대 분산 저장소 (SBI 신규 표준) |
| **Iceberg** | 대용량 테이블 포맷 (ACID, 스키마 변경, Time Travel 지원) |
| **OFS** | Ozone 파일 시스템 URI (`ofs://ozone1782570080/...`) |
| **HMS** | Hive Metastore — Iceberg 테이블 메타데이터(스키마) 저장소 |
| **Gateway Node** | 클러스터에 Job을 제출하는 전용 서버 |
| **YARN** | 클러스터 CPU/메모리 자원 관리자 |
| **Executor** | Spark가 실제 데이터 처리를 수행하는 워커 프로세스 |
| **kinit** | Kerberos 티켓 발급 명령 |
| **keytab** | kinit 없이 자동 인증하는 Kerberos 열쇠 파일 |
| **Delegation Token** | Gateway 인증을 executor까지 전달하는 토큰 |
| **SDV** | Synthetic Data Vault — 통계적으로 realistic한 가짜 데이터 생성 |
| **Medallion** | Bronze/Silver/Gold 3단계 데이터 아키텍처 |
| **Airflow** | 데이터 파이프라인 스케줄링·모니터링 도구 |
| **DAG** | Airflow에서 작업 순서를 정의한 그래프 |
| **Compaction** | Iceberg의 작은 파일들을 큰 파일로 합쳐 읽기 성능 향상 |
| **CloudCat** | Cloudera 내부 클러스터 자동 프로비저닝 도구 (Jenkins Job과 연동) |
| **GBN** | Cloudera 빌드 번호 (`gbn://...`) — CM/CDH 버전 지정 |
| **Jenkins** | Cloudera 내부 CI — DEV 클러스터 생성·재생성 Job 실행 |

---

## 16. DEV Jenkins / CloudCat 클러스터 재빌드

DEV 환경은 SBI 고객 PROD가 아니라 **Cloudera 내부 yCloud** 에서 Jenkins + CloudCat 으로 올립니다.  
클러스터를 삭제했거나 동일 스펙으로 다시 만들어야 할 때, 아래 파라미터를 Jenkins Job에 입력하면 됩니다.

### 관련 파일

| 파일 | 설명 |
|------|------|
| `governance/configs/infrastructure/dev_jenkins_build.yaml` | 빌드 파라미터 원본 (YAML) |
| `docs/infrastructure/dev-jenkins-rebuild.md` | 재빌드 상세 매뉴얼 |
| `scripts/infrastructure/print_dev_jenkins_params.sh` | Jenkins 붙여넣기용 출력 |

### Jenkins / CloudCat 빌드 파라미터 (전체)

| 파라미터 | 값 |
|----------|-----|
| **CLUSTER_SHORTNAME** | `ccycloud-{1..10}.jshin-sbi` |
| **CM_VERSION** | `gbn://74798696` |
| **CDH** | `gbn://74774806` |
| **CLOUDCAT_OS** | `redhat96` |
| **CLOUDCAT_BUDGET** | `--ycloud-queue=professional-services` |
| **DB** | `postgresql` |
| **KERBEROS** | `AD KERBEROS` |
| **JAVA_VERSION** | `17.0.11-openjdk` |

**호스트 (shortname):** `ccycloud-1.jshin-sbi` … `ccycloud-10.jshin-sbi` (10노드)  
운영 FQDN 예: `ccycloud-1.jshin-sbi.root.comops.site`

**OPTIONAL_ARGS:**

```
--install-python-version=3.11 --include-service-types=ZOOKEEPER,HDFS,HBASE,HIVE,YARN,SPARK3_ON_YARN,IMPALA,OOZIE,HUE,LIVY,SOLR,RANGER,RANGER_KMS,KAFKA,KUDU,ATLAS,KNOX,TEZ,HIVE_ON_TEZ,SCHEMAREGISTRY,STREAMS_MESSAGING_MANAGER,STREAMS_REPLICATION_MANAGER,OZONE --ha-service-types=ALL
```

포함 서비스 중 spark-optimal이 **직접 사용**하는 것: HDFS, YARN, Spark3 on YARN, Hive, Ozone, Ranger, Knox (Kerberos AD).

### 파라미터 빠르게 출력하기

```bash
bash scripts/infrastructure/print_dev_jenkins_params.sh
```

출력 예:

```
CLUSTER_SHORTNAME=ccycloud-{1..10}.jshin-sbi
CM_VERSION=gbn://74798696
CDH=gbn://74774806
...
OPTIONAL_ARGS=--install-python-version=3.11 --include-service-types=...
```

### 재빌드 후 spark-optimal 연동 (체크리스트)

1. **Jenkins Job 실행** — 위 파라미터로 CloudCat 프로비저닝 완료 대기
2. **Gateway 설정**
   ```bash
   git pull   # template·JAR 경로 최신화
   export SBI_ENV=dev
   cp config/env.template.conf config/env.conf
   # CM에서 HMS/Ozone OM service ID 변경 시 env.conf · dev.yaml 갱신
   source config/env.conf
   bash scripts/deployment/verify_jars.sh
   bash scripts/security/kinit_manager.sh
   ```
3. **YARN 용량 동기화** — 실측 후 `governance/configs/environments/dev.yaml` 갱신
   ```bash
   yarn node -list -all
   # cluster.total_vcores / total_memory_gb / node_managers 수정
   ```
4. **검증**
   ```bash
   pytest tests/ -q --ignore=tests/integration
   TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh
   ```

### GBN / 서비스 변경 시

| 변경 내용 | 수정 위치 |
|-----------|-----------|
| CM·CDH 버전 (GBN) | `dev_jenkins_build.yaml` → `cm_version`, `cdh_version` |
| Python·서비스 목록 | `dev_jenkins_build.yaml` → `optional_args` |
| Spark executor 튜닝 | `environments/dev.yaml`, `conf/dev/spark-defaults.conf` |

> GBN을 올릴 때는 CDP 7.3.x · Spark 3.5 · Iceberg · Ozone JAR 경로(`env.conf`) 호환성을 먼저 확인하세요.

상세: [docs/infrastructure/dev-jenkins-rebuild.md](docs/infrastructure/dev-jenkins-rebuild.md)

---

## 17. 추가 문서

| 문서 | 내용 |
|------|------|
| [Spark Job Standards](docs/standards/spark-job-standards.md) | 코딩·운영 표준 |
| [Enterprise Runbook](docs/operations/enterprise-runbook.md) | 운영 매뉴얼 |
| [Airflow Runbook (Gateway)](docs/operations/airflow-runbook.md) | CDP Airflow 배포 |
| [Airflow Local Install](docs/operations/airflow-local-install.md) | 로컬 Airflow 설치 |
| [Financial Medallion Pipeline](docs/pipeline/financial-medallion.md) | 금융 파이프라인 상세 |
| [Gateway Runbook](docs/operations/gateway-runbook.md) | Gateway 운영 |
| [DEV Jenkins Rebuild](docs/infrastructure/dev-jenkins-rebuild.md) | DEV CloudCat/Jenkins 재빌드 |
| [Python API](docs/api/python-api.md) | Python API 레퍼런스 |
| [Troubleshooting](docs/troubleshooting/common-issues.md) | 장애 대응 |

---

## 테스트 실행

```bash
pip install -r requirements/dev.txt
pytest tests/ -q --ignore=tests/integration
# 33 tests (환경 설정·Medallion·리소스 계산 포함)
```

---

## 라이선스 / 문의

SBI Bank 내부 프로젝트 — Cloudera CDP 7.3.1 환경 전용

문제 발생 시: `docs/troubleshooting/common-issues.md` → 데이터 플랫폼 팀 Escalation
