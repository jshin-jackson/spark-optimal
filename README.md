# SBI Spark Optimal

> **SBI 은행 데이터 플랫폼을 위한 Spark 베스트 프랙티스 프로젝트**  
> Cloudera CDP 7.3.1 · Spark 3.5 · Iceberg · Ozone · Kerberos · Ranger KMS (HDFS + Ozone TDE) · **Cloudera Iceberg–Ozone Ranger**

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
| ② | [6절](#6-처음-시작하기--5단계) | clone → env.conf → JAR 검증 → kinit → zip | 10분 |
| ②-b | [7절 — Ranger Web UI](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로) | **Ranger Admin**에서 cm_kms + cm_hdfs + cm_ozone + cm_hive 정책 등록 | 30분 |
| ③ | [7절](#7-금융-데이터-파이프라인-실습-예제) | security_check → HDFS EZ + Ozone TDE → 파이프라인 | 10분 |
| 나중 | [8~11절](#8-airflow로-스케줄링하기) | Airflow, 커스텀 Job, 환경·YAML 설정 | — |
| 막히면 | [14절 FAQ](#14-문제-해결-faq) · [15절 용어](#15-용어-설명-초보자용) | 오류 해결·용어 확인 | — |

**한 번에 복사해서 실행 (Gateway, DEV 기준):**

```bash
# ⓪ Python 3.11 (최초 1회 — [5절](#python-311-기본-설정-bashrc) 참고)
grep -q "alias python3=" ~/.bashrc || cat >> ~/.bashrc <<'EOF'
alias python3='/usr/bin/python3.11'
alias pip3='/usr/bin/python3.11 -m pip'
EOF
source ~/.bashrc

# ① 프로젝트 + 환경
git clone https://github.com/jshin-jackson/spark-optimal.git
cd spark-optimal
cp config/env.template.conf config/env.conf
export SBI_ENV=dev
source config/env.conf
bash scripts/deployment/verify_jars.sh

# ② Kerberos(인증) + 패키징
bash scripts/security/kinit_manager.sh
bash scripts/deployment/package_python.sh
pip install -r requirements/financial.txt

# ②-b Ranger Web UI — [7절 Ranger 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로) 완료 후 진행
#     (플랫폼/Ranger Admin: cm_kms → cm_hdfs → cm_ozone → cm_hive)
bash scripts/security/print_ranger_iceberg_pairs.sh   # 등록할 정책 목록 확인

# ③ Ranger probe + HDFS EZ + Ozone bucket (정책 등록 후)
bash scripts/security/security_check.sh
bash scripts/infrastructure/setup_hdfs_encryption_zone.sh
bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh

# ④ 소량 파이프라인 (약 100MB — 첫 실행 권장)
TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh
```

> **읽기만 먼저:** [1~3절](#1-프로젝트가-하는-일)(개념) → [4절](#4-폴더-구조-설명)(폴더 맵)은 실행과 병행해도 됩니다.  
> **Kerberos / Ranger / TDE:** [12절](#12-보안--kerberos-인증--ranger-권한) · [Iceberg–Ozone Ranger (Cloudera)](docs/operations/ranger-iceberg-ozone-pairs.md) · [HDFS Encryption](docs/operations/hdfs-encryption.md) · [Ozone Encryption](docs/operations/ozone-encryption.md)

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
12. [보안 — Kerberos + Ranger](#12-보안--kerberos-인증--ranger-권한)
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

- Spark 설정, Kerberos **인증**, Ranger **권한**, 리소스 할당을 **매번 수동으로 하지 않아도 됨**
- DEV / UAT / PROD 환경마다 **동일한 방식**으로 실행
- 환경별 YAML·conf 파일에 따라 **executor 수·메모리를 자동 계산** (코드 수정 불필요)

---

## 2. 전체 아키텍처 한눈에 보기

```
[Gateway Node]  ← 여기서 모든 명령 실행 (kinit + security_check + spark-submit)
      │
      │  [보안] Kerberos = 인증 · Ranger = 권한 · Ranger KMS = hdfs_encryption_key (HDFS) + ozone_encryption_key (Ozone)
      │
      ├── 1. SDV로 테스트 JSON 생성 (약 10GB)
      │
      ├── 2. HDFS에 Raw 데이터 업로드 (**Encryption Zone**)
      │         hdfs://ns1/{SBI_ENV}/raw/financial/transactions
      │
      ├── 3. Spark: HDFS JSON → Ozone Bronze (Iceberg, **TDE encrypted**)
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

Ozone Medallion 경로의 **모든 데이터**는 bucket `/{env}/data`에 **Ranger KMS 키 `ozone_encryption_key`로 저장 시 암호화(TDE)** 됩니다.  
HDFS Raw 경로(`/{env}/raw/financial/transactions`)는 **`hdfs_encryption_key`** 로 **Encryption Zone** 암호화됩니다.

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
│   ├── infrastructure/
│   │   └── dev_jenkins_build.yaml  ← DEV Jenkins/CloudCat 재빌드 파라미터
│   └── security/
│       ├── ranger.yaml                    ← ★ Ranger 권한 인벤토리 (cm_hdfs/cm_hive/cm_ozone)
│       ├── ranger_iceberg_ozone_pairs.yaml ← ★ Iceberg 테이블 ↔ Ozone 경로 paired 정책
│       ├── hdfs_encryption.yaml           ← ★ HDFS TDE (Ranger KMS Encryption Zone)
│       └── ozone_encryption.yaml          ← ★ Ozone TDE (Ranger KMS ozone_encryption_key)
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
│   ├── security/              ← kinit, 보안 점검, Ranger paired 정책 출력
│   │   ├── kinit_manager.sh
│   │   ├── security_check.sh
│   │   └── print_ranger_iceberg_pairs.sh
│   ├── submit/                ← spark-submit 래퍼
│   ├── deployment/            ← JAR 검증, Python zip 패키징
│   │   ├── verify_jars.sh     ← env.conf Iceberg/Ozone JAR 존재 확인
│   │   └── package_python.sh
│   ├── pipeline/              ← 전체 파이프라인 한 번에 실행
│   ├── infrastructure/        ← Jenkins/CloudCat, HDFS EZ + Ozone encrypted bucket
│   │   ├── print_dev_jenkins_params.sh
│   │   ├── setup_hdfs_encryption_zone.sh
│   │   └── setup_ozone_encrypted_bucket.sh
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
└── tests/                     ← 자동 테스트 (41개)
```

**처음 볼 파일 (실행 순서)**

| 순서 | 파일 | 언제 |
|------|------|------|
| 1 | `config/env.template.conf` | 6절 Step 2 — env.conf 만들 때 |
| 2 | `scripts/deployment/verify_jars.sh` | 6절 Step 2 — JAR 확인 |
| 3 | `scripts/security/kinit_manager.sh` | 6절 Step 3 — Kerberos **인증** |
| 4 | `scripts/security/security_check.sh` | 6절 Step 3 — Ranger + KMS + HDFS EZ probe |
| 5 | `scripts/security/print_ranger_iceberg_pairs.sh` | 7절 — Ranger Web UI 등록 전 정책 목록 출력 |
| 6 | `scripts/infrastructure/setup_hdfs_encryption_zone.sh` | 7절 — HDFS Encryption Zone (`hdfs_encryption_key`) |
| 7 | `scripts/infrastructure/setup_ozone_encrypted_bucket.sh` | 7절 — `-k ozone_encryption_key` bucket |
| 8 | `scripts/deployment/package_python.sh` | 6절 Step 4 — zip 빌드 |
| 9 | `scripts/pipeline/run_financial_pipeline.sh` | 7절 — 파이프라인 실행 |
| 10 | `governance/configs/security/ranger_iceberg_ozone_pairs.yaml` | Cloudera cm_hive + cm_ozone 정책 인벤토리 |
| 11 | `governance/configs/security/hdfs_encryption.yaml` | HDFS TDE · EZ 경로 |
| 12 | `governance/configs/security/ozone_encryption.yaml` | Ozone TDE · 키 이름 |
| 13 | `governance/configs/security/ranger.yaml` | Ranger + KMS ACL 인벤토리 |
| 14 | `governance/configs/environments/dev.yaml` | 경로·리소스 확인 |
| 15 | `conf/dev/spark-defaults.conf` | Spark + KMS URI |

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
| **Apache Ranger** | HDFS / HMS / Spark / Ozone **권한은 Ranger 정책만** ([12절](#12-보안--kerberos-인증--ranger-권한)) |
| **Ranger KMS + TDE** | HDFS **`hdfs_encryption_key`** · Ozone **`ozone_encryption_key`** ([HDFS](docs/operations/hdfs-encryption.md) · [Ozone](docs/operations/ozone-encryption.md)) |

### Python 3.11 기본 설정 (`~/.bashrc`)

DEV Jenkins/CloudCat 빌드에서 Python 3.11을 설치한 Gateway에서는, 터미널에서 `python3`·`pip3`가 3.11을 가리키도록 `~/.bashrc`에 alias를 추가합니다.

```bash
# ~/.bashrc 맨 아래에 추가
alias python3='/usr/bin/python3.11'
alias pip3='/usr/bin/python3.11 -m pip'

# 적용
source ~/.bashrc
python3 --version    # Python 3.11.x 확인
which python3        # alias python3='/usr/bin/python3.11'
```

> **참고:** 이 설정은 **Gateway 터미널에서 직접 실행하는** `python3` / `pip3`에 적용됩니다.  
> `spark-submit`·Airflow는 Cloudera Manager가 지정한 Python을 사용할 수 있습니다.

### Ranger 사전 준비 (첫 파이프라인 전)

HDFS / HMS / Spark / Ozone **권한은 Ranger 정책으로만** 부여합니다. Gateway에서 `kinit`만으로는 데이터 접근이 허용되지 않습니다.

**Ranger Admin Web UI**에서 아래 순서대로 등록한 뒤 Gateway 스크립트를 실행하세요.  
상세 UI 절차 → **[7절 — Ranger Web UI 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로)**

| 순서 | Ranger UI | 내용 |
|------|-----------|------|
| 0 | Cloudera Manager | Ozone **`ranger_service`** 활성화 |
| 1 | **Ranger KMS** (`cm_kms`) | **`hdfs_encryption_key`**, **`ozone_encryption_key`** + ACL |
| 2 | **cm_hdfs** | HDFS raw 경로 + Spark event log |
| 3 | **cm_ozone** | `dev_volume_plcy`, `dev_data_bucket_plcy` (volume/bucket **생성**) |
| 4 | **cm_hive** | Storage Handler (iceberg, RW Storage) + (선택) DB 정책 |
| 5 | **cm_hive** + **cm_ozone** | 테이블당 `_db_plcy` + `_uri_plcy` + `_data_{layer}_key_plcy` (×3) |
| 6 | Gateway | `security_check.sh` → HDFS EZ → Ozone bucket → 파이프라인 |

Gateway에서 등록할 정책 전체 목록:

```bash
bash scripts/security/print_ranger_iceberg_pairs.sh
```

상세: [12절](#12-보안--kerberos-인증--ranger-권한) · [Ranger](docs/operations/ranger-authorization.md) · [Iceberg–Ozone Paired Policies](docs/operations/ranger-iceberg-ozone-pairs.md) · [HDFS TDE](docs/operations/hdfs-encryption.md) · [Ozone TDE](docs/operations/ozone-encryption.md)

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

### Step 3: Kerberos 로그인 + Ranger 권한 확인

```bash
# systest.keytab 으로 인증 (Kerberos — 누구인지 증명)
bash scripts/security/kinit_manager.sh

# Kerberos + Ranger HDFS/Ozone 경로 probe (권한 — 접근 허용 여부)
bash scripts/security/security_check.sh
```

**kinit이란?**  
Kerberos(클러스터 보안 시스템)에 "나는 systest 사용자입니다"라고 **인증**하는 과정입니다.  
keytab = 비밀번호 없이 자동 인증하는 열쇠 파일.

**security_check.sh란?**  
유효한 Kerberos 티켓 + HDFS/Ozone **Ranger** probe + Ranger KMS **`hdfs_encryption_key`** / **`ozone_encryption_key`** + **HDFS Encryption Zone** 존재 여부를 확인합니다.  
실패 시 `ranger.yaml` / `hdfs_encryption.yaml` / `ozone_encryption.yaml`을 참고해 정책·키·EZ·암호화 bucket을 점검하세요.

### Step 4: Python 패키지 빌드

```bash
# Spark cluster mode에서 executor가 사용할 zip 파일 생성
bash scripts/deployment/package_python.sh
# → dist/spark_optimal.zip 생성됨
```

### Step 5: 테스트 실행

SDV 실행 전 Python 3.11 alias가 설정되어 있는지 확인하세요 → [5절 Python 3.11 설정](#python-311-기본-설정-bashrc)

```bash
# 소량(0.1GB) SDV 데이터 생성 — 클러스터 없이 Gateway에서 가능
pip install -r requirements/financial.txt
python3 data_gen/generate_financial_json.py --target-gb 0.1
```

여기까지 성공하면 **환경 설정(6절)이 완료**된 것입니다.

**다음 단계 → [7절](#7-금융-데이터-파이프라인-실습-예제)**  
먼저 **[Ranger Web UI에서 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로)** 을 완료한 뒤,  
`setup_hdfs_encryption_zone.sh` + `setup_ozone_encrypted_bucket.sh` 로 **HDFS EZ·암호화 bucket**을 준비하고  
`TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh` 로 첫 파이프라인을 실행하세요.

---

## 7. 금융 데이터 파이프라인 (실습 예제)

SBI 프로젝트의 **대표 예제**: SDV 가짜 금융 JSON 10GB → HDFS → Ozone Bronze/Silver/Gold

> **선행 조건:** [6절 5단계](#6-처음-시작하기--5단계)를 모두 마쳤을 것.  
> 새 터미널을 열었다면 `export SBI_ENV=dev && source config/env.conf` 후 `bash scripts/security/kinit_manager.sh`를 다시 실행하세요 (Kerberos 티켓 만료 시에도 동일).

### 파이프라인 실행 전: Ranger Web UI 정책 등록 (순서대로)

> **담당:** Ranger Admin 권한이 있는 플랫폼/보안 담당자  
> **시점:** HDFS Encryption Zone · Ozone bucket 생성 **전** (아래 TDE 스크립트 실행 전)  
> **Principal:** Gateway `systest@QE-INFRA-AD.CLOUDERA.COM` (Ranger UI 사용자 필드에 `systest` 입력)

#### 전체 순서 요약

| 순서 | Ranger UI | 정책 / 키 | Gateway 다음 단계 |
|------|-----------|-----------|-------------------|
| 0 | Cloudera Manager | Ozone **`ranger_service`** 활성화 | — |
| 1 | **Ranger KMS** (`cm_kms`) | **`hdfs_encryption_key`**, **`ozone_encryption_key`** + ACL | `hadoop key list` |
| 2 | **cm_hdfs** | HDFS raw + Spark event log | `security_check.sh` |
| 3 | **cm_ozone** | `dev_volume_plcy`, `dev_data_bucket_plcy` | `ozone sh volume create dev` |
| 4 | **cm_hive** | Storage Handler + (선택) DB 정책 | — |
| 5 | **cm_hive** + **cm_ozone** | 테이블당 `_db_plcy` + `_uri_plcy` + `_key_plcy` (×3) | `print_ranger_iceberg_pairs.sh` |
| 6 | Gateway | kinit + security_check | — |
| 7 | Gateway | HDFS EZ + Ozone encrypted bucket | `setup_*` 스크립트 |
| 8 | Gateway | 파이프라인 | `run_financial_pipeline.sh` |

인벤토리 YAML: `governance/configs/security/ranger.yaml` · `ranger_iceberg_ozone_pairs.yaml`

---

#### 0. Ranger Admin 접속 + Ozone Ranger 활성화

**Ranger Admin UI (DEV HA):**

| 항목 | 값 |
|------|-----|
| URL | https://ccycloud-1.jshin-sbi.root.comops.site:6182 |
| 대체 | CM → Clusters → **Ranger** → **Ranger Admin Web UI** |
| HA 호스트 | `ccycloud-1`, `ccycloud-10` (port 6182) |

**Ozone Ranger plugin (최초 1회, CM):**

1. Cloudera Manager → **Ozone** → **Configuration**
2. **`ranger_service`** 검색 → **Enable** (또는 `true`)
3. **Ozone** 서비스 **재시작**
4. Ranger Admin → **Service Manager** 에 **`cm_ozone`** 서비스가 보이는지 확인

---

#### 1. Ranger KMS — 암호화 키 (TDE)

Ranger Admin → **Key Manager** (또는 **Encryption** 탭) → 서비스 **`cm_kms`**

**키 1 — `hdfs_encryption_key` (HDFS Encryption Zone)**

1. **Add New Key** (또는 **Create Key**)
2. Key Name: **`hdfs_encryption_key`**
3. Cipher: AES/CTR/NoPadding, 256-bit (기본값)
4. **Add ACL** (키별 권한):

| User / Group | Permissions |
|--------------|-------------|
| **hdfs** (NameNode 서비스 사용자) | Get Metadata, Generate EEK |
| **systest** | Generate EEK, Decrypt EEK |

**키 2 — `ozone_encryption_key` (Ozone bucket TDE)**

1. **Add New Key** → Key Name: **`ozone_encryption_key`**
2. **Add ACL**:

| User / Group | Permissions |
|--------------|-------------|
| **OM** 서비스 사용자 (CM → Ozone → Kerberos principal, 예: `om/_HOST@...`) | Get Metadata, Generate EEK |
| **systest** | Generate EEK, Decrypt EEK |

Gateway 확인:

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
hadoop key list | grep -E 'hdfs_encryption_key|ozone_encryption_key'
```

> KMS URI가 비어 있으면 [13절](#13-sbi-클러스터-접속-정보) fallback URI 또는 `hdfs getconf -confKey hadoop.security.key.provider.path` 사용.

---

#### 2. cm_hdfs — HDFS 경로 정책

Ranger Admin → 상단 **Service Manager** → **`cm_hdfs`** 선택 → **Add New Policy**

**정책 A — Raw ingest (DEV)**

| 필드 | 값 |
|------|-----|
| Policy Name | `dev-raw-financial` |
| Resource: Path | `hdfs://ns1/dev/raw/financial/transactions` |
| Recursive | **ON** |
| Permissions | Read, Write, Execute |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |
| Policy Enabled | **ON** |

**정책 B — Spark event log (선택, 권장)**

| 필드 | 값 |
|------|-----|
| Policy Name | `dev-spark-eventlog` |
| Resource: Path | `hdfs:///user/spark/applicationHistory` |
| Recursive | **ON** |
| Permissions | Read, Write, Execute |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

**Add** 클릭 후 정책이 **Enabled** 상태인지 확인 (수 초~1분 후 Gateway에 반영).

---

#### SBI 정책 명명 규칙 (cm_hive · cm_ozone)

SBI DLH 운영 클러스터와 동일한 패턴을 사용합니다.

| 서비스 | 유형 | 패턴 | DEV 예시 | PROD 예시 |
|--------|------|------|----------|-----------|
| **cm_hive** | SQL table | `{env}_{table}_db_plcy` | `dev_brnz_transactions_db_plcy` | `prd_gld_bidetl_db_plcy` |
| **cm_hive** | URL | `{env}_{table}_uri_plcy` | `dev_brnz_transactions_uri_plcy` | `prd_gld_bidetl_uri_plcy` |
| **cm_ozone** | volume | `{env}_volume_plcy` | `dev_volume_plcy` | `prod_volume_plcy` |
| **cm_ozone** | bucket | `{env}_data_bucket_plcy` | `dev_data_bucket_plcy` | `prod_data_bucket_plcy` |
| **cm_ozone** | key/layer | `{env}_data_{layer}_key_plcy` | `dev_data_brnz_key_plcy` | `prod_data_brnz_key_plcy` |

**테이블당 SBI triple:** `_db_plcy` + `_uri_plcy` + `_data_{layer}_key_plcy`

**Role (PROD):** `SBI_ETLAdmin_Role`, `SBI_ETLUsers_RW_Role`, `SBI_ETLTester_RO_Role`  
**DEV Gateway:** User 필드에 `systest` 직접 추가 (또는 `SBI_ETLUsers_RW_Role` 부여)

---

#### 3. cm_ozone — volume / bucket 인프라 (생성 권한)

`ozone sh volume create dev` 또는 `setup_ozone_encrypted_bucket.sh` **전에** 등록합니다.  
테이블별 cm_ozone 정책(5단계)과 **별도**입니다.

Ranger Admin → **Service Manager** → **`cm_ozone`** → **Add New Policy**

**정책 A — volume 생성**

| 필드 | 값 |
|------|-----|
| Policy Name | **`dev_volume_plcy`** |
| volume | `dev` |
| bucket | `*` |
| key | `*` |
| Permissions | **Read, Write, Create** |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

**정책 B — bucket 생성**

| 필드 | 값 |
|------|-----|
| Policy Name | **`dev_data_bucket_plcy`** |
| volume | `dev` |
| bucket | **`data`** |
| key | `*` |
| Permissions | **Read, Write, Create, Delete** |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

Gateway 확인:

```bash
ozone sh volume create dev          # 이미 있으면 skip
ozone sh bucket create -k ozone_encryption_key dev/data
```

> **`PERMISSION_DENIED ... CREATE permission ... volume Volume:dev`** → `dev_volume_plcy` 정책 미등록. 위 정책 A를 추가하세요.

---

#### 4. cm_hive — 클러스터 공통 (Hadoop SQL, 1회)

Ranger UI의 **Hadoop SQL** 서비스 이름은 **`cm_hive`** 입니다 (Hive / Impala / Hue / Spark SQL 공용).

**4-a. Storage Handler (필수, 1회)**

Ranger → **`cm_hive`** → 기본 정책 **`all - storage-type, storage-url`** 찾기 → **Edit** (연필 아이콘)

| 필드 | 값 |
|------|-----|
| storage-type | `iceberg` |
| storage-url | `*` (Include) |
| Permissions | **RW Storage** |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

> RW Storage는 CREATE/ALTER **location** 만 허용합니다. 테이블 **데이터** 접근은 5단계 SQL + URL + cm_ozone 정책이 필요합니다.

**4-b. Database 정책 (선택, 권장)**

Ranger → **`cm_hive`** → **Add New Policy**

| 필드 | 값 |
|------|-----|
| Policy Name | `dev_sbi_financial_db_plcy` |
| database | `sbi_financial` |
| table | `*` |
| column | `*` |
| Permissions | Create, Select, Update, Alter, Drop, Index, Lock, All |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

---

#### 5. 테이블별 Iceberg-on-Ozone 정책 (×3)

Medallion 테이블마다 **cm_hive 2개 + cm_ozone 1개** (Storage Handler는 4-a에서 1회)를 등록합니다.  
**SBI triple:** `_db_plcy` + `_uri_plcy` + `_data_{layer}_key_plcy`

| 테이블 | cm_hive SQL | cm_hive URL | cm_ozone key | Ozone 경로 |
|--------|-------------|-------------|--------------|------------|
| `brnz_transactions` | `dev_brnz_transactions_db_plcy` | `dev_brnz_transactions_uri_plcy` | `dev_data_brnz_key_plcy` | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| `slvr_transactions` | `dev_slvr_transactions_db_plcy` | `dev_slvr_transactions_uri_plcy` | `dev_data_slvr_key_plcy` | `ofs://ozone1782570080/dev/data/slvr/transactions` |
| `gld_daily_report` | `dev_gld_daily_report_db_plcy` | `dev_gld_daily_report_uri_plcy` | `dev_data_gld_key_plcy` | `ofs://ozone1782570080/dev/data/gld/daily_transaction_report` |

아래는 **`brnz_transactions`** 예시입니다. 나머지 2개 테이블도 동일 패턴으로 반복하세요.

**5-a. cm_hive SQL table 정책 (`_db_plcy`)**

Ranger → **`cm_hive`** → **Add New Policy**

| 필드 | 값 |
|------|-----|
| Policy Name | `dev_brnz_transactions_db_plcy` |
| database | `sbi_financial` |
| table | `brnz_transactions` |
| column | `*` |
| Permissions | Select, Update, Create, Drop, Alter, Index, Lock, All |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

**5-b. cm_hive URL 정책 (`_uri_plcy`)**

Ranger → **`cm_hive`** → **Add New Policy** → 리소스 타입 **URL** 선택

| 필드 | 값 |
|------|-----|
| Policy Name | `dev_brnz_transactions_uri_plcy` |
| URL | `ofs://ozone1782570080/dev/data/brnz/transactions` |
| Permissions | Read, Write |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role` |

**5-c. cm_ozone key 정책 (`_data_{layer}_key_plcy`)**

Ranger → **`cm_ozone`** → **Add New Policy**

| 필드 | 값 |
|------|-----|
| Policy Name | `dev_data_brnz_key_plcy` |
| volume | `dev` |
| bucket | `data` |
| key | `brnz/transactions` |
| Permissions | Read, Write, Create, Delete |
| Allow Users / Roles | `systest` 또는 `SBI_ETLUsers_RW_Role`, `SBI_ETLAdmin_Role` |

Gateway에서 전체 목록 확인:

```bash
export SBI_ENV=dev && source config/env.conf
bash scripts/security/print_ranger_iceberg_pairs.sh
```

Cloudera 공식 참고: [Iceberg Ranger](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/spark-iceberg/topics/iceberg-ranger-introduction.html) · [Ozone policy](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/iceberg-how-to/topics/iceberg-ozone-policy.html)

---

#### 6. Gateway — 정책 반영 확인

Ranger 정책 등록 후 Gateway에서:

```bash
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh
bash scripts/security/security_check.sh
```

모든 probe가 OK이면 아래 **HDFS EZ + Ozone bucket** 단계로 진행합니다.

---

### 최초 실행 전: HDFS **Encryption Zone** + Ozone **암호화** bucket (Ranger KMS TDE)

**HDFS Raw**은 **`hdfs_encryption_key`**, **Medallion Ozone**은 **`ozone_encryption_key`** 로 **저장 시 암호화(TDE)** 됩니다.

| 항목 | HDFS | Ozone |
|------|------|-------|
| Ranger KMS service | `cm_kms` | `cm_kms` |
| Encryption key | **`hdfs_encryption_key`** | **`ozone_encryption_key`** |
| 적용 방식 | **Encryption Zone** (`hdfs crypto -createZone`) | bucket `--bucketkey` |
| DEV 경로 | `/dev/raw/financial/transactions` | `ofs://.../dev/data/brnz\|slvr\|gld/` |

```bash
# 한 번에: HDFS EZ + Ozone KMS 암호화 bucket 생성/검증
export SBI_ENV=dev
source config/env.conf
bash scripts/security/kinit_manager.sh
bash scripts/infrastructure/setup_hdfs_encryption_zone.sh
bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh
```

HDFS 수동 (빈 디렉터리에만 EZ 생성 가능):

```bash
hadoop key list | grep hdfs_encryption_key
hdfs dfs -mkdir -p /dev/raw/financial/transactions
hdfs crypto -createZone -keyName hdfs_encryption_key -path /dev/raw/financial/transactions
hdfs crypto -getZone /dev/raw/financial/transactions
```

Ozone 수동 (암호화 bucket — **키는 생성 시에만** 지정 가능):

```bash
ozone sh volume create dev
ozone sh bucket create -k ozone_encryption_key dev/data
```

> **주의:** EZ는 **빈 경로**에만 생성 · Ozone bucket도 `--bucketkey` 없이 만든 경우 **재생성** 필요.  
> **Ranger KMS:** hdfs → **`hdfs_encryption_key`** · OM → **`ozone_encryption_key`** · systest → 두 키 모두 Generate/Decrypt EEK

상세: [HDFS Encryption](docs/operations/hdfs-encryption.md) · [Ozone Encryption](docs/operations/ozone-encryption.md) · [Ranger](docs/operations/ranger-authorization.md)

Jenkins/CloudCat **재빌드** 후 `ofs_uri`, `hms_*`, **Ranger/KMS 정책**도 동일 경로·키에 맞게 갱신하세요.

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
> 환경·설정 상세는 [10~11절](#10-환경별-설정-dev--uat--prod), Kerberos/Ranger는 [12절](#12-보안--kerberos-인증--ranger-권한), 오류는 [14절 FAQ](#14-문제-해결-faq)를 참고하세요.

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

1. `kinit` (Kerberos **인증**)
2. `security_check.sh` (Kerberos + Ranger HDFS/Ozone **권한** probe)
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
| `config/env.conf` | Gateway Kerberos, JAR, **`HDFS_ENCRYPTION_KEY`**, **`OZONE_ENCRYPTION_KEY`**, **`KMS_PROVIDER_URI`**, Medallion 경로 |
| `scripts/deployment/verify_jars.sh` | `ICEBERG_JAR`·`SPARK_OZONE_JARS` 파일 존재 검증 |
| `scripts/deployment/package_python.sh` | Spark cluster mode용 `dist/spark_optimal.zip` 생성 |
| `governance/configs/infrastructure/dev_jenkins_build.yaml` | DEV Jenkins/CloudCat 재빌드 파라미터 (GBN, OPTIONAL_ARGS) |
| `governance/configs/security/ranger.yaml` | **Ranger 권한** 인벤토리 (HDFS/Ozone/Hive/Spark — Ranger only) |
| `governance/configs/security/hdfs_encryption.yaml` | **HDFS TDE** — Encryption Zone, `hdfs_encryption_key` |
| `governance/configs/security/ozone_encryption.yaml` | **Ozone TDE** — `ozone_encryption_key`, bucket `--bucketkey` |

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

authorization:
  model: ranger          # HDFS/HMS/Spark/Ozone — Ranger only
  policy_inventory: governance/configs/security/ranger.yaml

ozone_encryption:
  enabled: true
  encryption_key: ozone_encryption_key   # Ranger KMS (cm_kms)
  volume: dev
  bucket: data

hdfs_encryption:
  enabled: true
  encryption_key: hdfs_encryption_key   # Ranger KMS (cm_kms) — HDFS only
```

### 환경 전환 체크리스트

```bash
export SBI_ENV=dev          # 또는 uat / prod
source config/env.conf      # ${SBI_ENV} 경로 반영
echo $SPARK_CONF_DIR          # .../conf/dev 확인
echo $HDFS_FINANCIAL_RAW      # .../dev/raw/... 확인
bash scripts/deployment/verify_jars.sh   # CDP parcel JAR 경로 일치 확인
bash scripts/security/kinit_manager.sh   # Kerberos 인증
bash scripts/security/security_check.sh  # Ranger + KMS + HDFS EZ probe
bash scripts/infrastructure/setup_hdfs_encryption_zone.sh   # HDFS EZ (7절)
bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh  # encrypted bucket (7절)
echo $HDFS_ENCRYPTION_KEY $OZONE_ENCRYPTION_KEY $KMS_PROVIDER_URI
```

**CDP parcel / GBN 업그레이드 후:** `ls /opt/cloudera/parcels/CDH/jars/iceberg-spark-runtime*.jar`로 실제 파일명 확인 → `env.conf` 수정 → `verify_jars.sh` 재실행 (자동 JAR 선택 스크립트는 제공하지 않음).

---

## 12. 보안 — Kerberos (인증) + Ranger (권한)

> **초보자:** [6절 Step 3](#step-3-kerberos-로그인-kinit)에서 kinit을 실행합니다. 아래는 **인증·권한 분리**와 Delegation Token 상세입니다.

SBI 클러스터는 **Kerberos + Auto-TLS**로 **인증**하고, **HDFS / HMS / Spark / Ozone** 접근은 **Apache Ranger 정책으로만** **권한**을 부여합니다.

| 구분 | 담당 | spark-optimal에서 |
|------|------|-------------------|
| **인증 (Authentication)** | Kerberos — `systest` keytab, kinit | `kinit_manager.sh`, Delegation Token |
| **권한 (Authorization)** | **Ranger only** | `governance/configs/security/ranger.yaml` |
| **HDFS TDE (Encryption)** | **Ranger KMS** — key `hdfs_encryption_key` | `hdfs_encryption.yaml`, `setup_hdfs_encryption_zone.sh` |
| **Ozone TDE (Encryption)** | **Ranger KMS** — key `ozone_encryption_key` | `ozone_encryption.yaml`, `setup_ozone_encrypted_bucket.sh` |

**금지:** `chmod`, `chown`, `setfacl`, `hdfs dfs -chmod`, Ozone native ACL로 접근 제어  
**허용:** Ranger UI로 HDFS/Ozone/Hive 정책 · Ranger KMS UI로 **`hdfs_encryption_key`** / **`ozone_encryption_key`** ACL · HDFS EZ · bucket `-k`  
**Web UI 등록 절차:** [7절 — Ranger Web UI 정책 등록 (순서대로)](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로)

상세: [Ranger Authorization](docs/operations/ranger-authorization.md) · [HDFS Encryption (TDE)](docs/operations/hdfs-encryption.md) · [Ozone Encryption (TDE)](docs/operations/ozone-encryption.md)

### 매 배치 실행 전 체크리스트

- [ ] `bash scripts/security/kinit_manager.sh` 실행 (Kerberos 인증)
- [ ] `bash scripts/security/security_check.sh` 통과 (Kerberos + Ranger 경로 probe)
- [ ] **Iceberg-on-Ozone Ranger (Cloudera + SBI)** — Storage Handler + `{env}_{table}_db_plcy` + `_uri_plcy` + `{env}_data_{layer}_key_plcy`
- [ ] cm_ozone 인프라 — `{env}_volume_plcy` + `{env}_data_bucket_plcy`
- [ ] HDFS Encryption Zone on `/{env}/raw/financial/transactions` (key **`hdfs_encryption_key`**)
- [ ] Ozone bucket `/{env}/data` created with **`--bucketkey ozone_encryption_key`**
- [ ] Ranger KMS ACL — **`hdfs_encryption_key`** (hdfs + systest) · **`ozone_encryption_key`** (OM + systest)
- [ ] `spark_submit.sh` 사용 (직접 spark-submit 금지 — Delegation Token 누락 위험)
- [ ] cluster deploy mode 사용 (local mode 금지)

### 필수 Ranger 리소스 (DEV 예시)

`governance/configs/security/ranger.yaml`에 전체 목록이 있습니다.  
**Ranger Web UI 등록 절차 (순서·필드값):** [7절 — Ranger Web UI 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로)

| Ranger 서비스 | 리소스 | spark-optimal 용도 |
|---------------|--------|-------------------|
| **cm_hdfs** | `hdfs://ns1/dev/raw/financial/transactions` | Raw JSON 업로드 |
| **cm_hdfs** | `hdfs:///user/spark/applicationHistory` | Spark event log |
| **cm_hive** | Storage Handler — `iceberg`, RW Storage | CREATE/ALTER table location (cluster) |
| **cm_hive** | `{env}_{table}_db_plcy` + `{env}_{table}_uri_plcy` | Iceberg SQL + ofs:// table path |
| **cm_ozone** | **`dev_volume_plcy`**, **`dev_data_bucket_plcy`** | volume/bucket **생성** |
| **cm_ozone** | `{env}_data_{layer}_key_plcy` | OFS data files (brnz/slvr/gld layer) |
| **Ranger KMS** | **`hdfs_encryption_key`** · **`ozone_encryption_key`** | HDFS EZ · Ozone TDE |

> **Cloudera CDP 7.3.1 + SBI naming:** 테이블당 cm_hive **Storage Handler + _db_plcy + _uri_plcy** + cm_ozone **_data_{layer}_key_plcy**.  
> 출력: `bash scripts/security/print_ranger_iceberg_pairs.sh` · [SBI naming guide](docs/operations/ranger-iceberg-ozone-pairs.md)

UAT/PROD는 `environments/{uat,prod}.yaml`의 `medallion`·`hdfs_encryption`·`ozone_encryption` 경로에 맞춰 동일 패턴으로 정책을 추가합니다.

### HDFS Transparent Data Encryption (TDE)

- KMS key: **`hdfs_encryption_key`** (Ranger KMS UI → service `cm_kms`)
- EZ 생성: `hdfs crypto -createZone -keyName hdfs_encryption_key -path /{env}/raw/financial/transactions`
- 설정: `governance/configs/security/hdfs_encryption.yaml`

상세: [docs/operations/hdfs-encryption.md](docs/operations/hdfs-encryption.md)

### Ozone Transparent Data Encryption (TDE)

- KMS key: **`ozone_encryption_key`** (Ranger KMS UI → service `cm_kms`)
- Bucket `/{env}/data` 생성: `ozone sh bucket create -k ozone_encryption_key {env}/data`
- Spark: `KMS_PROVIDER_URI` + `spark.hadoop.hadoop.security.key.provider.path` (env.conf / spark-defaults)
- 설정: `governance/configs/security/ozone_encryption.yaml`

상세: [docs/operations/ozone-encryption.md](docs/operations/ozone-encryption.md)

### Delegation Token이란?

Gateway에서 kinit한 **Kerberos 인증** 정보를 **Spark executor(워커)**까지 전달하는 토큰입니다.  
Executor는 `systest` 신원으로 HDFS/Ozone/Hive에 접근을 **시도**하지만, **Ranger가 최종 허용/거부**합니다.

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
| **Ranger Admin** (HA) | `ccycloud-1.jshin-sbi.root.comops.site:6182`, `ccycloud-10.jshin-sbi.root.comops.site:6182` |
| **Ranger KMS** (HA) | `ccycloud-1.jshin-sbi.root.comops.site:9494`, `ccycloud-10.jshin-sbi.root.comops.site:9494` |
| Ranger Admin UI | https://ccycloud-1.jshin-sbi.root.comops.site:6182 (CM Quick Links 권장) |
| KMS provider URI (fallback) | `kms://https@ccycloud-1.jshin-sbi.root.comops.site:9494/kms` |
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

### Q. `ozone sh volume create dev` → `PERMISSION_DENIED ... CREATE permission ... volume Volume:dev`

**Ranger Ozone 인프라 정책**이 없습니다. Kerberos는 성공했지만 `cm_ozone`에 volume CREATE 권한이 없습니다.

Ranger Admin → **`cm_ozone`** → 정책 **`dev_volume_plcy`** 등록:

| volume | bucket | key | Permissions | User |
|--------|--------|-----|-------------|------|
| `dev` | `*` | `*` | Read, Write, **Create** | `systest` |

bucket 생성 전 **`dev_data_bucket_plcy`** 정책도 필요합니다 → [7절 Ranger Web UI — cm_ozone 인프라](#3-cm_ozone--volume--bucket-인프라-생성-권한)

### Q. `hdfs dfs -ls ofs://ozone1782570080/` 결과가 비어 있음

**정상입니다.** Ozone 루트에는 volume이 바로 보이지 않을 수 있습니다.  
Medallion 데이터는 `ofs://ozone1782570080/dev/data/...` 아래에 저장됩니다.

```bash
ozone sh volume list
ozone sh bucket list dev
```

volume `dev`, bucket `data`가 없으면 [7절 Ranger Web UI 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로) → [HDFS EZ + Ozone 암호화 준비](#최초-실행-전-hdfs-encryption-zone--ozone-암호화-bucket-ranger-kms-tde) 순으로 진행하세요.

### Q. Spark/HDFS/Ozone write fails with KMS / encryption / `GenerateEEK` error

**Ranger KMS, HDFS Encryption Zone, 또는 Ozone 암호화 bucket** 문제입니다.

```bash
echo $HDFS_ENCRYPTION_KEY $OZONE_ENCRYPTION_KEY $KMS_PROVIDER_URI
hadoop key list | grep -E 'hdfs_encryption_key|ozone_encryption_key'
bash scripts/infrastructure/setup_hdfs_encryption_zone.sh
bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh
bash scripts/security/security_check.sh
```

| 확인 | 조치 |
|------|------|
| `KMS_PROVIDER_URI` unset | `export HADOOP_CONF_DIR=/etc/hadoop/conf` 후 `hdfs getconf` → env.conf. fallback: `kms://https@ccycloud-1.jshin-sbi.root.comops.site:9494/kms` |
| `hadoop key list`에 키 없음 | Ranger KMS UI (`cm_kms`)에서 **`hdfs_encryption_key`** / **`ozone_encryption_key`** 생성 |
| HDFS EZ 없음 / wrong key | `hdfs crypto -getZone ...` → **`hdfs_encryption_key`** 로 setup_hdfs_encryption_zone.sh |
| bucket이 `--bucketkey` 없이 생성됨 | bucket 재생성: `--bucketkey ozone_encryption_key` |
| KMS ACL 없음 | **`hdfs_encryption_key`**: hdfs+systest · **`ozone_encryption_key`**: OM+systest |

상세: [docs/operations/hdfs-encryption.md](docs/operations/hdfs-encryption.md) · [docs/operations/ozone-encryption.md](docs/operations/ozone-encryption.md)

### Q. `Permission denied` / `AccessControlException` (Kerberos 티켓은 유효)

**Ranger 권한 문제**입니다. kinit은 성공했지만 Ranger 정책에 `systest` principal이 없거나 경로가 다릅니다.

```bash
klist -s && echo "ticket OK"
bash scripts/security/security_check.sh
# 실패 시 governance/configs/security/ranger.yaml 경로에 Ranger 정책 요청
```

`chmod`, `chown`, `setfacl`, `hdfs dfs -chmod` **사용 금지** — SBI 정책상 **Ranger만** 권한 부여.

Ranger Web UI 등록: [7절 — Ranger Web UI 정책 등록](#파이프라인-실행-전-ranger-web-ui-정책-등록-순서대로) · [docs/operations/ranger-authorization.md](docs/operations/ranger-authorization.md)

### Q. Spark Job은 시작됐는데 executor에서 Ozone 접근 실패

`spark.yarn.access.hadoopFileSystems`에 OFS URI가 포함되어 있는지 확인:

```bash
grep access.hadoopFileSystems conf/${SBI_ENV}/spark-defaults.conf
# hdfs://ns1,ofs://ozone1782570080 이 있어야 함
```

Delegation Token 설정이 맞아도 **Ranger Ozone 정책**이 없으면 executor에서 실패합니다 → `governance/configs/security/ranger.yaml`의 Ozone prefix 확인.

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
| **kinit** | Kerberos 티켓 발급 명령 (**인증** — 누구인지 증명) |
| **keytab** | kinit 없이 자동 인증하는 Kerberos 열쇠 파일 |
| **Ranger** | HDFS/HMS/Spark/Ozone **권한(authorization)** — SBI는 **Ranger만** 사용 |
| **Ranger KMS** | 암호화 키 저장소 — Ozone TDE key **`ozone_encryption_key`** |
| **Ozone TDE** | bucket 생성 시 `--bucketkey`로 저장 데이터 암호화 (읽기/쓰기 시 자동 복호화) |
| **Delegation Token** | Gateway **인증** 정보를 executor까지 전달 (권한은 Ranger, Ozone 암호화는 KMS) |
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
3. **Ranger + KMS** — HDFS/HMS/Spark/Ozone **Ranger 정책** + **`hdfs_encryption_key`** / **`ozone_encryption_key`** KMS ACL
   - `governance/configs/security/ranger.yaml` · `hdfs_encryption.yaml` · `ozone_encryption.yaml`
   - `bash scripts/infrastructure/setup_hdfs_encryption_zone.sh`
   - `bash scripts/infrastructure/setup_ozone_encrypted_bucket.sh`
   - `bash scripts/security/security_check.sh`
4. **YARN 용량 동기화** — 실측 후 `governance/configs/environments/dev.yaml` 갱신
   ```bash
   yarn node -list -all
   # cluster.total_vcores / total_memory_gb / node_managers 수정
   ```
5. **검증**
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
| [Ranger Authorization](docs/operations/ranger-authorization.md) | **Ranger-only** cm_hdfs / cm_hive / cm_ozone 권한 |
| [Ranger Iceberg–Ozone Pairs](docs/operations/ranger-iceberg-ozone-pairs.md) | **Cloudera CDP 7.3.1** — cm_hive + cm_ozone Iceberg on Ozone |
| [HDFS Encryption (TDE)](docs/operations/hdfs-encryption.md) | Ranger KMS **`hdfs_encryption_key`** · HDFS Encryption Zone |
| [Ozone Encryption (TDE)](docs/operations/ozone-encryption.md) | Ranger KMS `ozone_encryption_key` · encrypted bucket |
| [DEV Jenkins Rebuild](docs/infrastructure/dev-jenkins-rebuild.md) | DEV CloudCat/Jenkins 재빌드 |
| [Python API](docs/api/python-api.md) | Python API 레퍼런스 |
| [Troubleshooting](docs/troubleshooting/common-issues.md) | 장애 대응 |

---

## 테스트 실행

```bash
pip install -r requirements/dev.txt
pytest tests/ -q --ignore=tests/integration
# 41 tests (환경·Medallion·Cloudera Ranger Iceberg/Ozone·HDFS/Ozone TDE 포함)
```

---

## 라이선스 / 문의

SBI Bank 내부 프로젝트 — Cloudera CDP 7.3.1 환경 전용

문제 발생 시: `docs/troubleshooting/common-issues.md` → 데이터 플랫폼 팀 Escalation
