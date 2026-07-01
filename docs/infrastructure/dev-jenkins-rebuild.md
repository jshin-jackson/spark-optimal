# DEV Jenkins / CloudCat 클러스터 재빌드 가이드

SBI **spark-optimal DEV** 환경은 Cloudera 내부 **Jenkins + CloudCat** 으로 프로비저닝됩니다.  
클러스터를 삭제·재생성해야 할 때 아래 파라미터를 그대로 사용하면 동일 구성으로 다시 빌드할 수 있습니다.

---

## 빌드 파라미터 (원본 기록)

| Jenkins / CloudCat 파라미터 | 값 |
|-----------------------------|-----|
| **CLUSTER_SHORTNAME** | `ccycloud-{1..10}.jshin-sbi` |
| **CM_VERSION** | `gbn://74798696` |
| **CDH** | `gbn://74774806` |
| **CLOUDCAT_OS** | `redhat96` |
| **CLOUDCAT_BUDGET** | `--ycloud-queue=professional-services` |
| **DB** | `postgresql` |
| **KERBEROS** | `AD KERBEROS` |
| **JAVA_VERSION** | `17.0.11-openjdk` |
| **OPTIONAL_ARGS** | 아래 참조 |

### OPTIONAL_ARGS (전체)

```
--install-python-version=3.11 --include-service-types=ZOOKEEPER,HDFS,HBASE,HIVE,YARN,SPARK3_ON_YARN,IMPALA,OOZIE,HUE,LIVY,SOLR,RANGER,RANGER_KMS,KAFKA,KUDU,ATLAS,KNOX,TEZ,HIVE_ON_TEZ,SCHEMAREGISTRY,STREAMS_MESSAGING_MANAGER,STREAMS_REPLICATION_MANAGER,OZONE --ha-service-types=ALL
```

### 호스트 목록 (shortname)

`ccycloud-1.jshin-sbi` … `ccycloud-10.jshin-sbi` (총 10노드)

운영 FQDN 예: `ccycloud-1.jshin-sbi.root.comops.site` (CM 등록 후 확인)

---

## 설정 파일 (기계 가독)

동일 정보의 YAML:

```
governance/configs/infrastructure/dev_jenkins_build.yaml
```

Jenkins 파라미터를 터미널에 출력:

```bash
bash scripts/infrastructure/print_dev_jenkins_params.sh
```

---

## 재빌드 절차 (권장)

### 1. Jenkins에서 DEV 클러스터 Job 실행

1. Cloudera 내부 Jenkins에서 **CloudCat / yCloud DEV 프로비저닝 Job** 선택
2. 위 표의 파라미터 입력 (또는 `print_dev_jenkins_params.sh` 출력 복사)
3. 빌드 완료까지 대기 (CM + CDH + 서비스 배포)

> Jenkins Job URL·Job 이름은 팀 내부 Confluence/Jenkins catalog에서 확인하세요.  
> 이 저장소에는 GBN·OPTIONAL_ARGS 등 **재현에 필요한 빌드 스펙**만 보관합니다.

### 2. 클러스터 기동 후 Gateway 설정

```bash
export SBI_ENV=dev
cp config/env.template.conf config/env.conf
# CM에서 확인한 HMS/Ozone 주소가 바뀌었으면 env.conf 및 dev.yaml 갱신
source config/env.conf
bash scripts/security/kinit_manager.sh
```

### 3. YARN 용량 확인 → dev.yaml 동기화

재빌드 후 NodeManager 수·vCore·Memory가 달라질 수 있습니다.

```bash
yarn node -list -all
```

`governance/configs/environments/dev.yaml` 의 `cluster` 섹션을 실측 값으로 수정:

```yaml
cluster:
  total_vcores: 72      # yarn node -list 기준 합산
  total_memory_gb: 288
  node_managers: 9
  cores_per_node: 16
  memory_per_node_gb: 32
```

Spark executor 상한은 `resource_limits` 및 `conf/dev/spark-defaults.conf` 에서 조정.

### 4. spark-optimal 검증

```bash
pip install -r requirements/dev.txt
pytest tests/ -q --ignore=tests/integration

# Ranger 정책 적용 후
bash scripts/security/security_check.sh
TARGET_GB=0.1 bash scripts/pipeline/run_financial_pipeline.sh
```

---

## 포함 서비스 (OPTIONAL_ARGS 기준)

| 서비스 | spark-optimal 사용 |
|--------|-------------------|
| HDFS, YARN, Spark3 on YARN | 필수 |
| Hive, Hive on Tez | Iceberg / HMS |
| Ozone | Medallion 저장소 |
| Ranger, Knox, Kerberos (AD) | **권한(Ranger) + 인증(Kerberos)** — HDFS/HMS/Spark/Ozone |

재빌드 후 Ranger baseline: `governance/configs/security/ranger.yaml` → [Ranger Authorization](../operations/ranger-authorization.md)

| ZooKeeper, HBase, Kafka, … | DEV 풀스택 (HA) |

`--ha-service-types=ALL` — HA 구성으로 배포됩니다.

---

## 버전 업그레이드 시

| 변경 대상 | 수정 파일 |
|-----------|-----------|
| CM / CDH GBN | `dev_jenkins_build.yaml` → `cm_version`, `cdh_version` |
| Python / 서비스 목록 | `dev_jenkins_build.yaml` → `optional_args` |
| Spark executor 튜닝 | `environments/dev.yaml`, `conf/dev/spark-defaults.conf` |

GBN을 올릴 때는 Jenkins 빌드 **전에** CDP 7.3.x 호환성( Spark 3.5, Iceberg, Ozone JAR 경로)을 확인하세요.

---

## 관련 문서

- [README — DEV 클러스터 Spark 설정](../../README.md#10-환경별-설정-dev--uat--prod)
- [Gateway Runbook](../operations/gateway-runbook.md)
- `governance/configs/environments/dev.yaml` — 런타임 Spark/YARN/Medallion 설정
