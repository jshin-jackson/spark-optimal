"""
SBI 금융 Medallion 파이프라인 — Airflow DAG

[이 DAG가 하는 일]
  매일(또는 설정된 스케줄) 금융 거래 데이터 파이프라인 전체를 자동 실행합니다.

[실행 흐름]
  start
    → security_precheck     (kinit + 보안 점검)
    → data_ingestion        (SDV JSON 생성 → HDFS 업로드)
    → spark_medallion       (HDFS→Bronze → Bronze→Silver→Gold)
    → collect_metrics       (SLA/처리량 메트릭 수집)
  → end

[수동 실행]
  airflow dags trigger sbi_financial_medallion

[설정]
  Airflow Variables (scripts/airflow/import_variables.sh 로 등록):
    spark_optimal_home, sbi_env, financial_target_gb, hdfs_financial_raw, ...
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from common.sbi_config import SBIAirflowConfig

# Airflow Variable / 환경 변수에서 공통 설정 로드
cfg = SBIAirflowConfig.from_airflow()

# DAG 기본 인자 — 모든 Task에 공통 적용
default_args = {
    "owner": "sbi-data-platform",
    "depends_on_past": False,           # 이전 실행 성공 여부와 무관하게 실행
    "email_on_failure": bool(cfg.email_on_failure),
    "email": [cfg.email_on_failure] if cfg.email_on_failure else [],
    "retries": 2,                       # 실패 시 2회 재시도
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
}

with DAG(
    dag_id="sbi_financial_medallion",
    description="SDV JSON -> HDFS -> Ozone Bronze -> Silver -> Gold (SBI)",
    schedule=cfg.schedule_financial,   # 기본: 매일 02:00 (0 2 * * *)
    start_date=datetime(2026, 1, 1),
    catchup=False,                     # 과거 날짜 backfill 안 함
    max_active_runs=1,                 # 동시 실행 1개만 (리소스 보호)
    tags=["sbi", "financial", "medallion", "spark"],
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ── Task 1: Kerberos 티켓 + 환경 점검 ──
    security_precheck = BashOperator(
        task_id="security_precheck",
        bash_command=(
            f"{cfg.bash_prefix()} && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/security/kinit_manager.sh && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/security/security_check.sh"
        ),
    )

    # ── Task Group: 데이터 수집 (로컬 생성 → HDFS) ──
    with TaskGroup(group_id="data_ingestion") as data_ingestion:
        generate_sdv_json = BashOperator(
            task_id="generate_sdv_json",
            bash_command=(
                f"{cfg.bash_prefix()} && "
                f"pip install -q -r ${{SPARK_OPTIMAL_HOME}}/requirements/financial.txt && "
                f"python3 ${{SPARK_OPTIMAL_HOME}}/data_gen/generate_financial_json.py "
                f"--output-dir '{cfg.local_output}' "
                f"--target-gb {cfg.target_gb}"
            ),
        )

        upload_to_hdfs = BashOperator(
            task_id="upload_to_hdfs",
            bash_command=(
                f"{cfg.bash_prefix()} && "
                f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/data/upload_to_hdfs.sh "
                f"'{cfg.local_output}' '{cfg.hdfs_raw}'"
            ),
        )
        # SDV 생성 완료 후 HDFS 업로드
        generate_sdv_json >> upload_to_hdfs

    # ── Task Group: Spark Medallion (Bronze → Silver → Gold) ──
    with TaskGroup(group_id="spark_medallion") as spark_medallion:
        hdfs_to_bronze = BashOperator(
            task_id="hdfs_json_to_bronze",
            bash_command=(
                f"{cfg.bash_prefix()} && "
                f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/submit/spark_submit.sh migration "
                f"--py-file ${{SPARK_OPTIMAL_HOME}}/jobs/migration/hdfs_json_to_bronze_job.py "
                f"--project sbi_financial "
                f"--job hdfs_json_to_bronze "
                f"--source-path '{cfg.hdfs_raw}' "
                f"--data-size-gb {cfg.target_gb}"
            ),
            execution_timeout=timedelta(hours=4),
        )

        bronze_to_report = BashOperator(
            task_id="bronze_to_silver_gold",
            bash_command=(
                f"{cfg.bash_prefix()} && "
                f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/submit/spark_submit.sh etl "
                f"--py-file ${{SPARK_OPTIMAL_HOME}}/jobs/etl/bronze_to_report_job.py "
                f"--project sbi_financial "
                f"--job bronze_to_report "
                f"--data-size-gb {cfg.target_gb}"
            ),
            execution_timeout=timedelta(hours=3),
        )
        # Bronze 적재 완료 후 Silver/Gold ETL
        hdfs_to_bronze >> bronze_to_report

    # ── Task: 메트릭 수집 (성공/실패 무관하게 실행) ──
    collect_metrics = BashOperator(
        task_id="collect_metrics",
        bash_command=f"{cfg.bash_prefix()} && bash ${{SPARK_OPTIMAL_HOME}}/scripts/monitoring/collect_metrics.sh",
        trigger_rule="all_done",  # upstream 실패해도 메트릭은 수집
    )

    # DAG 의존성 정의
    start >> security_precheck >> data_ingestion >> spark_medallion >> collect_metrics >> end
