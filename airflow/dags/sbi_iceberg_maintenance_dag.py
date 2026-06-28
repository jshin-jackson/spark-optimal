"""SBI Iceberg maintenance — scheduled compaction and snapshot expiry."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from common.sbi_config import SBIAirflowConfig

cfg = SBIAirflowConfig.from_airflow()

BRONZE_TABLE = "spark_catalog.sbi_financial.brnz_transactions"
SILVER_TABLE = "spark_catalog.sbi_financial.slvr_transactions"

default_args = {
    "owner": "sbi-data-platform",
    "depends_on_past": False,
    "email_on_failure": bool(cfg.email_on_failure),
    "email": [cfg.email_on_failure] if cfg.email_on_failure else [],
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="sbi_iceberg_maintenance",
    description="Iceberg compaction and snapshot expiry for SBI financial tables",
    schedule=cfg.schedule_maintenance,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sbi", "iceberg", "maintenance", "spark"],
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    security_precheck = BashOperator(
        task_id="security_precheck",
        bash_command=(
            f"{cfg.bash_prefix()} && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/security/kinit_manager.sh"
        ),
    )

    maintain_bronze = BashOperator(
        task_id="maintain_bronze",
        bash_command=(
            f"{cfg.bash_prefix()} && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/submit/spark_submit.sh maintenance "
            f"--py-file ${{SPARK_OPTIMAL_HOME}}/jobs/maintenance/iceberg_maintenance_job.py "
            f"--table {BRONZE_TABLE} --force"
        ),
    )

    maintain_silver = BashOperator(
        task_id="maintain_silver",
        bash_command=(
            f"{cfg.bash_prefix()} && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/submit/spark_submit.sh maintenance "
            f"--py-file ${{SPARK_OPTIMAL_HOME}}/jobs/maintenance/iceberg_maintenance_job.py "
            f"--table {SILVER_TABLE} --force"
        ),
    )

    start >> security_precheck >> maintain_bronze >> maintain_silver >> end
