"""Manual trigger DAG for security validation on gateway node."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.sbi_config import SBIAirflowConfig

cfg = SBIAirflowConfig.from_airflow()

with DAG(
    dag_id="sbi_security_precheck",
    description="Kerberos kinit and delegation token readiness check",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sbi", "security"],
    default_args={
        "owner": "sbi-data-platform",
        "retries": 0,
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:
    BashOperator(
        task_id="kinit_and_check",
        bash_command=(
            f"{cfg.bash_prefix()} && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/security/kinit_manager.sh && "
            f"bash ${{SPARK_OPTIMAL_HOME}}/scripts/security/security_check.sh"
        ),
    )
