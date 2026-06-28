"""
SBI Airflow DAG 공통 설정 모듈

[설정 우선순위]
  1. Airflow Variable
  2. 환경 변수
  3. governance/configs/environments/{sbi_env}.yaml
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def _load_environment_yaml(project_home: str, sbi_env: str) -> dict[str, Any]:
    path = Path(project_home) / "governance/configs/environments" / f"{sbi_env}.yaml"
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as handle:
        return _expand_env(yaml.safe_load(handle) or {})


def _variable(name: str, default: str) -> str:
    try:
        from airflow.models import Variable  # type: ignore

        return Variable.get(name, default=default)
    except Exception:
        return os.environ.get(name.upper(), default)


@dataclass(frozen=True)
class SBIAirflowConfig:
    project_home: str
    sbi_env: str
    target_gb: float
    local_output: str
    hdfs_raw: str
    schedule_financial: str
    schedule_maintenance: str
    email_on_failure: Optional[str]

    @classmethod
    def from_airflow(cls) -> "SBIAirflowConfig":
        home = _variable("spark_optimal_home", os.environ.get("SPARK_OPTIMAL_HOME", "/opt/spark-optimal"))
        sbi_env = _variable("sbi_env", os.environ.get("SBI_ENV", "dev"))
        env_cfg = _load_environment_yaml(home, sbi_env)
        medallion = env_cfg.get("medallion", {})
        hdfs_uri = _variable("hdfs_uri", env_cfg.get("hdfs_uri", "hdfs://ns1"))
        default_hdfs_raw = medallion.get("hdfs_raw_path", f"{hdfs_uri}/{sbi_env}/raw/financial/transactions")
        return cls(
            project_home=home,
            sbi_env=sbi_env,
            target_gb=float(_variable("financial_target_gb", "10")),
            local_output=_variable("financial_local_output", f"{home}/data/output/financial"),
            hdfs_raw=_variable("hdfs_financial_raw", default_hdfs_raw),
            schedule_financial=_variable("schedule_financial_medallion", "0 2 * * *"),
            schedule_maintenance=_variable("schedule_iceberg_maintenance", "0 3 * * 0"),
            email_on_failure=_variable("sbi_alert_email", "") or None,
        )

    @property
    def env_script(self) -> str:
        conf = f"{self.project_home}/config/env.conf"
        template = f"{self.project_home}/config/env.template.conf"
        if os.path.isfile(conf):
            return f"source {conf}"
        return f"source {template}"

    def bash_prefix(self) -> str:
        return (
            f"set -euo pipefail && "
            f"export SPARK_OPTIMAL_HOME='{self.project_home}' && "
            f"export SBI_ENV='{self.sbi_env}' && "
            f"export SPARK_CONF_DIR='{self.project_home}/conf/{self.sbi_env}' && "
            f"{self.env_script}"
        )
