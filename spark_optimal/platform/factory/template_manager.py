"""Spark configuration template management."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from spark_optimal.config import PROJECT_ROOT, load_environment_config, load_resource_profiles, parse_spark_defaults, resolve_environment
from spark_optimal.governance.security.delegation_config import (
    build_iceberg_spark_config,
    build_kerberos_spark_config,
    build_ozone_spark_config,
)
from spark_optimal.governance.standards.spark_standards import PERFORMANCE_DEFAULTS


class TemplateManager:
    def load_environment_defaults(self, env: str | None = None) -> Dict[str, str]:
        env_name = resolve_environment(env)
        conf_path = PROJECT_ROOT / "conf" / env_name / "spark-defaults.conf"
        return parse_spark_defaults(conf_path)

    def build_workload_template(self, workload_type: str) -> Dict[str, str]:
        profiles = load_resource_profiles()
        profile = profiles.get(workload_type, profiles["etl_standard"])
        return dict(profile.get("spark_config", {}))

    def build_full_template(self, workload_type: str, env: str | None = None) -> Dict[str, str]:
        config: Dict[str, str] = {}
        config.update(self.load_environment_defaults(env))
        config.update(PERFORMANCE_DEFAULTS)
        config.update(build_kerberos_spark_config())
        config.update(build_iceberg_spark_config())
        config.update(build_ozone_spark_config())
        config.update(self.build_workload_template(workload_type))
        return config
