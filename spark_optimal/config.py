"""Shared configuration loading utilities."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_yaml(relative_path: str) -> dict[str, Any]:
    path = PROJECT_ROOT / relative_path
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return _expand_env(data)


def resolve_environment(env: str | None = None) -> str:
    return (env or os.environ.get("SBI_ENV") or os.environ.get("ENV_NAME") or "dev").lower()


def load_environment_config(env: str | None = None) -> dict[str, Any]:
    env_name = resolve_environment(env)
    return load_yaml(f"governance/configs/environments/{env_name}.yaml")


def _merge_resource_profiles(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for name in set(base) | set(override):
        base_profile = dict(base.get(name, {}))
        override_profile = override.get(name, {})
        if not override_profile:
            merged[name] = base_profile
            continue
        profile = {**base_profile, **{k: v for k, v in override_profile.items() if k != "spark_config"}}
        profile["spark_config"] = {
            **base_profile.get("spark_config", {}),
            **override_profile.get("spark_config", {}),
        }
        merged[name] = profile
    return merged


def load_cluster_config(env: str | None = None) -> dict[str, Any]:
    env_cfg = load_environment_config(env)
    cluster = env_cfg.get("cluster")
    if not cluster:
        env_name = resolve_environment(env)
        raise ValueError(
            f"Missing 'cluster' section in governance/configs/environments/{env_name}.yaml"
        )
    return cluster


def load_resource_limits(env: str | None = None) -> dict[str, Any]:
    env_cfg = load_environment_config(env)
    return env_cfg.get("resource_limits", {})


def load_resource_profiles(env: str | None = None) -> dict[str, Any]:
    base = load_yaml("governance/configs/workloads/resource_profiles.yaml")
    profiles = base.get("resource_profiles", {})
    env_name = resolve_environment(env)
    env_path = PROJECT_ROOT / f"governance/configs/workloads/resource_profiles_{env_name}.yaml"
    if env_path.exists():
        env_data = load_yaml(str(env_path.relative_to(PROJECT_ROOT)))
        profiles = _merge_resource_profiles(profiles, env_data.get("resource_profiles", {}))
    return profiles


def load_department_policies() -> dict[str, Any]:
    data = load_yaml("governance/configs/departments/policies.yaml")
    return data.get("departments", {})


def parse_spark_defaults(conf_path: Path) -> dict[str, str]:
    config: dict[str, str] = {}
    if not conf_path.exists():
        return config
    for line in conf_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        config[key.strip()] = os.path.expandvars(value.strip())
    return config
