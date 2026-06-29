"""Ranger policies for Iceberg on Ozone (Cloudera CDP 7.3.1 aligned)."""

from __future__ import annotations

from typing import Any

from spark_optimal.config import load_yaml, resolve_environment
from spark_optimal.platform.medallion.financial_config import load_financial_medallion_config


def load_ranger_iceberg_ozone_pairs() -> dict[str, Any]:
    return load_yaml("governance/configs/security/ranger_iceberg_ozone_pairs.yaml")


def _format_env(value: Any, env: str) -> Any:
    if isinstance(value, str):
        return value.replace("{env}", env)
    if isinstance(value, dict):
        return {k: _format_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [_format_env(v, env) for v in value]
    return value


def resolve_table_policies(env: str | None = None) -> list[dict[str, Any]]:
    """Return full per-table Ranger policy set (Hadoop SQL + cm_ozone)."""
    env_name = resolve_environment(env)
    pairs_cfg = load_ranger_iceberg_ozone_pairs()
    hadoop_sql = pairs_cfg["ranger_services"]["hadoop_sql"]
    ozone_svc = pairs_cfg["ranger_services"]["ozone"]
    resolved: list[dict[str, Any]] = []

    for entry in pairs_cfg.get("tables", []):
        table_name = entry["table_name"]
        hs = _format_env(entry["hadoop_sql"], env_name)
        oz = _format_env(entry["ozone"], env_name)
        resolved.append(
            {
                "layer": entry.get("layer"),
                "table_name": table_name,
                "catalog_table": entry.get("catalog_table"),
                "engines": entry.get("engines", []),
                "policies": [
                    {
                        "ranger_service": hadoop_sql,
                        "policy_type": "sql_table",
                        "policy_name": hs["sql_table"]["policy_name"],
                        "database": hs["sql_table"]["database"],
                        "table": hs["sql_table"]["table"],
                        "columns": hs["sql_table"].get("columns", "*"),
                        "permissions": hs["sql_table"]["permissions"],
                    },
                    {
                        "ranger_service": hadoop_sql,
                        "policy_type": "url",
                        "policy_name": hs["url"]["policy_name"],
                        "url": hs["url"]["url_pattern"],
                        "permissions": hs["url"]["permissions"],
                    },
                    {
                        "ranger_service": ozone_svc,
                        "policy_type": "volume_bucket_key",
                        "policy_name": oz["policy_name"],
                        "volume": oz["volume"],
                        "bucket": oz["bucket"],
                        "key": oz["key"],
                        "path": hs["url"]["url_pattern"],
                        "permissions": oz["permissions"],
                    },
                ],
                "optional_policies": [
                    {
                        "ranger_service": hadoop_sql,
                        "policy_type": "storage_handler",
                        "policy_name": hs["storage_handler"]["policy_name"],
                        "storage_type": hs["storage_handler"]["storage_type"],
                        "storage_url": hs["storage_handler"]["storage_url"],
                        "permissions": hs["storage_handler"]["permissions"],
                        "note": hs["storage_handler"].get(
                            "note", "Optional if cluster storage_handler covers systest"
                        ),
                    }
                ],
            }
        )
    return resolved


def resolve_paired_policies(env: str | None = None) -> list[dict[str, Any]]:
    """Backward-compatible view: primary SQL + Ozone pair per table."""
    result: list[dict[str, Any]] = []
    for entry in resolve_table_policies(env):
        sql = next(p for p in entry["policies"] if p["policy_type"] == "sql_table")
        ozone = next(p for p in entry["policies"] if p["policy_type"] == "volume_bucket_key")
        url = next(p for p in entry["policies"] if p["policy_type"] == "url")
        result.append(
            {
                "layer": entry["layer"],
                "table_name": entry["table_name"],
                "catalog_table": entry["catalog_table"],
                "engines": entry["engines"],
                "hive_policy": {
                    "ranger_service": sql["ranger_service"],
                    "policy_name": sql["policy_name"],
                    "database": sql["database"],
                    "table": sql["table"],
                    "actions": sql["permissions"],
                },
                "url_policy": {
                    "ranger_service": url["ranger_service"],
                    "policy_name": url["policy_name"],
                    "url": url["url"],
                    "actions": url["permissions"],
                },
                "ozone_policy": {
                    "ranger_service": ozone["ranger_service"],
                    "policy_name": ozone["policy_name"],
                    "path": ozone["path"],
                    "volume": ozone["volume"],
                    "bucket": ozone["bucket"],
                    "key": ozone["key"],
                    "actions": ozone["permissions"],
                },
            }
        )
    return result


def verify_pairs_match_medallion(env: str | None = None) -> list[str]:
    """Ensure Ozone URL paths match financial medallion table locations."""
    env_name = resolve_environment(env)
    medallion = load_financial_medallion_config(env_name)
    errors: list[str] = []

    layer_by_table = {
        "brnz_transactions": "brnz",
        "slvr_transactions": "slvr",
        "gld_daily_report": "gld",
    }
    for pair in resolve_paired_policies(env_name):
        table_name = pair["table_name"]
        layer = layer_by_table.get(table_name)
        if not layer:
            continue
        expected = medallion["tables"][layer]["location"]
        actual = pair["ozone_policy"]["path"]
        if expected != actual:
            errors.append(
                f"{table_name}: ozone path mismatch — config={actual!r} medallion={expected!r}"
            )
        expected_db = f"{env_name}_{table_name}_db_plcy"
        expected_uri = f"{env_name}_{table_name}_uri_plcy"
        expected_ozone = f"{env_name}_data_{layer}_key_plcy"
        if pair["hive_policy"]["policy_name"] != expected_db:
            errors.append(f"{table_name}: SQL policy_name must be {expected_db}")
        if pair["url_policy"]["policy_name"] != expected_uri:
            errors.append(f"{table_name}: URL policy_name must be {expected_uri}")
        if pair["ozone_policy"]["policy_name"] != expected_ozone:
            errors.append(f"{table_name}: cm_ozone policy_name must be {expected_ozone}")
    return errors
