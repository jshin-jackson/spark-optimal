"""Medallion path and table helpers."""

from __future__ import annotations

import copy
from typing import Any

from spark_optimal.config import load_environment_config, load_yaml, resolve_environment


def _apply_medallion_paths(cfg: dict[str, Any], medallion: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(cfg)
    suffixes = medallion.get("table_suffixes", {})
    brnz_base = medallion["brnz_base"]
    slvr_base = medallion["slvr_base"]
    gld_base = medallion["gld_base"]

    result["hdfs_raw_path"] = medallion["hdfs_raw_path"]
    result["ozone"] = {
        "brnz_base": brnz_base,
        "slvr_base": slvr_base,
        "gld_base": gld_base,
    }
    result["tables"]["brnz"]["location"] = f"{brnz_base}/{suffixes.get('brnz', 'transactions')}"
    result["tables"]["slvr"]["location"] = f"{slvr_base}/{suffixes.get('slvr', 'transactions')}"
    result["tables"]["gld"]["location"] = f"{gld_base}/{suffixes.get('gld', 'daily_transaction_report')}"
    return result


def load_financial_medallion_config(env: str | None = None) -> dict[str, Any]:
    base = load_yaml("governance/configs/medallion/financial.yaml")
    env_cfg = load_environment_config(env)
    medallion = env_cfg.get("medallion")
    if not medallion:
        env_name = resolve_environment(env)
        raise ValueError(
            f"Missing 'medallion' section in governance/configs/environments/{env_name}.yaml"
        )
    return _apply_medallion_paths(base, medallion)
