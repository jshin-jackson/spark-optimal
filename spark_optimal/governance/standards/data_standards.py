"""Data quality and schema standards for SBI Spark workloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

FINANCIAL_TRANSACTION_SCHEMA: Dict[str, str] = {
    "transaction_id": "string",
    "account_id": "string",
    "customer_id": "string",
    "transaction_type": "string",
    "amount": "decimal(18,2)",
    "currency": "string",
    "merchant_category": "string",
    "channel": "string",
    "transaction_ts": "timestamp",
    "status": "string",
}

NULL_THRESHOLDS: Dict[str, float] = {
    "transaction_id": 0.0,
    "account_id": 0.0,
    "amount": 0.01,
    "transaction_ts": 0.01,
}


@dataclass
class TableStandard:
    name: str
    required_columns: List[str]
    null_thresholds: Dict[str, float] = field(default_factory=dict)
    partition_columns: List[str] = field(default_factory=list)


BRONZE_TRANSACTION_STANDARD = TableStandard(
    name="brnz_transactions",
    required_columns=list(FINANCIAL_TRANSACTION_SCHEMA.keys()),
    null_thresholds=NULL_THRESHOLDS,
    partition_columns=["transaction_date"],
)

SILVER_TRANSACTION_STANDARD = TableStandard(
    name="slvr_transactions",
    required_columns=list(FINANCIAL_TRANSACTION_SCHEMA.keys()) + ["transaction_date"],
    null_thresholds=NULL_THRESHOLDS,
    partition_columns=["transaction_date"],
)
