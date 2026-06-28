"""Banking business rule validators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pyspark.sql import functions as F


@dataclass
class BusinessRuleResult:
    rule_name: str
    passed: bool
    detail: str


class BusinessRulesValidator:
    def validate_balance_integrity(self, df, amount_col: str = "amount", balance_col: str = "balance_after") -> BusinessRuleResult:
        invalid = df.filter((F.col(amount_col) < 0) | (F.col(balance_col) < 0)).count()
        return BusinessRuleResult(
            "balance_check",
            invalid == 0,
            f"{invalid} rows with negative amount/balance",
        )

    def validate_transaction_consistency(self, df, id_col: str = "transaction_id") -> BusinessRuleResult:
        total = df.count()
        distinct = df.select(id_col).distinct().count()
        passed = total == distinct
        return BusinessRuleResult("transaction_consistency", passed, f"total={total} distinct={distinct}")

    def run_all(self, df) -> List[BusinessRuleResult]:
        return [
            self.validate_balance_integrity(df),
            self.validate_transaction_consistency(df),
        ]
