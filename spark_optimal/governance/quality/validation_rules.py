"""Reusable data validation rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class RuleResult:
    rule_name: str
    passed: bool
    message: str = ""


@dataclass
class ValidationRule:
    name: str
    rule_type: str
    config: Dict[str, Any] = field(default_factory=dict)


class ValidationRuleRegistry:
    def apply_rule(self, df, rule: ValidationRule) -> RuleResult:
        if rule.rule_type == "not_null":
            col = rule.config["column"]
            null_count = df.filter(df[col].isNull()).count()
            threshold = rule.config.get("max_null_ratio", 0.0)
            total = max(df.count(), 1)
            ratio = null_count / total
            passed = ratio <= threshold
            return RuleResult(rule.name, passed, f"{col} null ratio {ratio:.4f}")

        if rule.rule_type == "positive_amount":
            col = rule.config.get("column", "amount")
            bad = df.filter(df[col] <= 0).count()
            passed = bad == 0
            return RuleResult(rule.name, passed, f"{bad} non-positive {col} rows")

        if rule.rule_type == "allowed_values":
            col = rule.config["column"]
            allowed = set(rule.config["values"])
            distinct = {r[0] for r in df.select(col).distinct().collect()}
            invalid = distinct - allowed
            passed = not invalid
            return RuleResult(rule.name, passed, f"invalid {col} values: {invalid}")

        return RuleResult(rule.name, True, "unknown rule type — skipped")

    def apply_all(self, df, rules: List[ValidationRule]) -> List[RuleResult]:
        return [self.apply_rule(df, rule) for rule in rules]
