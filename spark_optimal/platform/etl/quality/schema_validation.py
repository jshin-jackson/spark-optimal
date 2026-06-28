"""Schema validation for ETL inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class SchemaValidationResult:
    passed: bool
    missing_columns: List[str]
    extra_columns: List[str]


class SchemaValidator:
    def validate(self, df_columns: List[str], expected: Dict[str, str]) -> SchemaValidationResult:
        expected_cols = set(expected.keys())
        actual = set(df_columns)
        missing = sorted(expected_cols - actual)
        extra = sorted(actual - expected_cols)
        return SchemaValidationResult(passed=not missing, missing_columns=missing, extra_columns=extra)
