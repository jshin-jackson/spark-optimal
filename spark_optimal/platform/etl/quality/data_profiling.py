"""Basic data profiling."""

from __future__ import annotations

from typing import Any, Dict, List


class DataProfiler:
    def profile(self, df) -> Dict[str, Any]:
        row_count = df.count()
        columns = df.columns
        null_counts: Dict[str, int] = {}
        for col in columns:
            null_counts[col] = df.filter(df[col].isNull()).count()
        return {
            "row_count": row_count,
            "column_count": len(columns),
            "null_counts": null_counts,
        }
