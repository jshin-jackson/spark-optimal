"""Iceberg load helpers."""

from __future__ import annotations

from spark_optimal.governance.standards.spark_standards import ICEBERG_WRITE_DEFAULTS


class IcebergLoader:
    def __init__(self, table_name: str, location: str | None = None, partition_cols: list[str] | None = None) -> None:
        self.table_name = table_name
        self.location = location
        self.partition_cols = partition_cols or []

    def load(self, df, mode: str = "overwrite") -> int:
        writer = (
            df.writeTo(self.table_name)
            .tableProperty("write.format.default", ICEBERG_WRITE_DEFAULTS["write.format.default"])
            .tableProperty(
                "write.target-file-size-bytes",
                ICEBERG_WRITE_DEFAULTS["write.target-file-size-bytes"],
            )
        )
        if self.location:
            writer = writer.tableProperty("location", self.location)
        if self.partition_cols:
            writer = writer.partitionedBy(*self.partition_cols)
        if mode == "append":
            writer.append()
        else:
            writer.createOrReplace()
        return df.sparkSession.table(self.table_name).count()
