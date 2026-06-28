"""Standard data extractors."""

from __future__ import annotations

from pyspark.sql import DataFrame


class IcebergExtractor:
    def __init__(self, spark, table_name: str) -> None:
        self.spark = spark
        self.table_name = table_name

    def extract(self) -> DataFrame:
        return self.spark.table(self.table_name)


class HDFSJsonExtractor:
    def __init__(self, spark, path: str) -> None:
        self.spark = spark
        self.path = path

    def extract(self) -> DataFrame:
        return self.spark.read.option("multiLine", "false").json(self.path)
