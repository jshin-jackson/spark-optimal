"""Spark performance and coding standards."""

from __future__ import annotations

PERFORMANCE_DEFAULTS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
}

ICEBERG_WRITE_DEFAULTS = {
    "write.format.default": "parquet",
    "write.target-file-size-bytes": "268435456",
    "write.parquet.compression-codec": "snappy",
}

MIGRATION_READ_FORMATS = ("parquet", "orc", "json", "csv", "avro")
