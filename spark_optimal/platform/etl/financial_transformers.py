"""Financial medallion transformations: Bronze -> Silver -> Gold."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from spark_optimal.platform.etl.ozone_pipeline import BaseTransformer


class BronzeToSilverTransformer(BaseTransformer):
    """Cleanse raw JSON transactions for the silver layer."""

    def transform(self, df: DataFrame) -> DataFrame:
        parsed = (
            df.withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
            .withColumn("transaction_date", F.to_date("transaction_ts"))
            .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
            .withColumn("balance_after", F.col("balance_after").cast("decimal(18,2)"))
            .filter(F.col("status") == "SUCCESS")
            .filter(F.col("amount") > 0)
            .dropDuplicates(["transaction_id"])
        )
        return parsed.select(
            "transaction_id",
            "account_id",
            "customer_id",
            "transaction_type",
            "amount",
            "currency",
            "merchant_name",
            "merchant_category",
            "channel",
            "transaction_ts",
            "transaction_date",
            "branch_code",
            "status",
            "balance_after",
            "country_code",
        )


class SilverToGoldReportTransformer(BaseTransformer):
    """Build daily report aggregates for the gold layer."""

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy(
                F.col("transaction_date").alias("report_date"),
                "merchant_category",
                "channel",
                "currency",
            )
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.countDistinct("customer_id").alias("unique_customers"),
            )
            .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
            .withColumn("avg_amount", F.col("avg_amount").cast("decimal(18,2)"))
        )
