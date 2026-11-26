"""CDN request, egress and cost analytics using the supplied regional rate card."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streambharat.spark_session import create_spark


def build_cdn_egress_daily(
    requests: DataFrame,
    rates: DataFrame,
) -> DataFrame:
    """Aggregate successful and failed CDN requests with regional egress cost."""
    required_requests = {"cdn", "region", "request_ts", "status", "bytes", "content_id"}
    required_rates = {"cdn", "region", "rate_inr_per_gb"}
    missing_requests = required_requests - set(requests.columns)
    missing_rates = required_rates - set(rates.columns)
    if missing_requests:
        raise ValueError(f"CDN requests missing columns: {sorted(missing_requests)}")
    if missing_rates:
        raise ValueError(f"CDN rates missing columns: {sorted(missing_rates)}")

    rate_rows = (
        rates.select(
            F.lower(F.trim("cdn")).alias("cdn_key"),
            F.lower(F.trim("region")).alias("region_key"),
            F.col("rate_inr_per_gb").cast("double").alias("rate_inr_per_gb"),
        )
        .dropDuplicates(["cdn_key", "region_key"])
    )
    enriched = (
        requests.withColumn("activity_date", F.to_date("request_ts"))
        .withColumn("cdn_key", F.lower(F.trim("cdn")))
        .withColumn("region_key", F.lower(F.trim("region")))
        .withColumn("bytes", F.col("bytes").cast("long"))
        .join(rate_rows, ["cdn_key", "region_key"], "left")
        .withColumn("egress_gb", F.col("bytes") / F.lit(1024.0 ** 3))
        .withColumn(
            "cost_inr",
            F.col("egress_gb") * F.col("rate_inr_per_gb"),
        )
    )
    return (
        enriched.groupBy("activity_date", "cdn", "region")
        .agg(
            F.count(F.lit(1)).cast("long").alias("request_count"),
            F.sum("bytes").cast("long").alias("total_bytes"),
            F.sum("egress_gb").alias("egress_gb"),
            F.sum("cost_inr").cast("decimal(18,4)").alias("cost_inr"),
            F.sum(F.col("rate_inr_per_gb").isNull().cast("long")).alias("missing_rate_requests"),
            F.sum(F.col("status").between(400, 499).cast("long")).alias("http_4xx"),
            F.sum(F.col("status").between(500, 599).cast("long")).alias("http_5xx"),
            F.countDistinct("content_id").cast("long").alias("unique_content"),
        )
    )


def run_cdn_analytics(
    requests_path: str = "lake/silver/cdn_requests",
    rates_path: str = "lake/silver/cdn_rates",
    output_path: str = "lake/gold/cdn_egress_daily",
) -> None:
    spark = create_spark("StreamBharat-CDN-Egress-Analytics")
    try:
        requests = spark.read.format("delta").load(requests_path)
        rates = spark.read.format("delta").load(rates_path)
        output = build_cdn_egress_daily(requests, rates)
        output.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build CDN egress and regional cost metrics")
    parser.add_argument("--requests", default="lake/silver/cdn_requests")
    parser.add_argument("--rates", default="lake/silver/cdn_rates")
    parser.add_argument("--output", default="lake/gold/cdn_egress_daily")
    args = parser.parse_args()
    run_cdn_analytics(args.requests, args.rates, args.output)


if __name__ == "__main__":
    main()
