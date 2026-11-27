"""Daily DLQ breakdown by source topic and validation reason."""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from streambharat.spark_session import create_spark


def build_dlq_metrics(dlq):
    required = {
        "dlq_id", "source_topic", "kafka_partition", "kafka_offset",
        "reason_code", "quarantined_at",
    }
    missing = required - set(dlq.columns)
    if missing:
        raise ValueError(f"DLQ table missing columns: {sorted(missing)}")
    return (
        dlq.withColumn("activity_date", F.to_date("quarantined_at"))
        .groupBy("activity_date", "source_topic", "reason_code")
        .agg(
            F.countDistinct("dlq_id").cast("long").alias("invalid_records"),
            F.countDistinct("kafka_partition").cast("long").alias("affected_partitions"),
            F.min("quarantined_at").alias("first_seen_at"),
            F.max("quarantined_at").alias("last_seen_at"),
            F.min("kafka_offset").alias("first_offset"),
            F.max("kafka_offset").alias("last_offset"),
        )
    )


def run_dlq_metrics(
    source_path: str = "lake/bronze/dlq",
    output_path: str = "lake/gold/dlq_metrics_daily",
) -> None:
    spark = create_spark("StreamBharat-DLQ-Metrics")
    try:
        dlq = spark.read.format("delta").load(source_path)
        metrics = build_dlq_metrics(dlq)
        metrics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate Delta DLQ records by day/topic/reason")
    parser.add_argument("--source", default="lake/bronze/dlq")
    parser.add_argument("--output", default="lake/gold/dlq_metrics_daily")
    args = parser.parse_args()
    run_dlq_metrics(args.source, args.output)


if __name__ == "__main__":
    main()
