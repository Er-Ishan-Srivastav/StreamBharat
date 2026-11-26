"""Monthly Gold-versus-published metric reconciliation with explicit metric mapping."""
from __future__ import annotations

import argparse
import os
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, StringType, StructField, StructType,
)

from streambharat.spark_session import create_spark


ALIASES_SCHEMA = StructType([
    StructField("published_metric", StringType(), False),
    StructField("actual_metric", StringType(), False),
])


def normalize_metric_name(value: str) -> str:
    """Normalize labels for exact alias lookup without guessing semantics."""
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def compare_published_metrics(
    published: DataFrame,
    actual: DataFrame,
    aliases: DataFrame,
    tolerance_pct: float = 0.5,
) -> DataFrame:
    """Compare source-published monthly values with canonical monthly Gold metrics."""
    if not 0 <= tolerance_pct:
        raise ValueError("tolerance_pct must be non-negative")
    expected_published = {"month", "metric", "value"}
    expected_actual = {"month", "actual_metric", "actual_value"}
    expected_aliases = {"published_metric", "actual_metric"}
    for name, frame, expected in (
        ("published", published, expected_published),
        ("actual", actual, expected_actual),
        ("aliases", aliases, expected_aliases),
    ):
        missing = expected - set(frame.columns)
        if missing:
            raise ValueError(f"{name} metrics missing columns: {sorted(missing)}")

    month_text = F.trim(F.col("month").cast("string"))
    published_rows = (
        published
        .withColumn(
            "month",
            F.when(
                F.length(month_text) == 7,
                F.to_date(F.concat(month_text, F.lit("-01"))),
            ).otherwise(F.to_date(month_text)),
        )
        .withColumn("published_value", F.col("value").cast("double"))
        .withColumn("metric_key", F.regexp_replace(
            F.lower(F.trim(F.col("metric"))), "[^a-z0-9]", ""
        ))
        .withColumn(
            "published_ts",
            F.to_timestamp(F.col("published_ts"))
            if "published_ts" in published.columns
            else F.lit(None).cast("timestamp"),
        )
        .withColumn(
            "_revision_rank",
            F.row_number().over(
                Window.partitionBy("month", "metric").orderBy(
                    F.col("published_ts").desc_nulls_last()
                )
            ),
        )
        .filter(F.col("_revision_rank") == 1)
        .select("month", "metric", "metric_key", "published_value", "published_ts")
    )
    alias_rows = (
        aliases
        .withColumn(
            "metric_key",
            F.regexp_replace(F.lower(F.trim("published_metric")), "[^a-z0-9]", ""),
        )
        .select("metric_key", "actual_metric")
        .dropDuplicates(["metric_key"])
    )
    actual_rows = (
        actual
        .withColumn("month", F.to_date("month"))
        .withColumn("actual_value", F.col("actual_value").cast("double"))
        .select("month", "actual_metric", "actual_value")
    )

    compared = (
        published_rows.join(alias_rows, "metric_key", "left")
        .join(actual_rows, ["month", "actual_metric"], "left")
        .withColumn(
            "variance_absolute",
            F.abs(F.col("actual_value") - F.col("published_value")),
        )
        .withColumn(
            "variance_pct",
            F.when(
                F.abs(F.col("published_value")) > 0,
                F.col("variance_absolute") / F.abs(F.col("published_value")) * 100.0,
            ).when(
                F.col("actual_value") == F.col("published_value"),
                F.lit(0.0),
            ),
        )
        .withColumn("tolerance_pct", F.lit(float(tolerance_pct)))
        .withColumn(
            "reconciliation_status",
            F.when(F.col("actual_metric").isNull(), F.lit("UNMAPPED_METRIC"))
             .when(F.col("actual_value").isNull(), F.lit("MISSING_ACTUAL"))
             .when(F.col("variance_pct").isNull(), F.lit("ZERO_BASELINE_REVIEW"))
             .when(F.col("variance_pct") <= F.col("tolerance_pct"), F.lit("WITHIN_TOLERANCE"))
             .otherwise(F.lit("VARIANCE_OVER_TOLERANCE")),
        )
        .withColumn(
            "investigation_hint",
            F.when(
                F.col("reconciliation_status") == "VARIANCE_OVER_TOLERANCE",
                F.lit(
                    "Review metric definition, published cutoff, late-arrival backfill, "
                    "episode-to-series roll-up and aggregation level before changing source data"
                ),
            ).when(
                F.col("reconciliation_status") == "UNMAPPED_METRIC",
                F.lit("Add the source metric name and its canonical Gold metric to the alias file"),
            ).when(
                F.col("reconciliation_status") == "MISSING_ACTUAL",
                F.lit("Verify that the required Gold source table contains the published month"),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn("compared_at", F.current_timestamp())
        .select(
            "month", "metric", "actual_metric", "published_value", "actual_value",
            "variance_absolute", "variance_pct", "tolerance_pct",
            "reconciliation_status", "investigation_hint", "published_ts", "compared_at",
        )
    )
    return compared


def _month_metric(frame: DataFrame, date_col: str, name: str, expression) -> DataFrame:
    return (
        frame.withColumn("month", F.date_trunc("month", F.col(date_col)).cast("date"))
        .groupBy("month")
        .agg(expression.alias("actual_value"))
        .withColumn("actual_metric", F.lit(name))
        .select("month", "actual_metric", "actual_value")
    )


def build_actual_monthly_metrics(spark, gold_root: str = "lake/gold") -> DataFrame:
    """Build the documented canonical metric set from persisted Gold outputs."""
    from delta.tables import DeltaTable

    frames = []

    def read(name: str) -> DataFrame:
        path = os.path.join(gold_root, name)
        if not DeltaTable.isDeltaTable(spark, path):
            raise FileNotFoundError(f"Required Gold table is missing: {path}")
        return spark.read.format("delta").load(path)

    watch = read("watch_time_daily")
    frames.append(_month_metric(
        watch, "viewing_date", "watch_hours",
        F.sum("watched_seconds") / F.lit(3600.0),
    ))

    active = read("active_users_daily")
    for source_type, metric_name in (("DAU", "dau_average"), ("MAU", "mau_average")):
        frames.append(_month_metric(
            active.filter(F.col("metric_type") == source_type),
            "activity_date", metric_name, F.avg("active_users"),
        ))

    sessions = read("viewing_sessions")
    frames.append(_month_metric(sessions, "viewing_date", "sessions", F.count(F.lit(1))))
    frames.append(_month_metric(
        sessions.filter(F.col("completed")),
        "viewing_date", "completed_sessions", F.count(F.lit(1)),
    ))
    frames.append(_month_metric(
        sessions, "viewing_date", "average_completion_rate",
        F.avg("completion_rate"),
    ))

    campaigns = read("campaign_daily")
    frames.append(_month_metric(
        campaigns, "activity_date", "ad_impressions", F.sum("impressions")
    ))
    frames.append(_month_metric(
        campaigns, "activity_date", "ad_spend_inr", F.sum("spend_inr")
    ))

    revenue = read("revenue_reconciliation").filter(
        (F.col("settlement_status") == "SETTLED") & F.col("charge_ts").isNotNull()
    )
    frames.append(_month_metric(
        revenue, "charge_ts", "revenue_collected_inr", F.sum("charge_amount")
    ))

    cdn = read("cdn_egress_daily")
    frames.append(_month_metric(
        cdn, "activity_date", "cdn_egress_gb", F.sum("egress_gb")
    ))
    frames.append(_month_metric(
        cdn, "activity_date", "cdn_cost_inr", F.sum("cost_inr")
    ))

    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def run_published_reconciliation(
    published_path: str = "data/raw/reference/published_metrics.csv",
    alias_path: str = "config/quality/published_metric_aliases.csv",
    gold_root: str = "lake/gold",
    output_path: str = "lake/gold/published_metrics_reconciliation",
    tolerance_pct: float = 0.5,
) -> None:
    spark = create_spark("StreamBharat-Published-Metrics-Reconciliation")
    try:
        published = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(published_path)
        )
        aliases = spark.read.schema(ALIASES_SCHEMA).option("header", True).csv(alias_path)
        actual = build_actual_monthly_metrics(spark, gold_root)
        compared = compare_published_metrics(published, actual, aliases, tolerance_pct)
        compared.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)
        compared.groupBy("reconciliation_status").count().orderBy(
            "reconciliation_status"
        ).show(truncate=False)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare monthly Gold metrics with the published course reference"
    )
    parser.add_argument("--published", default="data/raw/reference/published_metrics.csv")
    parser.add_argument("--aliases", default="config/quality/published_metric_aliases.csv")
    parser.add_argument("--gold-root", default="lake/gold")
    parser.add_argument("--output", default="lake/gold/published_metrics_reconciliation")
    parser.add_argument("--tolerance-pct", type=float, default=0.5)
    args = parser.parse_args()
    run_published_reconciliation(
        args.published, args.aliases, args.gold_root, args.output, args.tolerance_pct
    )


if __name__ == "__main__":
    main()
