"""Q18 — Live impression frequency caps and campaign spend pacing.

A Delta event ledger keyed by impression_id is the source of truth. Aggregates are
recomputed from that ledger, making foreachBatch retries idempotent instead of
incrementing counters twice after a failed micro-batch retry.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from .spark_session import create_spark

AD_SCHEMA = StructType([
    StructField("impression_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("ad_break_ts", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("cpm_inr", DoubleType(), True),
])


def _persist_impressions(spark, ledger_path: str, incoming):
    parsed = (
        incoming.withColumn("event_ts", F.to_timestamp("ad_break_ts"))
        .filter(F.col("event_ts").isNotNull() & F.col("impression_id").isNotNull())
        .dropDuplicates(["impression_id"])
        .withColumn("ad_date", F.to_date("event_ts"))
    )
    if parsed.isEmpty():
        return
    if not DeltaTable.isDeltaTable(spark, ledger_path):
        parsed.write.format("delta").mode("overwrite").save(ledger_path)
    else:
        (
            DeltaTable.forPath(spark, ledger_path).alias("t")
            .merge(parsed.alias("s"), "t.impression_id = s.impression_id")
            .whenNotMatchedInsertAll()
            .execute()
        )


def _process_ads_batch(df, batch_id, spark, campaigns, ledger_path, cap_state_path,
                       spend_state_path, capped_path, alerts_path, match_start, match_hours):
    if df.isEmpty():
        return
    _persist_impressions(spark, ledger_path, df)
    if not DeltaTable.isDeltaTable(spark, ledger_path):
        return

    ledger = spark.read.format("delta").load(ledger_path)
    # Rebuild aggregates from unique impression IDs; no additive MERGE side effects.
    counts = ledger.groupBy("user_id", "campaign_id", "ad_date").agg(
        F.countDistinct("impression_id").alias("impression_count")
    )
    spends = ledger.groupBy("campaign_id", "ad_date").agg(
        (F.sum("cpm_inr") / F.lit(1000.0)).alias("spend_inr")
    )
    counts.write.format("delta").mode("overwrite").save(cap_state_path)
    spends.write.format("delta").mode("overwrite").save(spend_state_path)

    campaign_caps = campaigns.select(
        "campaign_id", F.col("daily_frequency_cap").cast("int").alias("daily_frequency_cap")
    )
    capped = (
        counts.join(campaign_caps, "campaign_id")
        .filter(F.col("impression_count") >= F.col("daily_frequency_cap"))
        .withColumn("remaining_impressions", F.lit(0))
        .withColumn("cap_reached", F.lit(True))
    )
    capped.write.format("delta").mode("overwrite").save(capped_path)

    duration = F.datediff("end_date", "start_date") + 1
    cfg = campaigns.withColumn("campaign_days", duration).withColumn(
        "daily_budget_inr", F.col("budget_inr") / F.greatest("campaign_days", F.lit(1))
    )
    pacing = (
        spends.join(cfg.select("campaign_id", "daily_budget_inr", "budget_inr"), "campaign_id")
        .withColumn("match_start", F.to_timestamp(F.lit(match_start)))
        .withColumn(
            "elapsed_hours",
            F.least(
                F.greatest(
                    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("match_start")) / 3600.0,
                    F.lit(0.0),
                ),
                F.lit(float(match_hours)),
            ),
        )
        .withColumn(
            "ideal_spend_inr",
            F.col("daily_budget_inr") * F.col("elapsed_hours") / F.lit(float(match_hours)),
        )
        .withColumn(
            "pacing_ratio",
            F.when(F.col("ideal_spend_inr") > 0, F.col("spend_inr") / F.col("ideal_spend_inr")),
        )
        .withColumn(
            "alert",
            F.when(F.col("pacing_ratio") > 1.2, F.lit("OVERSPENDING"))
            .when(
                (F.col("elapsed_hours") >= F.lit(0.5)) & (F.col("pacing_ratio") < 0.7),
                F.lit("UNDERDELIVERING"),
            ),
        )
        .filter(F.col("alert").isNotNull())
        .withColumn("batch_processed_at", F.current_timestamp())
    )
    pacing.write.format("delta").mode("overwrite").save(alerts_path)


def run_q18(input_path="data/stream/ads", campaigns_path="data/raw/ads/ad_campaigns.csv",
            checkpoint_path="data/checkpoints/q18_ads",
            ledger_path="data/delta/silver/live_ad_impression_ledger",
            cap_state_path="data/delta/silver/live_ad_caps_state",
            spend_state_path="data/delta/silver/live_ad_spend_state",
            capped_path="data/delta/gold/capped_users_live",
            alerts_path="data/delta/gold/pacing_alerts_live", match_start=None, match_hours=3):
    if not os.path.isdir(input_path):
        raise FileNotFoundError("Start ott_stream_simulator.py --stream ads first")
    if match_hours <= 0:
        raise ValueError("match_hours must be positive")
    match_start = match_start or datetime.now().replace(
        hour=18, minute=0, second=0, microsecond=0
    ).isoformat(sep=" ")
    spark = create_spark("StreamBharat-Q18-Live-Ad-Pacing")
    try:
        campaigns = spark.read.option("header", True).option("inferSchema", True).csv(campaigns_path)
        stream = spark.readStream.schema(AD_SCHEMA).option("maxFilesPerTrigger", 10).json(input_path)
        for path in (checkpoint_path, ledger_path, cap_state_path, spend_state_path, capped_path, alerts_path):
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        query = (
            stream.writeStream.foreachBatch(
                lambda df, bid: _process_ads_batch(
                    df, bid, spark, campaigns, ledger_path, cap_state_path,
                    spend_state_path, capped_path, alerts_path, match_start, match_hours,
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .queryName("streambharat_q18_ad_caps_and_pacing")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/stream/ads")
    parser.add_argument("--match-start", default=None)
    parser.add_argument("--match-hours", type=float, default=3)
    args = parser.parse_args()
    run_q18(input_path=args.input, match_start=args.match_start, match_hours=args.match_hours)
