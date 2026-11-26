"""Subscription charge reconciliation and partner royalty allocation."""
from __future__ import annotations

import argparse
import glob
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


CHARGE_EVENT_TYPES = ("subscribe", "renew", "upgrade", "downgrade")


def _map_payload(bronze: DataFrame) -> DataFrame:
    return bronze.withColumn(
        "_payload", F.from_json(F.col("raw_value"), MapType(StringType(), StringType()))
    )


def build_revenue_reconciliation(
    subscription_bronze: DataFrame,
    settlement_bronze: DataFrame,
    match_window_hours: int = 48,
) -> DataFrame:
    """Match each subscription charge to at most one gateway settlement.

    Matching is strict on user, amount and event type, and allows a settlement
    to arrive up to match_window_hours after the charge. Candidate pairs are
    ranked by absolute event-time distance; a settlement is never assigned to
    two charges. Unused candidate settlements are exposed as duplicates.
    """
    if match_window_hours < 1:
        raise ValueError("match_window_hours must be positive")

    subscription = _map_payload(subscription_bronze)
    charges = (
        subscription.select(
            F.element_at("_payload", F.lit("event_id")).alias("source_event_id"),
            F.element_at("_payload", F.lit("user_id")).alias("user_id"),
            F.lower(F.trim(F.element_at("_payload", F.lit("event_type")))).alias("event_type"),
            F.element_at("_payload", F.lit("price")).cast("decimal(18,2)").alias("charge_amount"),
            F.to_timestamp(F.element_at("_payload", F.lit("event_ts"))).alias("charge_ts"),
        )
        .filter(
            F.col("event_type").isin(*CHARGE_EVENT_TYPES)
            & F.col("user_id").isNotNull()
            & F.col("charge_amount").isNotNull()
            & (F.col("charge_amount") >= 0)
            & F.col("charge_ts").isNotNull()
        )
        .withColumn(
            "charge_id",
            F.coalesce(
                F.col("source_event_id"),
                F.sha2(
                    F.concat_ws(
                        "|", "user_id", "event_type", F.col("charge_amount").cast("string"),
                        F.col("charge_ts").cast("string"),
                    ),
                    256,
                ),
            ),
        )
        .drop("source_event_id")
        .dropDuplicates(["charge_id"])
    )

    settlement_payload = _map_payload(settlement_bronze)
    settlements = (
        settlement_payload.select(
            F.element_at("_payload", F.lit("gateway")).alias("gateway"),
            F.element_at("_payload", F.lit("user_id")).alias("user_id"),
            F.element_at("_payload", F.lit("amount_inr")).cast("decimal(18,2)").alias("settlement_amount"),
            F.to_timestamp(F.element_at("_payload", F.lit("settled_ts"))).alias("settlement_ts"),
            F.lower(F.trim(F.element_at("_payload", F.lit("event_type")))).alias("event_type"),
            F.element_at("_payload", F.lit("settlement_id")).alias("source_settlement_id"),
            F.col("source_topic") if "source_topic" in settlement_payload.columns else F.lit(None).cast("string").alias("source_topic"),
            F.col("kafka_partition") if "kafka_partition" in settlement_payload.columns else F.lit(None).cast("int").alias("kafka_partition"),
            F.col("kafka_offset") if "kafka_offset" in settlement_payload.columns else F.lit(None).cast("long").alias("kafka_offset"),
        )
        .filter(
            F.col("user_id").isNotNull()
            & F.col("settlement_amount").isNotNull()
            & (F.col("settlement_amount") >= 0)
            & F.col("settlement_ts").isNotNull()
            & F.col("event_type").isNotNull()
        )
        .withColumn(
            "settlement_id",
            F.coalesce(
                F.col("source_settlement_id"),
                F.sha2(
                    F.concat_ws(
                        "|", F.coalesce("gateway", F.lit("")),
                        "user_id", F.col("settlement_amount").cast("string"),
                        F.col("settlement_ts").cast("string"), "event_type",
                        F.coalesce(F.col("source_topic"), F.lit("")),
                        F.coalesce(F.col("kafka_partition").cast("string"), F.lit("")),
                        F.coalesce(F.col("kafka_offset").cast("string"), F.lit("")),
                    ),
                    256,
                ),
            ),
        )
        .dropDuplicates(["settlement_id"])
    )

    c = charges.alias("c")
    s = settlements.alias("s")
    candidates = (
        c.join(
            s,
            (F.col("c.user_id") == F.col("s.user_id"))
            & (F.col("c.charge_amount") == F.col("s.settlement_amount"))
            & (F.col("c.event_type") == F.col("s.event_type"))
            & (F.col("s.settlement_ts") >= F.col("c.charge_ts"))
            & (
                F.col("s.settlement_ts")
                <= F.col("c.charge_ts") + F.expr(f"INTERVAL {match_window_hours} HOURS")
            ),
            "inner",
        )
        .select(
            F.col("c.charge_id").alias("charge_id"),
            F.col("c.user_id").alias("user_id"),
            F.col("c.charge_amount").alias("charge_amount"),
            F.col("c.charge_ts").alias("charge_ts"),
            F.col("s.settlement_id").alias("settlement_id"),
            F.col("s.settlement_ts").alias("settlement_ts"),
        )
        .withColumn(
            "charge_rank",
            F.row_number().over(
                Window.partitionBy("charge_id").orderBy(
                    F.abs(F.unix_timestamp("settlement_ts") - F.unix_timestamp("charge_ts")),
                    F.col("settlement_ts"),
                    F.col("settlement_id"),
                )
            ),
        )
        .withColumn(
            "settlement_rank",
            F.row_number().over(
                Window.partitionBy("settlement_id").orderBy(
                    F.abs(F.unix_timestamp("settlement_ts") - F.unix_timestamp("charge_ts")),
                    F.col("charge_ts"),
                    F.col("charge_id"),
                )
            ),
        )
    )
    matched = candidates.filter(
        (F.col("charge_rank") == 1) & (F.col("settlement_rank") == 1)
    ).select(
        "charge_id", "user_id", "charge_amount", "charge_ts",
        "settlement_id", "settlement_ts",
    )

    charge_results = (
        charges.join(
            matched.select(
                "charge_id", "settlement_id", "settlement_ts"
            ),
            "charge_id",
            "left",
        )
        .withColumn(
            "settlement_status",
            F.when(F.col("settlement_id").isNotNull(), F.lit("SETTLED"))
             .otherwise(F.lit("UNSETTLED_CHARGE")),
        )
        .select(
            "charge_id", "user_id", "charge_amount", "settlement_id",
            "settlement_status", "charge_ts", "settlement_ts",
        )
    )

    matched_settlement_ids = matched.select("settlement_id").dropDuplicates()
    candidate_settlement_ids = candidates.select("settlement_id").dropDuplicates()
    unused_settlements = (
        settlements.join(matched_settlement_ids, "settlement_id", "left_anti")
        .join(candidate_settlement_ids.withColumn("has_candidate", F.lit(True)), "settlement_id", "left")
        .withColumn(
            "settlement_status",
            F.when(F.col("has_candidate"), F.lit("DUPLICATE_SETTLEMENT"))
             .otherwise(F.lit("ORPHAN_SETTLEMENT")),
        )
        .select(
            F.concat(F.lit("SETTLEMENT:"), F.col("settlement_id")).alias("charge_id"),
            "user_id",
            F.lit(None).cast("decimal(18,2)").alias("charge_amount"),
            "settlement_id",
            "settlement_status",
            F.lit(None).cast("timestamp").alias("charge_ts"),
            "settlement_ts",
        )
    )
    return charge_results.unionByName(unused_settlements)


def build_title_history(spark, catalog_dir: str) -> DataFrame:
    """Build point-in-time title/partner intervals from monthly snapshots."""
    snapshots = sorted(glob.glob(os.path.join(catalog_dir, "catalog_*.json")))
    if not snapshots:
        raise FileNotFoundError(f"No monthly catalog snapshots found in {catalog_dir}")
    frames = [read_catalog_snapshot(spark, path) for path in snapshots]
    all_versions = frames[0]
    for frame in frames[1:]:
        all_versions = all_versions.unionByName(frame)
    ordering = Window.partitionBy("content_id").orderBy("snapshot_date")
    return (
        all_versions.select(
            "content_id", "partner_id", "tier", "snapshot_date"
        )
        .dropDuplicates(["content_id", "snapshot_date"])
        .withColumn("valid_from", F.col("snapshot_date"))
        .withColumn("valid_to", F.lead("snapshot_date").over(ordering))
        .select("content_id", "partner_id", "tier", "valid_from", "valid_to")
    )


def build_partner_royalties(
    watch_time: DataFrame,
    title_history: DataFrame,
    revenue_reconciliation: DataFrame,
    partners: DataFrame,
) -> DataFrame:
    """Allocate collected subscription revenue by point-in-time partner watch share.

    Partner revenue share is expressed as a percentage (for example 35 means
    35%). The quarterly minimum guarantee is applied after revenue allocation.
    """
    required_watch = {"viewing_date", "content_id", "watched_seconds"}
    required_titles = {"content_id", "partner_id", "valid_from", "valid_to"}
    required_revenue = {"charge_ts", "charge_amount", "settlement_status"}
    required_partners = {
        "partner_id", "revenue_share_pct", "minimum_guarantee_inr_per_quarter"
    }
    for name, frame, required in (
        ("watch_time", watch_time, required_watch),
        ("title_history", title_history, required_titles),
        ("revenue_reconciliation", revenue_reconciliation, required_revenue),
        ("partners", partners, required_partners),
    ):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{name} missing columns: {sorted(missing)}")

    w = watch_time.alias("w")
    t = title_history.alias("t")
    attributed = (
        w.join(
            t,
            (F.col("w.content_id") == F.col("t.content_id"))
            & (F.col("w.viewing_date") >= F.to_date(F.col("t.valid_from")))
            & (
                F.col("t.valid_to").isNull()
                | (F.col("w.viewing_date") < F.to_date(F.col("t.valid_to")))
            ),
            "inner",
        )
        .filter(F.col("t.partner_id").isNotNull())
        .withColumn(
            "quarter_start",
            F.date_trunc("quarter", F.col("w.viewing_date")).cast("date"),
        )
    )
    partner_watch = attributed.groupBy("quarter_start", F.col("t.partner_id").alias("partner_id")).agg(
        F.sum(F.col("w.watched_seconds")).cast("long").alias("partner_watched_seconds")
    )
    total_watch = (
        watch_time.withColumn(
            "quarter_start", F.date_trunc("quarter", "viewing_date").cast("date")
        )
        .groupBy("quarter_start")
        .agg(F.sum("watched_seconds").cast("long").alias("total_watched_seconds"))
    )
    revenue_pool = (
        revenue_reconciliation.filter(
            (F.col("settlement_status") == "SETTLED")
            & F.col("charge_ts").isNotNull()
        )
        .withColumn(
            "quarter_start", F.date_trunc("quarter", "charge_ts").cast("date")
        )
        .groupBy("quarter_start")
        .agg(F.sum("charge_amount").cast("decimal(18,2)").alias("collected_revenue_inr"))
    )
    quarter_domain = (
        total_watch.select("quarter_start")
        .unionByName(revenue_pool.select("quarter_start"))
        .dropDuplicates()
    )
    quarter_partners = quarter_domain.crossJoin(partners)
    allocated = (
        quarter_partners
        .join(partner_watch, ["quarter_start", "partner_id"], "left")
        .join(total_watch, "quarter_start", "left")
        .join(revenue_pool, "quarter_start", "left")
        .withColumn(
            "partner_watched_seconds",
            F.coalesce(F.col("partner_watched_seconds"), F.lit(0)).cast("long"),
        )
        .withColumn(
            "total_watched_seconds",
            F.coalesce(F.col("total_watched_seconds"), F.lit(0)).cast("long"),
        )
        .withColumn(
            "watch_time_share",
            F.when(
                F.col("total_watched_seconds") > 0,
                F.col("partner_watched_seconds") / F.col("total_watched_seconds"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "revenue_share_inr",
            (
                F.coalesce(F.col("collected_revenue_inr"), F.lit(0).cast("decimal(18,2)"))
                * F.col("watch_time_share")
                * F.coalesce(F.col("revenue_share_pct"), F.lit(0.0))
                / F.lit(100.0)
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "minimum_guarantee",
            F.coalesce(
                F.col("minimum_guarantee_inr_per_quarter").cast("decimal(18,2)"),
                F.lit(0).cast("decimal(18,2)"),
            ),
        )
        .withColumn(
            "guarantee_topup_inr",
            F.greatest(
                F.col("minimum_guarantee") - F.col("revenue_share_inr"),
                F.lit(0).cast("decimal(18,2)"),
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "payout_inr",
            (F.col("revenue_share_inr") + F.col("guarantee_topup_inr")).cast("decimal(18,2)"),
        )
        .select(
            "quarter_start", "partner_id", "revenue_share_inr",
            "guarantee_topup_inr", "payout_inr",
            "partner_watched_seconds", "total_watched_seconds",
            "watch_time_share", "collected_revenue_inr",
        )
    )
    return allocated


def run_finance_royalties(
    watch_time_path: str = "lake/gold/watch_time_daily",
    revenue_path: str = "lake/gold/revenue_reconciliation",
    royalty_path: str = "lake/gold/royalties_quarterly",
    partners_path: str = "data/raw/reference/content_partners.csv",
    catalog_dir: str = "data/raw/content",
) -> None:
    spark = create_spark("StreamBharat-Finance-and-Royalties")
    try:
        watch_time = spark.read.format("delta").load(watch_time_path)
        revenue = spark.read.format("delta").load(revenue_path)
        title_history = build_title_history(spark, catalog_dir)
        partners = spark.read.option("header", True).option("inferSchema", True).csv(partners_path)
        royalties = build_partner_royalties(watch_time, title_history, revenue, partners)
        royalties.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(royalty_path)
    finally:
        spark.stop()


def run_reconciliation(
    subscription_path: str = "lake/bronze/billing_subscr",
    settlement_path: str = "lake/bronze/billing_settle",
    output_path: str = "lake/gold/revenue_reconciliation",
    match_window_hours: int = 48,
) -> None:
    spark = create_spark("StreamBharat-Revenue-Reconciliation")
    try:
        subscription = spark.read.format("delta").load(subscription_path)
        settlement = spark.read.format("delta").load(settlement_path)
        result = build_revenue_reconciliation(
            subscription, settlement, match_window_hours
        )
        result.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile subscription charges and calculate partner royalties"
    )
    parser.add_argument("--subscription", default="lake/bronze/billing_subscr")
    parser.add_argument("--settlement", default="lake/bronze/billing_settle")
    parser.add_argument("--revenue-output", default="lake/gold/revenue_reconciliation")
    parser.add_argument("--watch-time", default="lake/gold/watch_time_daily")
    parser.add_argument("--royalty-output", default="lake/gold/royalties_quarterly")
    parser.add_argument("--partners", default="data/raw/reference/content_partners.csv")
    parser.add_argument("--catalog-dir", default="data/raw/content")
    parser.add_argument("--match-window-hours", type=int, default=48)
    parser.add_argument("--reconcile-only", action="store_true")
    args = parser.parse_args()
    run_reconciliation(
        args.subscription, args.settlement, args.revenue_output,
        args.match_window_hours,
    )
    if not args.reconcile_only:
        run_finance_royalties(
            args.watch_time, args.revenue_output, args.royalty_output,
            args.partners, args.catalog_dir,
        )


if __name__ == "__main__":
    main()
