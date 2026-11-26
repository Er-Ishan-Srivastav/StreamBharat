"""Ad-break/impression reconciliation and daily campaign delivery metrics."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


def _payload(bronze: DataFrame) -> DataFrame:
    return bronze.withColumn(
        "_payload", F.from_json(F.col("raw_value"), MapType(StringType(), StringType()))
    )


def parse_breaks(bronze: DataFrame) -> DataFrame:
    """Parse the ad-break source while retaining broker lineage."""
    data = _payload(bronze)
    return data.select(
        F.element_at("_payload", F.lit("break_id")).alias("break_id"),
        F.element_at("_payload", F.lit("impression_id")).alias("impression_id"),
        F.element_at("_payload", F.lit("user_id")).alias("user_id"),
        F.element_at("_payload", F.lit("profile_id")).alias("profile_id"),
        F.element_at("_payload", F.lit("content_id")).alias("content_id"),
        F.to_timestamp(F.element_at("_payload", F.lit("break_ts"))).alias("break_ts"),
        F.element_at("_payload", F.lit("filled")).cast("int").alias("filled"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    ).filter(F.col("break_id").isNotNull() & F.col("break_ts").isNotNull())


def parse_impressions(bronze: DataFrame) -> DataFrame:
    """Parse completed ad-impression events from their raw Bronze envelopes."""
    data = _payload(bronze)
    return data.select(
        F.element_at("_payload", F.lit("impression_id")).alias("impression_id"),
        F.element_at("_payload", F.lit("user_id")).alias("user_id"),
        F.element_at("_payload", F.lit("profile_id")).alias("profile_id"),
        F.element_at("_payload", F.lit("content_id")).alias("content_id"),
        F.element_at("_payload", F.lit("campaign_id")).alias("campaign_id"),
        F.to_timestamp(F.element_at("_payload", F.lit("ad_break_ts"))).alias("ad_break_ts"),
        F.element_at("_payload", F.lit("duration_sec")).cast("int").alias("duration_sec"),
        F.element_at("_payload", F.lit("completed")).cast("int").alias("completed"),
        F.element_at("_payload", F.lit("cpm_inr")).cast("decimal(18,4)").alias("cpm_inr"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    ).filter(F.col("impression_id").isNotNull() & F.col("ad_break_ts").isNotNull())


def build_ad_delivery(
    breaks: DataFrame,
    impressions: DataFrame,
    campaigns: DataFrame,
    content_genres: DataFrame,
    lateness_minutes: int = 15,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return delivery facts, campaign-day metrics and hourly fill-rate metrics.

    Breaks are linked to impressions by the source impression_id. The event-time
    bound allows the ad duration plus a configurable lateness allowance.
    """
    if lateness_minutes < 0:
        raise ValueError("lateness_minutes cannot be negative")
    required_break = {"break_id", "impression_id", "user_id", "content_id", "break_ts", "filled"}
    required_impression = {
        "impression_id", "user_id", "content_id", "campaign_id",
        "ad_break_ts", "duration_sec", "cpm_inr",
    }
    if required_break - set(breaks.columns):
        raise ValueError(f"Break data missing: {sorted(required_break - set(breaks.columns))}")
    if required_impression - set(impressions.columns):
        raise ValueError(
            f"Impression data missing: {sorted(required_impression - set(impressions.columns))}"
        )

    b = breaks.dropDuplicates(["break_id"]).alias("b")
    i = impressions.dropDuplicates(["impression_id"]).alias("i")
    bound_seconds = F.col("i.duration_sec").cast("long") + F.lit(lateness_minutes * 60)
    impression_delay_seconds = (
        F.unix_timestamp("i.ad_break_ts") - F.unix_timestamp("b.break_ts")
    )
    join_condition = (
        (F.col("b.impression_id") == F.col("i.impression_id"))
        & (F.col("b.user_id") == F.col("i.user_id"))
        & (F.col("b.content_id") == F.col("i.content_id"))
        & impression_delay_seconds.between(F.lit(0), bound_seconds)
    )
    linked = b.join(i, join_condition, "left")
    matched = F.col("i.impression_id").isNotNull()
    delivery_from_break = linked.select(
        F.coalesce(F.col("i.impression_id"), F.concat(F.lit("BREAK:"), F.col("b.break_id"))).alias("delivery_id"),
        F.col("b.break_id").alias("break_id"),
        F.col("i.impression_id").alias("impression_id"),
        F.col("i.campaign_id").alias("campaign_id"),
        F.col("b.user_id").alias("user_id"),
        F.coalesce(F.col("i.profile_id"), F.col("b.profile_id")).alias("profile_id"),
        F.col("b.content_id").alias("content_id"),
        F.col("b.break_ts").alias("break_ts"),
        F.col("i.ad_break_ts").alias("ad_break_ts"),
        F.col("i.duration_sec").alias("duration_sec"),
        F.col("i.completed").alias("completed"),
        F.col("i.cpm_inr").alias("cpm_inr"),
        F.when(matched, F.lit("DELIVERED"))
         .when(F.coalesce(F.col("b.filled"), F.lit(0)) == 0, F.lit("UNFILLED_BREAK"))
         .otherwise(F.lit("FILLED_WITHOUT_IMPRESSION")).alias("delivery_status"),
        F.col("b.kafka_partition").alias("break_partition"),
        F.col("b.kafka_offset").alias("break_offset"),
        F.col("i.kafka_partition").alias("impression_partition"),
        F.col("i.kafka_offset").alias("impression_offset"),
    )

    matched_ids = (
        linked.filter(matched)
        .select(F.col("i.impression_id").alias("impression_id"))
        .dropDuplicates()
    )
    orphan_impressions = (
        impressions.join(matched_ids, "impression_id", "left_anti")
        .select(
            F.concat(F.lit("IMPRESSION:"), F.col("impression_id")).alias("delivery_id"),
            F.lit(None).cast("string").alias("break_id"),
            "impression_id", "campaign_id", "user_id", "profile_id", "content_id",
            F.lit(None).cast("timestamp").alias("break_ts"),
            "ad_break_ts", "duration_sec", "completed", "cpm_inr",
            F.lit("ORPHAN_IMPRESSION").alias("delivery_status"),
            F.lit(None).cast("int").alias("break_partition"),
            F.lit(None).cast("long").alias("break_offset"),
            "kafka_partition", "kafka_offset",
        ).withColumnRenamed("kafka_partition", "impression_partition")
         .withColumnRenamed("kafka_offset", "impression_offset")
    )
    delivery = delivery_from_break.unionByName(orphan_impressions)

    campaign_contract = campaigns.select(
        "campaign_id",
        F.col("budget_inr").cast("double").alias("budget_inr"),
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.col("daily_frequency_cap").cast("int").alias("daily_frequency_cap"),
        F.coalesce(F.col("target_genres"), F.lit("ALL")).alias("target_genres"),
    )
    genres = content_genres.select(
        "content_id",
        F.coalesce("genres", F.array().cast("array<string>")).alias("content_genres"),
    )
    enriched = (
        delivery.join(campaign_contract, "campaign_id", "left")
        .join(genres, "content_id", "left")
        .withColumn("target_genre_list", F.split(F.col("target_genres"), r"\|"))
        .withColumn(
            "genre_eligible",
            F.when(
                F.upper(F.trim(F.col("target_genres"))) == "ALL",
                F.lit(True),
            ).otherwise(
                F.size(
                    F.array_intersect(
                        F.transform(F.col("target_genre_list"), lambda x: F.lower(F.trim(x))),
                        F.transform(
                            F.coalesce(F.col("content_genres"), F.array().cast("array<string>")),
                            lambda x: F.lower(F.trim(x)),
                        ),
                    )
                ) > 0
            ),
        )
        .withColumn("activity_date", F.to_date(F.coalesce("ad_break_ts", "break_ts")))
        .withColumn(
            "spend_inr",
            F.when(
                F.col("delivery_status") == "DELIVERED",
                F.col("cpm_inr").cast("double") / F.lit(1000.0),
            ).otherwise(F.lit(0.0)),
        )
    )
    delivery = enriched.select(
        "delivery_id", "break_id", "impression_id", "campaign_id", "user_id",
        "profile_id", "content_id", "break_ts", "ad_break_ts", "duration_sec",
        "completed", "cpm_inr", "delivery_status", "genre_eligible",
        "activity_date", "spend_inr", "break_partition", "break_offset",
        "impression_partition", "impression_offset",
    )

    user_campaign_day = (
        delivery.filter(F.col("delivery_status") == "DELIVERED")
        .groupBy("activity_date", "campaign_id", "user_id")
        .agg(F.countDistinct("impression_id").alias("user_impressions"))
        .join(
            campaign_contract.select("campaign_id", "daily_frequency_cap"),
            "campaign_id",
            "left",
        )
        .withColumn(
            "frequency_cap_breach",
            F.col("user_impressions") > F.col("daily_frequency_cap"),
        )
    )
    frequency = user_campaign_day.groupBy("activity_date", "campaign_id").agg(
        F.sum(F.col("frequency_cap_breach").cast("int")).alias("frequency_cap_breaches")
    )
    campaign_day = (
        delivery.filter(F.col("campaign_id").isNotNull() & F.col("activity_date").isNotNull())
        .groupBy("activity_date", "campaign_id")
        .agg(
            F.sum((F.col("delivery_status") == "DELIVERED").cast("long")).alias("impressions"),
            F.sum("spend_inr").alias("spend_inr"),
            F.sum((F.col("delivery_status") == "UNFILLED_BREAK").cast("long")).alias("unfilled_breaks"),
            F.sum((F.col("delivery_status") == "FILLED_WITHOUT_IMPRESSION").cast("long")).alias("filled_missing_impression"),
            F.sum((F.col("delivery_status") == "ORPHAN_IMPRESSION").cast("long")).alias("orphan_impressions"),
            F.sum(
                ((F.col("delivery_status") == "DELIVERED") & (~F.col("genre_eligible"))).cast("long")
            ).alias("genre_mismatch_count"),
        )
        .join(frequency, ["activity_date", "campaign_id"], "left")
        .join(campaign_contract, "campaign_id", "left")
        .withColumn(
            "campaign_days",
            F.greatest(F.datediff("end_date", "start_date") + F.lit(1), F.lit(1)),
        )
        .withColumn("daily_budget_inr", F.col("budget_inr") / F.col("campaign_days"))
        .withColumn(
            "pacing_ratio",
            F.when(F.col("daily_budget_inr") > 0, F.col("spend_inr") / F.col("daily_budget_inr")),
        )
        .withColumn(
            "alert_status",
            F.when(F.col("genre_mismatch_count") > 0, F.lit("GENRE_TARGET_BREACH"))
             .when(F.col("frequency_cap_breaches") > 0, F.lit("FREQUENCY_CAP_BREACH"))
             .when(F.col("pacing_ratio") > 1.2, F.lit("OVERSPENDING"))
             .otherwise(F.lit("ON_TRACK")),
        )
        .drop("campaign_days", "start_date", "end_date", "budget_inr", "daily_frequency_cap", "target_genres")
    )
    campaign_day = campaign_day.withColumn(
        "frequency_cap_breaches", F.coalesce("frequency_cap_breaches", F.lit(0))
    )

    hourly = (
        delivery.filter(F.col("break_ts").isNotNull())
        .withColumn("hour_start", F.date_trunc("hour", "break_ts"))
        .groupBy("hour_start")
        .agg(
            F.count("break_id").alias("total_breaks"),
            F.sum((F.col("delivery_status") == "DELIVERED").cast("long")).alias("filled_breaks"),
            F.sum((F.col("delivery_status") == "UNFILLED_BREAK").cast("long")).alias("unfilled_breaks"),
        )
        .withColumn(
            "fill_rate",
            F.when(F.col("total_breaks") > 0, F.col("filled_breaks") / F.col("total_breaks")),
        )
    )
    return delivery, campaign_day, hourly


def run_ad_matching(
    breaks_path: str = "lake/bronze/ads_breaks",
    impressions_path: str = "lake/bronze/ads_impressions",
    campaigns_path: str = "data/raw/ads/ad_campaigns.csv",
    catalog_path: str = "data/raw/content/catalog_2026-10-01.json",
    delivery_path: str = "lake/silver/ad_delivery",
    campaign_gold_path: str = "lake/gold/campaign_daily",
    hourly_gold_path: str = "lake/gold/ad_fill_rate_hourly",
    date_arg: str | None = None,
) -> None:
    spark = create_spark("StreamBharat-Ad-Delivery-Reconciliation")
    try:
        breaks = parse_breaks(spark.read.format("delta").load(breaks_path))
        impressions = parse_impressions(spark.read.format("delta").load(impressions_path))
        if date_arg:
            target_date = F.to_date(F.lit(date_arg))
            breaks = breaks.filter(F.to_date("break_ts") == target_date)
            impressions = impressions.filter(F.to_date("ad_break_ts") == target_date)
        campaigns = spark.read.option("header", True).option("inferSchema", True).csv(campaigns_path)
        catalog = read_catalog_snapshot(spark, catalog_path)
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(campaigns_path)), "reference", "genre_mapping.csv")
        genre_mapping = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(mapping_path)
            .select(
                F.lower(F.trim("raw_genre")).alias("raw_genre_key"),
                F.col("standard_genre"),
            )
        )
        content_genres = (
            catalog.select("content_id", F.explode_outer("genres").alias("raw_genre"))
            .withColumn("raw_genre_key", F.lower(F.trim("raw_genre")))
            .join(genre_mapping, "raw_genre_key", "left")
            .withColumn("standard_genre", F.coalesce("standard_genre", "raw_genre"))
            .groupBy("content_id")
            .agg(F.collect_set("standard_genre").alias("genres"))
        )
        delivery, campaign_day, hourly = build_ad_delivery(
            breaks, impressions, campaigns, content_genres
        )
        _write_delta(spark, delivery, delivery_path, ["delivery_id"], date_arg)
        _write_delta(spark, campaign_day, campaign_gold_path, ["activity_date", "campaign_id"], date_arg)
        _write_delta(spark, hourly, hourly_gold_path, ["hour_start"], date_arg)
    finally:
        spark.stop()


def _write_delta(spark, frame: DataFrame, path: str, keys: list[str], date_arg: str | None):
    if date_arg and DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path).alias("target")
            .merge(
                frame.dropDuplicates(keys).alias("source"),
                " AND ".join(f"target.{key} = source.{key}" for key in keys),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Match ad breaks and impressions")
    parser.add_argument("--breaks", default="lake/bronze/ads_breaks")
    parser.add_argument("--impressions", default="lake/bronze/ads_impressions")
    parser.add_argument("--campaigns", default="data/raw/ads/ad_campaigns.csv")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--delivery", default="lake/silver/ad_delivery")
    parser.add_argument("--campaign-gold", default="lake/gold/campaign_daily")
    parser.add_argument("--hourly-gold", default="lake/gold/ad_fill_rate_hourly")
    parser.add_argument("--date", help="Rebuild ad delivery for one ad-break date")
    args = parser.parse_args()
    run_ad_matching(
        args.breaks, args.impressions, args.campaigns, args.catalog,
        args.delivery, args.campaign_gold, args.hourly_gold, args.date,
    )


if __name__ == "__main__":
    main()
