"""Gold engagement aggregates from the Kafka Silver player table."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


def build_engagement_gold(
    spark,
    player_path: str,
    catalog_path: str,
    gold_root: str,
    date_arg: str | None = None,
) -> None:
    """Build watch-time, DAU/MAU and language-level top-title Gold tables."""
    events = spark.read.format("delta").load(player_path)
    required = {
        "event_id", "user_id", "profile_id", "device_id", "content_id",
        "event_type", "position_sec", "event_ts_corrected",
    }
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"Silver player table is missing columns: {sorted(missing)}")

    events = (
        events.filter(
            F.col("user_id").isNotNull()
            & F.col("profile_id").isNotNull()
            & F.col("content_id").isNotNull()
            & F.col("event_ts_corrected").isNotNull()
        )
        .withColumn("event_ts", F.col("event_ts_corrected").cast("timestamp"))
        .withColumn("viewing_date", F.to_date("event_ts"))
        .filter(F.col("event_type").isin("start", "heartbeat", "pause", "stop", "complete"))
    )
    target_date = F.to_date(F.lit(date_arg)) if date_arg else None
    if date_arg:
        events = events.filter(
            (F.col("viewing_date") >= F.date_sub(target_date, 29))
            & (F.col("viewing_date") <= target_date)
        )

    catalog = read_catalog_snapshot(spark, catalog_path)
    content = (
        catalog.select(
            "content_id",
            "title",
            "duration_sec",
            "parent_series_id",
            F.element_at("languages", 1).alias("language"),
        )
        .dropDuplicates(["content_id"])
    )
    parents = content.select(
        F.col("content_id").alias("parent_id"),
        F.col("title").alias("parent_title"),
    )

    by_content = (
        events.join(content.alias("child"), "content_id", "left")
        .join(
            parents.alias("parent"),
            F.col("child.parent_series_id") == F.col("parent.parent_id"),
            "left",
        )
        .withColumn(
            "report_content_id",
            F.coalesce(F.col("child.parent_series_id"), F.col("content_id")),
        )
        .withColumn(
            "report_title",
            F.coalesce(F.col("parent.parent_title"), F.col("child.title")),
        )
        .withColumn("language", F.coalesce(F.col("child.language"), F.lit("Unknown")))
        .withColumn("duration_sec", F.col("child.duration_sec").cast("double"))
    )

    event_order = Window.partitionBy(
        "profile_id", "device_id", "content_id"
    ).orderBy("event_ts", "event_id")
    ordered = (
        by_content
        .withColumn("previous_event_ts", F.lag("event_ts").over(event_order))
        .withColumn("previous_event_type", F.lag("event_type").over(event_order))
        .withColumn("previous_position", F.lag("position_sec").over(event_order))
        .withColumn(
            "gap_seconds",
            F.col("event_ts").cast("long")
            - F.col("previous_event_ts").cast("long"),
        )
        .withColumn(
            "position_delta",
            F.col("position_sec").cast("double")
            - F.col("previous_position").cast("double"),
        )
        .withColumn(
            "new_session",
            F.when(
                F.col("previous_event_ts").isNull()
                | (F.col("gap_seconds") > 1800),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "session_number",
            F.sum("new_session").over(
                event_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "session_id",
            F.sha2(
                F.concat_ws(
                    "|", "profile_id", "device_id", "content_id",
                    F.col("session_number").cast("string"),
                ),
                256,
            ),
        )
    )

    # Credit only heartbeat intervals after a start/heartbeat, up to five minutes.
    # A large forward position jump is treated as a seek and receives no watch credit.
    valid_interval = (
        (F.col("event_type") == "heartbeat")
        & F.col("previous_event_type").isin("start", "heartbeat")
        & F.col("gap_seconds").between(1, 300)
        & (F.col("position_delta") > 0)
        & (F.col("position_delta") <= F.col("gap_seconds") + 5)
    )
    remaining_duration = F.greatest(
        F.coalesce(F.col("duration_sec"), F.lit(1.0e12))
        - F.col("previous_position").cast("double"),
        F.lit(0.0),
    )
    ordered = ordered.withColumn(
        "watched_seconds",
        F.when(
            valid_interval,
            F.least(
                F.col("gap_seconds").cast("double"),
                F.col("position_delta"),
                remaining_duration,
                F.lit(300.0),
            ),
        ).otherwise(F.lit(0.0)),
    )

    daily_ordered = ordered
    history_ordered = ordered
    if date_arg:
        daily_ordered = ordered.filter(F.col("viewing_date") == target_date)

    watch_time = (
        daily_ordered.groupBy(
            F.col("viewing_date"),
            "profile_id",
            "user_id",
            "content_id",
        )
        .agg(
            F.sum("watched_seconds").cast("long").alias("watched_seconds"),
            F.max(F.col("position_sec").cast("double")).alias("max_position_sec"),
            F.max("duration_sec").alias("duration_sec"),
        )
        .withColumn(
            "completion_rate",
            F.when(
                F.col("duration_sec") > 0,
                F.least(
                    F.col("max_position_sec") / F.col("duration_sec"),
                    F.lit(1.0),
                ),
            ),
        )
        .select(
            "viewing_date", "profile_id", "user_id", "content_id",
            "watched_seconds", "completion_rate",
        )
    )
    _write_gold(watch_time, os.path.join(gold_root, "watch_time_daily"),
                ["viewing_date", "profile_id", "content_id"], date_arg)

    daily_users = (
        history_ordered.filter(F.col("event_type").isin("start", "heartbeat"))
        .select(
            F.col("viewing_date").alias("activity_date"),
            "user_id",
        )
        .dropDuplicates()
    )
    dau_input = (
        daily_users.filter(F.col("activity_date") == target_date)
        if date_arg else daily_users
    )
    dau = dau_input.groupBy("activity_date").agg(
        F.countDistinct("user_id").cast("long").alias("active_users")
    ).withColumn("metric_type", F.lit("DAU"))

    days = dau_input.select("activity_date").distinct().alias("days")
    users = daily_users.alias("users")
    mau = (
        days.join(
            users,
            (F.col("users.activity_date") <= F.col("days.activity_date"))
            & (
                F.col("users.activity_date")
                >= F.date_sub(F.col("days.activity_date"), 29)
            ),
            "left",
        )
        .groupBy(F.col("days.activity_date").alias("activity_date"))
        .agg(F.countDistinct("users.user_id").cast("long").alias("active_users"))
        .withColumn("metric_type", F.lit("MAU"))
    )
    active_users = dau.select("activity_date", "metric_type", "active_users").unionByName(
        mau.select("activity_date", "metric_type", "active_users")
    )
    _write_gold(active_users, os.path.join(gold_root, "active_users_daily"),
                ["activity_date", "metric_type"], date_arg)

    ranked_titles = (
        daily_ordered.filter(F.col("event_type").isin("start", "heartbeat"))
        .groupBy(
            F.col("viewing_date").alias("activity_date"),
            "language",
            F.col("report_content_id").alias("content_id"),
            F.col("report_title").alias("title"),
        )
        .agg(
            F.countDistinct("profile_id").cast("long").alias("viewers"),
            F.sum("watched_seconds").cast("long").alias("watched_seconds"),
        )
    )
    ranking = Window.partitionBy("activity_date", "language").orderBy(
        F.col("viewers").desc(),
        F.col("watched_seconds").desc(),
        F.col("content_id"),
    )
    top_titles = (
        ranked_titles.withColumn("rank_no", F.row_number().over(ranking))
        .filter(F.col("rank_no") <= 10)
        .select(
            "activity_date", "language", "rank_no", "content_id",
            "title", "viewers", "watched_seconds",
        )
    )
    _write_gold(top_titles, os.path.join(gold_root, "top_titles_daily"),
                ["activity_date", "language", "rank_no"], date_arg)


def _write_gold(frame, path: str, keys: list[str], date_arg: str | None) -> None:
    if date_arg and DeltaTable.isDeltaTable(frame.sparkSession, path):
        date_column = keys[0]
        target = DeltaTable.forPath(frame.sparkSession, path)
        target.delete(
            F.to_date(F.col(date_column)) == F.to_date(F.lit(date_arg))
        )
        frame.dropDuplicates(keys).write.format("delta").mode("append").save(path)
    else:
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build watch-time, DAU/MAU and top-title Gold analytics"
    )
    parser.add_argument("--player", default="lake/silver/player_events")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--gold-root", default="lake/gold")
    parser.add_argument("--date", help="Recompute one viewing date; MAU includes the prior 29 days")
    args = parser.parse_args()

    spark = create_spark("StreamBharat-Gold-Engagement-Analytics")
    try:
        build_engagement_gold(spark, args.player, args.catalog, args.gold_root, args.date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
