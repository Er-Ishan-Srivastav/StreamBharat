"""Gold viewing sessions and drop-off analytics from canonical Silver playback."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


PLAYER_EVENTS = ("start", "heartbeat", "pause", "stop", "complete", "end")


def build_viewing_sessions(
    player: DataFrame,
    catalog: DataFrame,
    session_gap_seconds: int = 1800,
    heartbeat_max_seconds: int = 300,
) -> tuple[DataFrame, DataFrame]:
    """Return session facts and daily content drop-off aggregates.

    A session is scoped to profile/device/content and restarts after a 30-minute
    event-time gap. Watched time is credited only for forward-progress heartbeat
    intervals of at most five minutes; seeks and end-screen overrun do not count.
    """
    if session_gap_seconds < 1 or heartbeat_max_seconds < 1:
        raise ValueError("session and heartbeat limits must be positive")

    events = (
        player.filter(F.col("event_type").isin(*PLAYER_EVENTS))
        .filter(
            F.col("profile_id").isNotNull()
            & F.col("device_id").isNotNull()
            & F.col("content_id").isNotNull()
        )
        .withColumn("event_ts", F.to_timestamp("event_ts_corrected"))
        .filter(F.col("event_ts").isNotNull())
        .join(
            catalog.select(
                "content_id",
                "title",
                "duration_sec",
                "parent_series_id",
                F.element_at("languages", 1).alias("language"),
            ).dropDuplicates(["content_id"]),
            "content_id",
            "left",
        )
        .withColumn("duration_bound", F.coalesce(F.col("duration_sec").cast("double"), F.lit(1.0e12)))
        .withColumn(
            "position_clamped",
            F.least(
                F.greatest(F.coalesce(F.col("position_sec").cast("double"), F.lit(0.0)), F.lit(0.0)),
                F.col("duration_bound"),
            ),
        )
    )

    ordering = Window.partitionBy(
        "profile_id", "device_id", "content_id"
    ).orderBy("event_ts", "event_id")
    preceding = ordering.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    ordered = (
        events
        .withColumn("previous_event_ts", F.lag("event_ts").over(ordering))
        .withColumn("previous_event_type", F.lag("event_type").over(ordering))
        .withColumn("previous_position", F.lag("position_clamped").over(ordering))
        .withColumn(
            "gap_seconds",
            F.col("event_ts").cast("long") - F.col("previous_event_ts").cast("long"),
        )
        .withColumn(
            "position_delta",
            F.col("position_clamped") - F.col("previous_position"),
        )
        .withColumn(
            "new_session",
            F.when(
                F.col("previous_event_ts").isNull()
                | (F.col("gap_seconds") > session_gap_seconds),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("session_sequence", F.sum("new_session").over(preceding))
        .withColumn(
            "session_start_ts",
            F.last(
                F.when(F.col("new_session") == 1, F.col("event_ts")),
                ignorenulls=True,
            ).over(preceding),
        )
        .withColumn(
            "session_id",
            F.sha2(
                F.concat_ws(
                    "|", "profile_id", "device_id", "content_id",
                    F.col("session_start_ts").cast("string"),
                ),
                256,
            ),
        )
    )

    valid_heartbeat = (
        (F.col("event_type") == "heartbeat")
        & F.col("previous_event_type").isin("start", "heartbeat")
        & F.col("gap_seconds").between(1, heartbeat_max_seconds)
        & (F.col("position_delta") > 0)
        & (
            F.col("position_delta")
            <= F.col("gap_seconds") + F.lit(5)
        )
    )
    ordered = ordered.withColumn(
        "watched_seconds_credit",
        F.when(
            valid_heartbeat,
            F.least(
                F.col("gap_seconds").cast("double"),
                F.col("position_delta"),
                F.greatest(
                    F.col("duration_bound") - F.col("previous_position"),
                    F.lit(0.0),
                ),
                F.lit(float(heartbeat_max_seconds)),
            ),
        ).otherwise(F.lit(0.0)),
    )

    sessions = (
        ordered.groupBy(
            "session_id",
            "session_start_ts",
            "user_id",
            "profile_id",
            "device_id",
            "content_id",
            "title",
            "parent_series_id",
            "language",
        )
        .agg(
            F.max("event_ts").alias("session_end_ts"),
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.sum("watched_seconds_credit").cast("long").alias("watched_seconds"),
            F.max("position_clamped").alias("max_position_sec"),
            F.max("duration_sec").alias("duration_sec"),
            F.max(
                F.when(F.col("event_type") == "complete", F.lit(1)).otherwise(F.lit(0))
            ).alias("completion_event"),
        )
        .withColumn("viewing_date", F.to_date("session_start_ts"))
        .withColumn(
            "session_duration_seconds",
            F.col("session_end_ts").cast("long") - F.col("session_start_ts").cast("long"),
        )
        .withColumn(
            "completion_rate",
            F.when(
                F.col("duration_sec") > 0,
                F.least(F.col("max_position_sec") / F.col("duration_sec"), F.lit(1.0)),
            ),
        )
        .withColumn("completed", F.col("completion_event") == 1)
        .withColumn(
            "dropoff_seconds",
            F.when(
                F.col("duration_sec") > 0,
                F.greatest(
                    F.col("duration_sec") - F.col("max_position_sec"),
                    F.lit(0.0),
                ),
            ),
        )
        .withColumn(
            "report_content_id",
            F.coalesce(F.col("parent_series_id"), F.col("content_id")),
        )
        .select(
            "session_id", "viewing_date", "user_id", "profile_id", "device_id",
            "content_id", "report_content_id", "title", "language",
            "session_start_ts", "session_end_ts", "session_duration_seconds",
            "event_count", "watched_seconds", "max_position_sec", "duration_sec",
            "completion_rate", "completed", "dropoff_seconds",
        )
    )

    dropoff = (
        sessions.groupBy("viewing_date", "report_content_id", "language")
        .agg(
            F.count(F.lit(1)).cast("long").alias("sessions"),
            F.countDistinct("profile_id").cast("long").alias("viewers"),
            F.sum(F.col("watched_seconds")).cast("long").alias("watched_seconds"),
            F.sum(F.col("completed").cast("int")).cast("long").alias("completed_sessions"),
            F.avg("completion_rate").alias("average_completion_rate"),
            F.avg("dropoff_seconds").alias("average_dropoff_seconds"),
        )
    )
    return sessions, dropoff


def run_session_analytics(
    player_path: str = "lake/silver/player_events",
    catalog_path: str = "data/raw/content/catalog_2026-10-01.json",
    session_path: str = "lake/gold/viewing_sessions",
    dropoff_path: str = "lake/gold/content_dropoff_daily",
    date_arg: str | None = None,
) -> None:
    spark = create_spark("StreamBharat-Viewing-Session-Analytics")
    try:
        player = spark.read.format("delta").load(player_path)
        if date_arg:
            target_date = F.to_date(F.lit(date_arg))
            # Retain previous and next day as boundary buffers for sessions crossing midnight.
            player = player.filter(
                F.to_date("event_ts_corrected").between(
                    F.date_sub(target_date, 1), F.date_add(target_date, 1)
                )
            )
        catalog = read_catalog_snapshot(spark, catalog_path)
        sessions, dropoff = build_viewing_sessions(player, catalog)
        if date_arg:
            sessions = sessions.filter(F.col("viewing_date") == target_date)
            dropoff = dropoff.filter(F.col("viewing_date") == target_date)
        _write_delta(
            spark, sessions, session_path, ["session_id"], "viewing_date", date_arg
        )
        _write_delta(
            spark, dropoff, dropoff_path,
            ["viewing_date", "report_content_id", "language"], "viewing_date", date_arg,
        )
    finally:
        spark.stop()


def _write_delta(
    spark, frame: DataFrame, path: str, keys: list[str],
    date_column: str, date_arg: str | None,
):
    if date_arg and DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete(
            F.to_date(F.col(date_column)) == F.to_date(F.lit(date_arg))
        )
        frame.dropDuplicates(keys).write.format("delta").mode("append").save(path)
    else:
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build 30-minute viewing sessions and daily content drop-off"
    )
    parser.add_argument("--player", default="lake/silver/player_events")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--sessions", default="lake/gold/viewing_sessions")
    parser.add_argument("--dropoff", default="lake/gold/content_dropoff_daily")
    parser.add_argument("--date", help="Rebuild sessions started on this viewing date")
    args = parser.parse_args()
    run_session_analytics(
        args.player, args.catalog, args.sessions, args.dropoff, args.date
    )


if __name__ == "__main__":
    main()
