"""Q17 — Trending titles from the live simulator or Kafka topic."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from streambharat.spark_session import create_spark

PLAYS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("profile_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("event_ts", StringType(), True),
])


def build_historical_baseline(
    spark,
    bronze_path: str,
    catalog_path: str,
    output_path: str,
    as_of: str,
    window_minutes: int = 10,
) -> None:
    """Build an average unique-viewer baseline from the previous seven full days."""
    if window_minutes < 1 or 60 % window_minutes != 0:
        raise ValueError("window_minutes must be a positive divisor of 60")
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"Catalogue snapshot not found: {catalog_path}")
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        raise FileNotFoundError(f"Player Delta table not found at {bronze_path}")

    with open(catalog_path, "r", encoding="utf-8") as handle:
        catalog = json.load(handle)
    rows = [
        (row.get("content_id"), (row.get("languages") or ["Unknown"])[0])
        for row in catalog
        if row.get("type") in ("movie", "series") and row.get("content_id")
    ]
    catalog_df = spark.createDataFrame(rows, ["content_id", "language"])

    source = spark.read.format("delta").load(bronze_path)
    if "event_ts_corrected" in source.columns:
        event_time_column = "event_ts_corrected"
    elif "server_ts" in source.columns:
        event_time_column = "server_ts"
    elif "event_ts" in source.columns:
        event_time_column = "event_ts"
    else:
        raise ValueError(
            "Player history must contain event_ts_corrected, server_ts or event_ts"
        )

    historical = (
        source.filter(F.col("event_type") == "start")
        .withColumn("event_ts", F.to_timestamp(F.col(event_time_column)))
        .withColumn("event_date", F.to_date("event_ts"))
        .filter(
            (F.col("event_date") >= F.date_sub(F.to_date(F.lit(as_of)), 7))
            & (F.col("event_date") < F.to_date(F.lit(as_of)))
        )
    )
    if "download_id" in historical.columns:
        historical = historical.filter(
            F.col("download_id").isNull() | (F.trim("download_id") == "")
        )

    historical = (
        historical.join(F.broadcast(catalog_df), "content_id", "inner")
        .withColumn("hour_of_day", F.hour("event_ts"))
        .withColumn(
            "baseline_window_start",
            F.window("event_ts", f"{window_minutes} minutes").getField("start"),
        )
        .groupBy(
            "content_id", "language", "hour_of_day",
            "event_date", "baseline_window_start",
        )
        .agg(F.countDistinct("profile_id").alias("window_unique_viewers"))
    )
    baseline_column = f"baseline_avg_{window_minutes}m_viewers"
    baseline = historical.groupBy(
        "content_id", "language", "hour_of_day"
    ).agg(
        F.avg("window_unique_viewers").alias(baseline_column)
    )
    baseline.write.format("delta").mode("overwrite").save(output_path)


def _merge_trends(
    batch_df,
    batch_id: int,
    target_path: str,
    baseline_df,
    min_viewers: int,
    top_n: int,
    spark,
    window_minutes: int = 10,
    kafka_bootstrap_servers: str | None = None,
    kafka_topic: str = "ott.metrics.trending",
) -> None:
    """Replace each recomputed window/language snapshot and optionally publish it."""
    if batch_df.isEmpty():
        return

    active_keys = batch_df.select("window", "language").distinct()
    baseline_column = f"baseline_avg_{window_minutes}m_viewers"
    enriched = (
        batch_df.withColumn("hour_of_day", F.hour("window.start"))
        .join(F.broadcast(baseline_df), ["content_id", "language", "hour_of_day"], "left")
        .withColumn(
            baseline_column,
            F.coalesce(F.col(baseline_column), F.lit(0.0)),
        )
        .withColumn(
            "growth_ratio",
            F.col("unique_viewers")
            / F.greatest(F.col(baseline_column), F.lit(1.0)),
        )
        .filter(F.col("unique_viewers") >= F.lit(min_viewers))
    )
    ordering = Window.partitionBy(
        "window.start", "window.end", "language"
    ).orderBy(
        F.col("growth_ratio").desc(),
        F.col("unique_viewers").desc(),
        F.col("content_id"),
    )
    ranked = (
        enriched.withColumn("rank_in_language", F.row_number().over(ordering))
        .filter(F.col("rank_in_language") <= top_n)
        .withColumn("processed_at", F.current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        target.alias("target").merge(
            active_keys.alias("active"),
            """
            target.window.start = active.window.start
            AND target.window.end = active.window.end
            AND target.language = active.language
            """,
        ).whenMatchedDelete().execute()
        if not ranked.isEmpty():
            ranked.write.format("delta").mode("append").save(target_path)
    elif not ranked.isEmpty():
        ranked.write.format("delta").mode("overwrite").save(target_path)

    if kafka_bootstrap_servers:
        ranked_slots = ranked.select("window", "language", "rank_in_language")
        rank_numbers = spark.range(1, top_n + 1).select(
            F.col("id").cast("int").alias("rank_in_language")
        )
        empty_slots = (
            active_keys.crossJoin(rank_numbers)
            .join(
                ranked_slots,
                ["window", "language", "rank_in_language"],
                "left_anti",
            )
        )
        ranked_kafka = ranked.select(
            F.concat_ws(
                "|",
                F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.col("language"),
                F.col("rank_in_language").cast("string"),
            ).alias("key"),
            F.to_json(
                F.struct(
                    F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "language",
                    "content_id",
                    "unique_viewers",
                    "hour_of_day",
                    F.col(baseline_column).alias("baseline_avg_window_viewers"),
                    "growth_ratio",
                    "rank_in_language",
                    "processed_at",
                    F.lit(False).alias("is_empty"),
                )
            ).alias("value"),
        )
        empty_kafka = empty_slots.select(
            F.concat_ws(
                "|",
                F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.col("language"),
                F.col("rank_in_language").cast("string"),
            ).alias("key"),
            F.to_json(
                F.struct(
                    F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "language",
                    F.lit(None).cast("string").alias("content_id"),
                    F.lit(0).cast("long").alias("unique_viewers"),
                    F.hour(F.col("window.start")).alias("hour_of_day"),
                    F.lit(0.0).alias("baseline_avg_window_viewers"),
                    F.lit(0.0).alias("growth_ratio"),
                    "rank_in_language",
                    F.current_timestamp().alias("processed_at"),
                    F.lit(True).alias("is_empty"),
                )
            ).alias("value"),
        )
        kafka_rows = ranked_kafka.unionByName(empty_kafka)
        if not kafka_rows.isEmpty():
            (
                kafka_rows.write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("topic", kafka_topic)
                .save()
            )


def _persist_and_recompute_trends(
    incoming_df,
    batch_id: int,
    live_events_path: str,
    output_path: str,
    baseline_df,
    min_viewers: int,
    top_n: int,
    spark,
    late_minutes: int = 15,
    window_minutes: int = 10,
    slide_duration: str = "1 minute",
    kafka_bootstrap_servers: str | None = None,
    kafka_topic: str = "ott.metrics.trending",
) -> None:
    """Persist unique starts and recompute event-time windows over the late-data horizon."""
    if incoming_df.isEmpty():
        return
    if late_minutes < 0 or window_minutes < 1:
        raise ValueError("late_minutes cannot be negative and window_minutes must be positive")

    incoming = (
        incoming_df.filter(F.col("event_id").isNotNull())
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .filter(F.col("event_ts").isNotNull())
        .dropDuplicates(["event_id"])
    )
    if incoming.isEmpty():
        return

    if not DeltaTable.isDeltaTable(spark, live_events_path):
        incoming.write.format("delta").mode("overwrite").save(live_events_path)
    else:
        (
            DeltaTable.forPath(spark, live_events_path)
            .alias("target")
            .merge(incoming.alias("source"), "target.event_id = source.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    events = spark.read.format("delta").load(live_events_path)
    max_ts = events.agg(F.max("event_ts").alias("max_ts")).first()["max_ts"]
    if max_ts is None:
        return

    lower_bound = max_ts - timedelta(minutes=window_minutes + late_minutes)
    recent = events.filter(F.col("event_ts") >= F.lit(lower_bound))
    windows = (
        recent.groupBy(
            F.window(
                "event_ts",
                f"{window_minutes} minutes",
                slide_duration,
            ).alias("window"),
            "language",
            "content_id",
        )
        .agg(F.countDistinct("profile_id").alias("unique_viewers"))
    )
    _merge_trends(
        windows,
        batch_id,
        output_path,
        baseline_df,
        min_viewers,
        top_n,
        spark,
        window_minutes,
        kafka_bootstrap_servers,
        kafka_topic,
    )
    DeltaTable.forPath(spark, live_events_path).delete(
        F.col("event_ts") < F.lit(lower_bound)
    )


def run_q17(
    input_path: str,
    bronze_path: str,
    catalog_path: str,
    baseline_path: str,
    output_path: str,
    checkpoint_path: str,
    as_of: str,
    min_viewers: int = 200,
    top_n: int = 20,
    max_files_per_trigger: int = 10,
    late_minutes: int = 15,
) -> None:
    if not os.path.isdir(input_path):
        raise FileNotFoundError(
            f"Stream folder not found: {input_path}. Start the plays simulator first."
        )
    if min_viewers < 1 or top_n < 1 or late_minutes < 0:
        raise ValueError("min_viewers/top_n must be positive; late_minutes cannot be negative")

    spark = create_spark("StreamBharat-Q17-Real-Time-Trending")
    try:
        build_historical_baseline(
            spark, bronze_path, catalog_path, baseline_path, as_of, window_minutes=10
        )
        baseline_df = spark.read.format("delta").load(baseline_path)
        stream = (
            spark.readStream.schema(PLAYS_SCHEMA)
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .json(input_path)
        )
        os.makedirs(os.path.dirname(os.path.abspath(checkpoint_path)), exist_ok=True)
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        live_path = os.path.join(os.path.dirname(output_path), "_live_play_events")
        query = (
            stream.writeStream.foreachBatch(
                lambda df, bid: _persist_and_recompute_trends(
                    df,
                    bid,
                    live_path,
                    output_path,
                    baseline_df,
                    min_viewers,
                    top_n,
                    spark,
                    late_minutes,
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .queryName("streambharat_q17_trending")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Q17: Live trending with Spark Structured Streaming"
    )
    parser.add_argument("--input", default="data/stream/plays")
    parser.add_argument("--bronze", default="data/delta/bronze/playback")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--baseline", default="data/delta/reference/trending_baseline")
    parser.add_argument("--output", default="data/delta/gold/trending")
    parser.add_argument("--checkpoint", default="data/checkpoints/q17_trending")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--min-viewers", type=int, default=200)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-files-per-trigger", type=int, default=10)
    parser.add_argument("--late-minutes", type=int, default=15)
    args = parser.parse_args()
    run_q17(
        args.input,
        args.bronze,
        args.catalog,
        args.baseline,
        args.output,
        args.checkpoint,
        args.as_of,
        args.min_viewers,
        args.top_n,
        args.max_files_per_trigger,
        args.late_minutes,
    )


if __name__ == "__main__":
    main()
