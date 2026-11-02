"""Q17 — Real-time trending titles from the supplied file-based stream simulator."""
from __future__ import annotations
import argparse, json, os
from datetime import date, timedelta
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from delta.tables import DeltaTable
from .spark_session import create_spark

PLAYS_SCHEMA = StructType([
    StructField("event_id", StringType(), True), StructField("user_id", StringType(), True),
    StructField("profile_id", StringType(), True), StructField("content_id", StringType(), True),
    StructField("language", StringType(), True), StructField("event_ts", StringType(), True),
])

def build_historical_baseline(spark, bronze_path: str, catalog_path: str, output_path: str, as_of: str) -> None:
    """Build per-title/language/hour average unique-viewer baseline for seven complete dates."""
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"Catalogue snapshot not found: {catalog_path}")
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        raise FileNotFoundError(f"Bronze playback Delta not found at {bronze_path}. Run Q4 first.")
    with open(catalog_path, "r", encoding="utf-8") as handle:
        catalog = json.load(handle)
    rows = [(r.get("content_id"), (r.get("languages") or ["Unknown"])[0])
            for r in catalog if r.get("type") in ("movie", "series") and r.get("content_id")]
    catalog_df = spark.createDataFrame(rows, ["content_id", "language"])
    as_of_date = F.to_date(F.lit(as_of))
    historical = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("event_type") == "start")
        .filter(F.col("download_id").isNull() | (F.trim(F.col("download_id")) == ""))
        .withColumn("event_ts", F.to_timestamp("server_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .filter((F.col("event_date") >= F.date_sub(as_of_date, 7)) & (F.col("event_date") < as_of_date))
        .join(F.broadcast(catalog_df), "content_id", "inner")
        .withColumn("hour_of_day", F.hour("event_ts"))
        .groupBy("content_id", "language", "hour_of_day", "event_date")
        .agg(F.countDistinct("profile_id").alias("hourly_unique_viewers"))
    )
    baseline = historical.groupBy("content_id", "language", "hour_of_day").agg(
        (F.avg("hourly_unique_viewers") / F.lit(6.0)).alias("baseline_avg_10m_viewers")
    )
    baseline.write.format("delta").mode("overwrite").save(output_path)

def _merge_trends(batch_df, batch_id: int, target_path: str, baseline_df, min_viewers: int, top_n: int, spark) -> None:
    """Replace each recomputed window/language snapshot, even when no row qualifies."""
    if batch_df.isEmpty():
        return
    active_keys = batch_df.select("window", "language").distinct()
    enriched = (
        batch_df.withColumn("hour_of_day", F.hour("window.start"))
        .join(F.broadcast(baseline_df), ["content_id", "language", "hour_of_day"], "left")
        .withColumn("baseline_avg_10m_viewers", F.coalesce("baseline_avg_10m_viewers", F.lit(0.0)))
        .withColumn("growth_ratio", F.col("unique_viewers") /
                    F.greatest(F.col("baseline_avg_10m_viewers"), F.lit(1.0)))
        .filter(F.col("unique_viewers") >= F.lit(min_viewers))
    )
    ordering = Window.partitionBy("window.start", "window.end", "language").orderBy(
        F.col("growth_ratio").desc(), F.col("unique_viewers").desc(), F.col("content_id")
    )
    ranked = (enriched.withColumn("rank_in_language", F.row_number().over(ordering))
              .filter(F.col("rank_in_language") <= top_n)
              .withColumn("processed_at", F.current_timestamp()))
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        target.alias("t").merge(
            active_keys.alias("k"),
            "t.window.start = k.window.start AND t.window.end = k.window.end AND t.language = k.language"
        ).whenMatchedDelete().execute()
        if not ranked.isEmpty():
            ranked.write.format("delta").mode("append").save(target_path)
    elif not ranked.isEmpty():
        ranked.write.format("delta").mode("overwrite").save(target_path)

def _persist_and_recompute_trends(incoming_df, batch_id: int, live_events_path: str, output_path: str,
                                  baseline_df, min_viewers: int, top_n: int, spark, late_minutes: int = 15) -> None:
    """Persist unique events and recompute exact distinct viewers over the active event-time horizon."""
    if incoming_df.isEmpty():
        return
    incoming = (incoming_df.filter(F.col("event_id").isNotNull())
                .withColumn("event_ts", F.to_timestamp("event_ts"))
                .filter(F.col("event_ts").isNotNull()).dropDuplicates(["event_id"]))
    if incoming.isEmpty():
        return
    if not DeltaTable.isDeltaTable(spark, live_events_path):
        incoming.write.format("delta").mode("overwrite").save(live_events_path)
    else:
        DeltaTable.forPath(spark, live_events_path).alias("t").merge(
            incoming.alias("s"), "t.event_id = s.event_id"
        ).whenNotMatchedInsertAll().execute()
    events = spark.read.format("delta").load(live_events_path)
    max_ts = events.agg(F.max("event_ts").alias("max_ts")).first()["max_ts"]
    if max_ts is None:
        return
    lower_bound = max_ts - timedelta(minutes=10 + late_minutes)
    recent = events.filter(F.col("event_ts") >= F.lit(lower_bound))
    windows = (recent.groupBy(F.window("event_ts", "10 minutes", "1 minute").alias("window"),
                              "language", "content_id")
               .agg(F.countDistinct("profile_id").alias("unique_viewers")))
    _merge_trends(windows, batch_id, output_path, baseline_df, min_viewers, top_n, spark)
    DeltaTable.forPath(spark, live_events_path).delete(F.col("event_ts") < F.lit(lower_bound))

def run_q17(input_path: str, bronze_path: str, catalog_path: str, baseline_path: str, output_path: str,
            checkpoint_path: str, as_of: str, min_viewers: int = 200, top_n: int = 20,
            max_files_per_trigger: int = 10, late_minutes: int = 15) -> None:
    if not os.path.isdir(input_path):
        raise FileNotFoundError(f"Stream folder not found: {input_path}. Start the plays simulator first.")
    if min_viewers < 1 or top_n < 1 or late_minutes < 0:
        raise ValueError("min_viewers and top_n must be positive; late_minutes cannot be negative")
    spark = create_spark("StreamBharat-Q17-Real-Time-Trending")
    try:
        build_historical_baseline(spark, bronze_path, catalog_path, baseline_path, as_of)
        baseline_df = spark.read.format("delta").load(baseline_path)
        stream = (spark.readStream.schema(PLAYS_SCHEMA).option("maxFilesPerTrigger", max_files_per_trigger)
                  .json(input_path))
        for directory in (os.path.dirname(os.path.abspath(checkpoint_path)),
                          os.path.dirname(os.path.abspath(output_path))):
            os.makedirs(directory, exist_ok=True)
        live_path = os.path.join(os.path.dirname(output_path), "_live_play_events")
        query = (stream.writeStream.foreachBatch(
            lambda df, bid: _persist_and_recompute_trends(df, bid, live_path, output_path,
                baseline_df, min_viewers, top_n, spark, late_minutes))
            .option("checkpointLocation", checkpoint_path).queryName("streambharat_q17_trending")
            .trigger(processingTime="5 seconds").start())
        query.awaitTermination()
    finally:
        spark.stop()

def main() -> None:
    parser = argparse.ArgumentParser(description="Q17: Live trending with Spark Structured Streaming")
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
    a = parser.parse_args()
    run_q17(a.input, a.bronze, a.catalog, a.baseline, a.output, a.checkpoint, a.as_of,
            a.min_viewers, a.top_n, a.max_files_per_trigger, a.late_minutes)

if __name__ == "__main__":
    main()
