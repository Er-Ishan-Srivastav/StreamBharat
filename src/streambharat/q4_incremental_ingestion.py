"""Q4 — Incremental, checkpointed playback ingestion into a Bronze Delta table."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)
from delta.tables import DeltaTable

from .spark_session import create_spark


# V2 is backward-compatible with the supplied JSONL files: cast_device is nullable.
# It also accepts the additive field when newer player versions begin sending it.
PLAYBACK_SCHEMA_V1 = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("profile_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("position_sec", IntegerType(), True),
    StructField("client_ts", StringType(), True),
    StructField("server_ts", StringType(), True),
    StructField("ip_city", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("download_id", StringType(), True),
])
PLAYBACK_SCHEMA_V2 = StructType(PLAYBACK_SCHEMA_V1.fields + [
    StructField("cast_device", StringType(), True),
])


def ensure_cast_device_column(spark, target_path: str) -> None:
    """Apply the additive Delta schema migration when upgrading an existing Bronze table."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        return
    existing = {field.name for field in spark.read.format("delta").load(target_path).schema.fields}
    if "cast_device" not in existing:
        escaped = target_path.replace("`", "``")
        spark.sql(f"ALTER TABLE delta.`{escaped}` ADD COLUMNS (cast_device STRING)")


def _merge_micro_batch(batch_df, batch_id: int, target_path: str, spark) -> None:
    """Idempotent upsert; merge prevents replays from duplicating event_id values."""
    if batch_df.isEmpty():
        return

    # File sources may contain repeated delivery of an event in one micro-batch.
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("server_ts").desc_nulls_last(), F.col("_source_file").desc()
    )
    deduplicated = (
        batch_df.filter(F.col("event_id").isNotNull())
        .withColumn("_row_number", F.row_number().over(ordering))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        (deduplicated.write.format("delta").mode("overwrite").save(target_path))
        return

    ensure_cast_device_column(spark, target_path)
    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("target")
        .merge(deduplicated.alias("source"), "target.event_id = source.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_q4(input_path: str, target_path: str, checkpoint_path: str,
           max_files_per_trigger: int = 5, available_now: bool = True) -> None:
    """Ingest all unseen arrival-day files, or keep streaming when available_now=False."""
    if not os.path.isdir(input_path):
        raise FileNotFoundError(f"Playback input directory not found: {input_path}")
    os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(checkpoint_path)), exist_ok=True)

    spark = create_spark("StreamBharat-Q4-Incremental-Playback-Ingestion")
    ensure_cast_device_column(spark, target_path)

    stream = (
        spark.readStream
        .schema(PLAYBACK_SCHEMA_V2)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("mode", "PERMISSIVE")
        .json(input_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )

    writer = (
        stream.writeStream
        .foreachBatch(lambda df, bid: _merge_micro_batch(df, bid, target_path, spark))
        .option("checkpointLocation", checkpoint_path)
        .queryName("streambharat_q4_playback_bronze")
    )
    if available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime="10 seconds").start()
    query.awaitTermination()
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Q4: Incremental playback JSONL -> Bronze Delta")
    parser.add_argument("--input", default="data/raw/player")
    parser.add_argument("--target", default="data/delta/bronze/playback")
    parser.add_argument("--checkpoint", default="data/checkpoints/q4_playback")
    parser.add_argument("--max-files-per-trigger", type=int, default=5)
    parser.add_argument("--continuous", action="store_true", help="Keep the stream running")
    args = parser.parse_args()
    run_q4(args.input, args.target, args.checkpoint, args.max_files_per_trigger,
           available_now=not args.continuous)


if __name__ == "__main__":
    main()
