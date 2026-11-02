"""Q4 — Incremental, checkpointed playback ingestion into a Bronze Delta table."""
from __future__ import annotations
import argparse, os
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from delta.tables import DeltaTable
from .spark_session import create_spark

PLAYBACK_SCHEMA_V1 = StructType([
    StructField("event_id", StringType(), True), StructField("user_id", StringType(), True),
    StructField("profile_id", StringType(), True), StructField("device_id", StringType(), True),
    StructField("content_id", StringType(), True), StructField("event_type", StringType(), True),
    StructField("position_sec", IntegerType(), True), StructField("client_ts", StringType(), True),
    StructField("server_ts", StringType(), True), StructField("ip_city", StringType(), True),
    StructField("device_type", StringType(), True), StructField("app_version", StringType(), True),
    StructField("download_id", StringType(), True),
])
PLAYBACK_SCHEMA_V2 = StructType(PLAYBACK_SCHEMA_V1.fields + [StructField("cast_device", StringType(), True)])

def ensure_cast_device_column(spark, target_path: str) -> None:
    """Add the nullable cast_device column to an existing Delta table."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        return
    names = {f.name for f in spark.read.format("delta").load(target_path).schema.fields}
    if "cast_device" not in names:
        escaped = target_path.replace("`", "``")
        spark.sql(f"ALTER TABLE delta.`{escaped}` ADD COLUMNS (cast_device STRING)")

def _merge_micro_batch(batch_df, batch_id: int, target_path: str, spark) -> None:
    """Deduplicate each batch and merge by event_id, with explicit schema checks."""
    if batch_df.isEmpty():
        return
    if "event_id" not in batch_df.columns:
        raise ValueError("Playback batch is missing required key event_id")
    if "_source_file" not in batch_df.columns:
        batch_df = batch_df.withColumn("_source_file", F.lit(None).cast("string"))
    if "_ingested_at" not in batch_df.columns:
        batch_df = batch_df.withColumn("_ingested_at", F.current_timestamp())
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("server_ts").desc_nulls_last(), F.col("_source_file").desc_nulls_last()
    )
    source = (batch_df.filter(F.col("event_id").isNotNull())
              .withColumn("_rn", F.row_number().over(ordering))
              .filter(F.col("_rn") == 1).drop("_rn"))
    if source.isEmpty():
        return
    if not DeltaTable.isDeltaTable(spark, target_path):
        source.write.format("delta").mode("overwrite").save(target_path)
        return
    ensure_cast_device_column(spark, target_path)
    target_schema = spark.read.format("delta").load(target_path).schema
    target_types = {f.name: f.dataType for f in target_schema.fields}
    unexpected = set(source.columns) - set(target_types)
    if unexpected:
        raise ValueError(
            "Source contains columns absent from the target Delta schema: "
            f"{sorted(unexpected)}. Apply the documented additive schema migration "
            "before resuming this checkpoint."
        )
    for name, dtype in target_types.items():
        if name not in source.columns:
            source = source.withColumn(name, F.lit(None).cast(dtype))
    source = source.select(*target_types.keys())
    DeltaTable.forPath(spark, target_path).alias("t").merge(
        source.alias("s"), "t.event_id = s.event_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def run_q4(input_path: str, target_path: str, checkpoint_path: str,
           max_files_per_trigger: int = 5, available_now: bool = True) -> None:
    if not os.path.isdir(input_path):
        raise FileNotFoundError(f"Playback input directory not found: {input_path}")
    for p in (os.path.dirname(os.path.abspath(target_path)),
              os.path.dirname(os.path.abspath(checkpoint_path))):
        os.makedirs(p, exist_ok=True)
    spark = create_spark("StreamBharat-Q4-Incremental-Playback-Ingestion")
    try:
        ensure_cast_device_column(spark, target_path)
        stream = (spark.readStream.schema(PLAYBACK_SCHEMA_V2)
                  .option("maxFilesPerTrigger", max_files_per_trigger)
                  .option("mode", "PERMISSIVE").json(input_path)
                  .withColumn("_source_file", F.input_file_name())
                  .withColumn("_ingested_at", F.current_timestamp()))
        writer = (stream.writeStream
                  .foreachBatch(lambda df, bid: _merge_micro_batch(df, bid, target_path, spark))
                  .option("checkpointLocation", checkpoint_path)
                  .queryName("streambharat_q4_playback_bronze"))
        query = writer.trigger(availableNow=True).start() if available_now else writer.trigger(
            processingTime="10 seconds").start()
        query.awaitTermination()
    finally:
        spark.stop()

def main() -> None:
    p = argparse.ArgumentParser(description="Q4: Incremental playback JSONL -> Bronze Delta")
    p.add_argument("--input", default="data/raw/player")
    p.add_argument("--target", default="data/delta/bronze/playback")
    p.add_argument("--checkpoint", default="data/checkpoints/q4_playback")
    p.add_argument("--max-files-per-trigger", type=int, default=5)
    p.add_argument("--continuous", action="store_true")
    a = p.parse_args()
    run_q4(a.input, a.target, a.checkpoint, a.max_files_per_trigger, not a.continuous)

if __name__ == "__main__":
    main()
