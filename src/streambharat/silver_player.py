"""Silver player events: schema evolution, device clock correction and event deduplication."""
from __future__ import annotations

import argparse
import os
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType
)
from delta.tables import DeltaTable

from .spark_session import create_spark

PLAYER_V1 = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("profile_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("position_sec", LongType(), True),
    StructField("client_ts", StringType(), True),
    StructField("server_ts", StringType(), True),
    StructField("ip_city", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("download_id", StringType(), True),
    StructField("position_ms", LongType(), True),
    StructField("network", StringType(), True),
    StructField("schema_version", StringType(), True),
])

def transform_player_events(bronze_df):
    parsed = bronze_df.withColumn("_json", F.from_json("raw_value", PLAYER_V1))
    expanded = parsed.select(
        "source_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "ingested_at", "_json.*"
    )

    # v2 uses position_ms. Convert both versions to the canonical position_sec field.
    expanded = expanded.withColumn(
        "position_sec",
        F.when(F.col("position_ms").isNotNull(), F.col("position_ms") / F.lit(1000.0))
         .otherwise(F.col("position_sec").cast("double"))
    ).withColumn("client_time", F.to_timestamp("client_ts")) \
     .withColumn("server_time", F.to_timestamp("server_ts"))

    # Estimate clock offset only from online rows; use median to resist outliers.
    offset = (
        expanded.filter(F.col("download_id").isNull() | (F.trim("download_id") == ""))
        .filter(F.col("device_id").isNotNull() & F.col("client_time").isNotNull() & F.col("server_time").isNotNull())
        .withColumn("offset_seconds", F.unix_timestamp("server_time") - F.unix_timestamp("client_time"))
        .groupBy("device_id")
        .agg(F.expr("percentile_approx(offset_seconds, 0.5)").alias("median_clock_offset_seconds"))
    )
    conformed = expanded.join(offset, "device_id", "left").withColumn(
        "event_ts_corrected",
        F.when(
            F.col("download_id").isNotNull() & (F.trim("download_id") != ""),
            F.col("client_time")
        ).otherwise(
            F.to_timestamp(F.from_unixtime(
                F.unix_timestamp("client_time") + F.coalesce("median_clock_offset_seconds", F.lit(0))
            ))
        )
    )
    return conformed, offset


def merge_silver(spark, source_df, target_path: str):
    valid = source_df.filter(F.col("event_id").isNotNull())
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("server_time").desc_nulls_last(),
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last()
    )
    latest = valid.withColumn("_rn", F.row_number().over(ordering)).filter("_rn = 1").drop("_rn")
    if not DeltaTable.isDeltaTable(spark, target_path):
        latest.write.format("delta").mode("overwrite").save(target_path)
    else:
        DeltaTable.forPath(spark, target_path).alias("t").merge(
            latest.alias("s"), "t.event_id = s.event_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def run_silver_player(bronze_path="lake/bronze/player_events",
                      silver_path="lake/silver/player_events",
                      offsets_path="lake/silver/device_clock_offsets",
                      quarantine_path="lake/bronze/dlq/player_events"):
    spark = create_spark("StreamBharat-Silver-Player")
    try:
        bronze = spark.read.format("delta").load(bronze_path)
        good, offsets = transform_player_events(bronze)
        malformed = good.filter(
            F.col("event_id").isNull() | F.col("event_ts_corrected").isNull()
        )
        if not malformed.isEmpty():
            (malformed.withColumn("reason_code", F.lit("MISSING_KEY_OR_INVALID_TIMESTAMP"))
             .write.format("delta").mode("append").save(quarantine_path))
        merge_silver(spark, good.filter(
            F.col("event_id").isNotNull() & F.col("event_ts_corrected").isNotNull()
        ), silver_path)
        offsets.write.format("delta").mode("overwrite").save(offsets_path)
    finally:
        spark.stop()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--bronze",default="lake/bronze/player_events")
    p.add_argument("--silver",default="lake/silver/player_events")
    p.add_argument("--offsets",default="lake/silver/device_clock_offsets")
    p.add_argument("--quarantine",default="lake/bronze/dlq/player_events")
    a=p.parse_args()
    run_silver_player(a.bronze,a.silver,a.offsets,a.quarantine)

if __name__=="__main__":
    main()
