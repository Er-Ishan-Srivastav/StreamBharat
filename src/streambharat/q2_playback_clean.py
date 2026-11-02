"""Q2 — Deduplicate playback, estimate device clock offsets and create Silver timestamps."""
from __future__ import annotations
import argparse, os
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from .spark_session import create_spark

PLAYBACK_SCHEMA = StructType([
    StructField("event_id",StringType()),StructField("user_id",StringType()),StructField("profile_id",StringType()),
    StructField("device_id",StringType()),StructField("content_id",StringType()),StructField("event_type",StringType()),
    StructField("position_sec",IntegerType()),StructField("client_ts",StringType()),StructField("server_ts",StringType()),
    StructField("ip_city",StringType()),StructField("device_type",StringType()),StructField("app_version",StringType()),
    StructField("download_id",StringType()),StructField("cast_device",StringType())])


def run_q2(input_path="data/raw/player", output_path="data/delta/silver/playback_clean",
           offset_path="data/delta/silver/device_clock_offsets", min_online_events=5,
           max_mad_seconds=300, max_offset_days=365):
    spark=create_spark("StreamBharat-Q2-Playback-Cleaning")
    try:
        raw=(spark.read.schema(PLAYBACK_SCHEMA).json(input_path)
             .withColumn("client_time",F.to_timestamp("client_ts"))
             .withColumn("server_time",F.to_timestamp("server_ts")))
        order=Window.partitionBy("event_id").orderBy(F.col("server_time").desc_nulls_last())
        clean=(raw.filter(F.col("event_id").isNotNull()).withColumn("_rn",F.row_number().over(order))
               .filter(F.col("_rn")==1).drop("_rn"))
        online=clean.filter((F.col("download_id").isNull())|(F.trim("download_id")==""))
        offsets=(online.filter(F.col("client_time").isNotNull()&F.col("server_time").isNotNull())
          .withColumn("offset_seconds",F.col("client_time").cast("long")-F.col("server_time").cast("long"))
          .groupBy("device_id").agg(F.count("offset_seconds").alias("online_samples"),
             F.expr("percentile_approx(offset_seconds, 0.5, 10000)").alias("median_offset_seconds"),
             F.expr("percentile_approx(abs(offset_seconds), 0.5, 10000)").alias("median_abs_offset_seconds")))
        # MAD is calculated around the device median; unlike a median absolute offset,
        # it measures consistency and still permits a stable multi-hour clock error.
        deviations=(online.withColumn("offset_seconds",F.col("client_time").cast("long")-F.col("server_time").cast("long"))
            .join(offsets.select("device_id","median_offset_seconds"),"device_id")
            .withColumn("abs_deviation",F.abs(F.col("offset_seconds")-F.col("median_offset_seconds")))
            .groupBy("device_id").agg(F.expr("percentile_approx(abs_deviation, 0.5, 10000)").alias("offset_mad_seconds")))
        offsets=(offsets.join(deviations,"device_id","left")
            .withColumn("offset_reliable",(F.col("online_samples")>=min_online_events)&
                 (F.col("offset_mad_seconds")<=max_mad_seconds)&
                 (F.abs(F.col("median_offset_seconds"))<=max_offset_days*86400)))
        enriched=(clean.join(offsets,"device_id","left")
            .withColumn("is_offline",F.col("download_id").isNotNull()&(F.trim("download_id")!=""))
            .withColumn("event_ts_corrected",F.when(F.col("offset_reliable")&F.col("client_time").isNotNull(),
                 F.expr("timestamp_seconds(unix_timestamp(client_time) - median_offset_seconds)"))
                .otherwise(F.col("server_time")))
            .withColumn("event_ts",F.col("event_ts_corrected"))
            .withColumn("event_date",F.to_date("event_ts_corrected"))
            .withColumn("clock_correction_applied",F.col("offset_reliable")&F.col("client_time").isNotNull()))
        enriched.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(output_path)
        offsets.write.format("delta").mode("overwrite").save(offset_path)
        before=(clean.filter(F.col("event_type")=="heartbeat").withColumn("day",F.to_date("server_time"))
                .groupBy("day").agg((F.count("*")*5/60).alias("estimated_server_day_watch_hours")))
        after=(enriched.filter(F.col("event_type")=="heartbeat").withColumn("day",F.to_date("event_ts_corrected"))
               .groupBy("day").agg((F.count("*")*5/60).alias("estimated_corrected_day_watch_hours")))
        impact=before.join(after,"day","full")
        impact.write.format("delta").mode("overwrite").save("data/delta/silver/q2_daily_clock_impact")
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--input",default="data/raw/player"); p.add_argument("--output",default="data/delta/silver/playback_clean")
    p.add_argument("--offset-output",default="data/delta/silver/device_clock_offsets"); p.add_argument("--min-online-events",type=int,default=5)
    a=p.parse_args(); run_q2(a.input,a.output,a.offset_output,a.min_online_events)
