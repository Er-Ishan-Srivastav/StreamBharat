"""Q5 — Sessionise playback and calculate watch time without counting seeks/pauses."""
from __future__ import annotations
import argparse, os
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q5(playback_path="data/delta/silver/playback_clean", content_path="data/delta/silver/catalog/dim_content",
           sessions_path="data/delta/gold/viewing_sessions", daily_path="data/delta/gold/watch_time_daily"):
    spark=create_spark("StreamBharat-Q5-Viewing-Sessions")
    try:
        events=spark.read.format("delta").load(playback_path)
        content=spark.read.format("delta").load(content_path).select("content_id","duration_sec","parent_series_id")
        # Sessions are per device; a gap over 30 minutes starts a new session.
        w=Window.partitionBy("device_id").orderBy("event_ts_corrected","event_id")
        ordered=(events.withColumn("event_ts",F.to_timestamp("event_ts_corrected"))
          .withColumn("prev_ts",F.lag("event_ts").over(w))
          .withColumn("prev_type",F.lag("event_type").over(w))
          .withColumn("prev_position",F.lag("position_sec").over(w))
          .withColumn("gap_seconds",F.col("event_ts").cast("long")-F.col("prev_ts").cast("long"))
          .withColumn("new_session",F.when(F.col("prev_ts").isNull()|(F.col("gap_seconds")>1800),1).otherwise(0))
          .withColumn("session_seq",F.sum("new_session").over(w.rowsBetween(Window.unboundedPreceding,Window.currentRow)))
          .withColumn("session_id",F.sha2(F.concat_ws("|","device_id",F.col("session_seq").cast("string")),256)))
        # Count only an observed heartbeat interval of <= 6 minutes. A seek/pause
        # breaks the interval, so skipped content and paused time are not credited.
        interval=(F.col("event_type")=="heartbeat")&F.col("prev_type").isin("start","heartbeat")&F.col("gap_seconds").between(1,360)
        ordered=ordered.withColumn("interval_minutes",F.when(interval,F.col("gap_seconds")/60.0).otherwise(F.lit(0.0)))
        # Ignore events after an inactivity gap of four hours; 30-minute sessionisation
        # is stricter but the explicit cap documents the inactivity termination rule.
        ordered=ordered.filter(F.col("gap_seconds").isNull()|(F.col("gap_seconds")<=14400))
        per_title=(ordered.groupBy("session_id","user_id","profile_id","device_id","content_id",
                  F.to_date("event_ts").alias("watch_date"))
          .agg(F.sum("interval_minutes").alias("raw_watch_minutes"),F.min("event_ts").alias("session_start"),
               F.max("event_ts").alias("session_end"),F.max("ip_city").alias("city"),
               F.max("device_type").alias("platform"),F.max("is_offline").alias("is_offline")))
        per_title=(per_title.join(content,"content_id","left")
          .withColumn("duration_minutes",F.col("duration_sec")/60.0)
          .withColumn("watch_minutes",F.least("raw_watch_minutes",F.col("duration_minutes")*F.lit(1.5)))
          .withColumn("completion",F.when(F.col("duration_minutes")>0,
                 F.least(F.col("watch_minutes")/F.col("duration_minutes"),F.lit(1.0))).otherwise(F.lit(None).cast("double"))))
        per_title.write.format("delta").mode("overwrite").save(sessions_path)
        daily=(per_title.groupBy("watch_date","user_id","profile_id","content_id","platform","city")
               .agg(F.sum("watch_minutes").alias("watch_minutes"),F.max("completion").alias("completion"),
                    F.countDistinct("session_id").alias("sessions"),F.max("is_offline").alias("is_offline")))
        daily.write.format("delta").mode("overwrite").save(daily_path)
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--playback",default="data/delta/silver/playback_clean"); p.add_argument("--content",default="data/delta/silver/catalog/dim_content"); a=p.parse_args(); run_q5(a.playback,a.content)
