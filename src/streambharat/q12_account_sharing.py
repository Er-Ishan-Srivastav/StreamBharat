"""Q12 — Detect excessive concurrent screens and simultaneous multi-city account use."""
from __future__ import annotations
import argparse, math
from pyspark.sql import functions as F
from pyspark.sql import Window
from .spark_session import create_spark


def run_q12(sessions_path="data/delta/gold/viewing_sessions", periods_path="data/delta/gold/subscription_periods",
            plans_path="data/raw/reference/plans.csv", output_path="data/delta/gold/account_sharing_flags"):
    spark=create_spark("StreamBharat-Q12-Account-Sharing")
    try:
        sessions=spark.read.format("delta").load(sessions_path).filter(~F.coalesce("is_offline",F.lit(False)))
        sessions=(sessions.withColumn("slot_begin",F.to_timestamp(F.from_unixtime(F.floor(F.unix_timestamp("session_start")/300)*300)))
            # Subtract one second so an end exactly on a five-minute boundary does
            # not create a zero-duration extra concurrency slot.
            .withColumn("slot_end",F.to_timestamp(F.from_unixtime(F.floor((F.unix_timestamp("session_end")-1)/300)*300)))
            .withColumn("slot_start",F.explode(F.sequence("slot_begin","slot_end",F.expr("INTERVAL 5 MINUTES"))))
            .withColumn("slot_date",F.to_date("slot_start")))
        concurrency=(sessions.groupBy("user_id","slot_start","slot_date")
            .agg(F.countDistinct("device_id").alias("concurrent_devices"),
                 F.countDistinct("city").alias("concurrent_cities")))
        monthly=(sessions.withColumn("month",F.date_format("session_start","yyyy-MM"))
            .groupBy("user_id","month").agg(F.countDistinct("device_id").alias("devices_in_month"),
                F.countDistinct("city").alias("cities_in_month")))
        peak=(concurrency.groupBy("user_id",F.date_format("slot_date","yyyy-MM").alias("month"))
            .agg(F.max("concurrent_devices").alias("peak_concurrent_devices"),
                 F.countDistinct(F.when(F.col("concurrent_devices")>1,F.col("slot_date"))).alias("multi_screen_days"),
                 F.countDistinct(F.when(F.col("concurrent_cities")>1,F.col("slot_start"))).alias("multi_city_slots")))
        # A city-pair is a proxy only: the source provides no coordinates to compute distance.
        limits=spark.read.option("header",True).option("inferSchema",True).csv(plans_path).select(
            F.col("plan"),F.col("max_screens").cast("int").alias("screen_limit"))
        limit_rows=limits.alias("lim")
        subscribers=spark.read.format("delta").load("data/delta/gold/subscribers_daily")
        paid_daily=subscribers.filter(F.col("status").isin("paid","grace"))
        # Compare each concurrency day against the entitlement valid on that day.
        concurrency_days=(concurrency.groupBy("user_id",F.date_format("slot_date","yyyy-MM").alias("month"),"slot_date")
            .agg(F.max("concurrent_devices").alias("max_devices_day")))
        cd=concurrency_days.alias("cd"); pd=paid_daily.alias("pd")
        over_days=(cd.join(pd,
                    (F.col("cd.user_id")==F.col("pd.user_id"))&
                    (F.col("cd.slot_date")==F.col("pd.snapshot_date")),"inner")
            .join(limit_rows,F.col("pd.plan")==F.col("lim.plan"))
            .filter(F.col("cd.max_devices_day")>F.col("lim.screen_limit"))
            .groupBy(F.col("cd.user_id").alias("user_id"),F.col("cd.month").alias("month"))
            .agg(F.countDistinct(F.col("cd.slot_date")).alias("days_over_limit")))
        # Monthly plan/revenue annotation uses the last paid/grace day in that month.
        state=(paid_daily.withColumn("month",F.date_format("snapshot_date","yyyy-MM"))
            .withColumn("_rn",F.row_number().over(Window.partitionBy("user_id","month").orderBy(F.col("snapshot_date").desc())))
            .filter(F.col("_rn")==1).select("user_id","month","plan","monthly_price_inr"))

        flags=(peak.join(monthly,["user_id","month"],"full").join(state,["user_id","month"],"left")
            .join(over_days,["user_id","month"],"left").join(limits,"plan","left")
            .fillna({"screen_limit":1,"peak_concurrent_devices":0,"multi_screen_days":0,"multi_city_slots":0,"days_over_limit":0})
            .withColumn("exceeds_screen_limit",F.col("peak_concurrent_devices")>F.col("screen_limit"))
            .withColumn("sharing_flag",(F.col("days_over_limit")>=5)|((F.col("multi_city_slots")>=5)))
            .withColumn("estimated_households",F.greatest(F.ceil(F.col("peak_concurrent_devices")/F.col("screen_limit")),F.lit(1)))
            .withColumn("additional_households",F.greatest(F.col("estimated_households")-1,F.lit(0)))
            .withColumn("potential_monthly_revenue_inr",F.col("additional_households")*F.coalesce(F.col("monthly_price_inr"),F.lit(149.0))))
        flags.write.format("delta").mode("overwrite").save(output_path)
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--sessions",default="data/delta/gold/viewing_sessions"); a=p.parse_args(); run_q12(sessions_path=a.sessions)
