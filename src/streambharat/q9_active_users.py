"""Q9 — Exact rolling DAU/WAU/MAU by plan, platform and state."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q9(watch_path="data/delta/gold/watch_time_daily", users_path="data/raw/app/users.json",
           subscribers_path="data/delta/gold/subscribers_daily", output_path="data/delta/gold/active_users_daily",
           published_path="data/raw/reference/published_metrics.csv"):
    spark=create_spark("StreamBharat-Q9-Active-Users")
    try:
        watch=spark.read.format("delta").load(watch_path).filter(F.col("watch_minutes")>=1)
        users=spark.read.json(users_path).select("user_id","state")
        subs=spark.read.format("delta").load(subscribers_path).select(
            F.col("user_id"),F.col("snapshot_date").alias("watch_date"),F.col("plan"),F.col("status"))
        active=(watch.join(users,"user_id","left").join(subs,["user_id","watch_date"],"left")
            .withColumn("plan",F.when(F.col("status").isin("paid","grace"),F.col("plan")).otherwise(F.lit("free")))
            .withColumn("platform",F.coalesce("platform",F.lit("unknown")))
            .select("user_id","watch_date","plan","platform","state").dropDuplicates())
        # Expand each active user-day into the next 30 reporting dates. This makes
        # exact rolling distinct counts possible without unsupported window countDistinct.
        expanded=(active.withColumn("report_date",F.explode(F.sequence(F.col("watch_date"),
                    F.date_add(F.col("watch_date"),29),F.expr("INTERVAL 1 DAY")))))
        grouped=(expanded.groupBy("report_date","plan","platform","state")
            .agg(F.countDistinct(F.when(F.datediff("report_date","watch_date")==0,F.col("user_id"))).alias("dau"),
                 F.countDistinct(F.when(F.datediff("report_date","watch_date")<=6,F.col("user_id"))).alias("wau"),
                 F.countDistinct("user_id").alias("mau"),
                 F.approx_count_distinct(F.when(F.datediff("report_date","watch_date")<=6,F.col("user_id"))).alias("wau_approx"),
                 F.approx_count_distinct("user_id").alias("mau_approx"))
            .withColumn("stickiness",F.when(F.col("mau")>0,F.col("dau")/F.col("mau")).otherwise(0.0)))
        grouped.write.format("delta").mode("overwrite").save(output_path)
        published=spark.read.option("header",True).option("inferSchema",True).csv(published_path)
        # Board MAU is the distinct audience in the calendar month, not the
        # maximum rolling-30-day number observed on any day in that month.
        dau_month=(active.withColumn("month",F.date_format("watch_date","yyyy-MM"))
            .groupBy("month","watch_date").agg(F.countDistinct("user_id").alias("dau"))
            .groupBy("month").agg(F.avg("dau").alias("avg_daily_active_users")))
        calendar_mau=(active.withColumn("month",F.date_format("watch_date","yyyy-MM"))
            .groupBy("month").agg(F.countDistinct("user_id").alias("monthly_active_users")))
        monthly=dau_month.join(calendar_mau,"month","inner")
        recomputed=(monthly.select("month",F.lit("avg_daily_active_users").alias("metric"),
                F.col("avg_daily_active_users").alias("recalculated_value"))
            .unionByName(monthly.select("month",F.lit("monthly_active_users").alias("metric"),
                F.col("monthly_active_users").alias("recalculated_value")))
            .withColumn("recalculated_at",F.current_timestamp()))
        check=(published.join(recomputed, ["month","metric"], "left")
            .withColumn("relative_delta",F.abs(F.col("recalculated_value")-F.col("value"))/
                F.greatest(F.abs(F.col("value")),F.lit(1.0)))
            .withColumn("drift_alert",F.col("relative_delta")>0.005))
        check.write.format("delta").mode("overwrite").save("data/delta/quality/q9_published_metric_comparison")
        recomputed.write.format("delta").mode("overwrite").save("data/delta/quality/q9_recomputed_metrics")
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--watch",default="data/delta/gold/watch_time_daily"); a=p.parse_args(); run_q9(watch_path=a.watch)
