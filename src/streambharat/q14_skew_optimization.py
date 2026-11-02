"""Q14 — Diagnose skew, compare AQE/salting, optimize event storage and price CDN usage."""
from __future__ import annotations
import argparse, time, os
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from .spark_session import create_spark


def run_q14(playback_path="data/delta/silver/playback_clean", cdn_path="data/delta/silver/cdn_usage_hourly",
            content_path="data/delta/silver/catalog/dim_content", rates_path="data/raw/reference/cdn_rates.csv",
            report_path="artifacts/reports/q14_performance", costs_path="data/delta/gold/cdn_cost_by_title",
            hot_content_id="CNT00981", hot_date="2026-09-27", salt_buckets=16):
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be at least 2")
    spark=create_spark("StreamBharat-Q14-Skew-Optimization")
    try:
        events=spark.read.format("delta").load(playback_path).withColumn("event_date",F.to_date("event_ts_corrected"))
        skew=(events.filter(F.col("event_date")==hot_date).groupBy("content_id").count()
              .withColumn("share_pct",F.col("count")/F.sum("count").over(Window.partitionBy())*100))
        skew.orderBy(F.desc("count")).write.mode("overwrite").option("header",True).csv(report_path+"/hot_key_counts")
        content=spark.read.format("delta").load(content_path).select("content_id","title","partner_id")
        detected=skew.orderBy(F.desc("count")).select("content_id").first()
        if detected and detected["content_id"]:
            hot_content_id=detected["content_id"]
        # Baseline join and AQE join timings, measured with count actions and logged Spark config.
        measurements=[]
        for name,aqe in [("baseline",False),("aqe",True)]:
            spark.conf.set("spark.sql.adaptive.enabled",str(aqe).lower())
            t0=time.perf_counter(); count=events.join(content,"content_id","left").count(); elapsed=time.perf_counter()-t0
            measurements.append((name,aqe,None,count,elapsed))
        # Salt only the hot content key on the large side; replicate its dimension row
        # across every bucket so the join remains semantically complete.
        spark.conf.set("spark.sql.adaptive.enabled","true")
        hot=events.withColumn("salt",F.when(F.col("content_id")==hot_content_id,
                     F.pmod(F.xxhash64("event_id"),F.lit(salt_buckets))).otherwise(F.lit(0)))
        dim_salted=(content.withColumn("salt_values",F.when(F.col("content_id")==hot_content_id,
                     F.sequence(F.lit(0),F.lit(salt_buckets-1))).otherwise(F.array(F.lit(0))))
                    .withColumn("salt",F.explode("salt_values")).drop("salt_values"))
        t0=time.perf_counter(); salted_count=hot.join(dim_salted,["content_id","salt"],"left").count(); elapsed=time.perf_counter()-t0
        measurements.append(("aqe_salted",True,salt_buckets,salted_count,elapsed))
        perf=spark.createDataFrame(measurements,["method","aqe_enabled","salt_buckets","joined_rows","elapsed_seconds"])
        perf.write.mode("overwrite").option("header",True).csv(report_path+"/join_benchmark")
        # Physical layout by corrected event date/hour; Delta Z-order is optional but measured.
        layout=events.withColumn("event_hour",F.hour("event_ts_corrected"))
        layout_path="data/delta/performance/playback_partitioned"
        layout.write.format("delta").mode("overwrite").partitionBy("event_date","event_hour").save(layout_path)
        before=time.perf_counter(); spark.read.format("delta").load(layout_path).filter(
            (F.col("event_date")==hot_date)&(F.col("content_id")==hot_content_id)).count(); before_s=time.perf_counter()-before
        optimize_result="not_available"
        try:
            DeltaTable.forPath(spark,layout_path).optimize().executeZOrderBy("content_id")
            optimize_result="executed"
        except Exception as exc:
            optimize_result=f"unsupported_or_failed: {type(exc).__name__}: {exc}"
        after=time.perf_counter(); spark.read.format("delta").load(layout_path).filter(
            (F.col("event_date")==hot_date)&(F.col("content_id")==hot_content_id)).count(); after_s=time.perf_counter()-after
        (spark.createDataFrame([(before_s,after_s,optimize_result)],
            ["single_title_seconds_before_zorder","single_title_seconds_after_zorder","zorder_status"])
            .write.mode("overwrite").option("header",True).csv(report_path+"/layout_benchmark"))
        rates=spark.read.option("header",True).option("inferSchema",True).csv(rates_path)
        cdn=spark.read.format("delta").load(cdn_path)
        costs=(cdn.join(rates,["cdn","region"],"left")
            .withColumn("cost_inr",F.col("bytes")/F.lit(1024**3)*F.col("rate_inr_per_gb"))
            .groupBy("content_id","region").agg(F.sum("bytes").alias("bytes"),F.sum("cost_inr").alias("cdn_cost_inr")))
        costs.write.format("delta").mode("overwrite").save(costs_path)
        costs.filter(F.col("content_id")==hot_content_id).write.format("delta").mode("overwrite").save(costs_path+"_cricket_final")
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--hot-content-id",default="CNT00981"); a=p.parse_args(); run_q14(hot_content_id=a.hot_content_id)
