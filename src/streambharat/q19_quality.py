"""Q19 — Kids-safety audit, business quality gates and published-metric freeze checks."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q19(playback_path="data/delta/silver/playback_clean", content_path="data/delta/gold/dim_content_scd2",
            profiles_path="data/raw/app/profiles.json", watch_path="data/delta/gold/watch_time_daily",
            royalties_path="data/delta/gold/partner_royalties_quarterly",
            published_path="data/raw/reference/published_metrics.csv", recomputed_path="data/delta/quality/q9_recomputed_metrics",
            output_root="data/delta/quality"):
    spark=create_spark("StreamBharat-Q19-Data-Quality-and-Kids-Safety")
    try:
        playback=spark.read.format("delta").load(playback_path)
        profiles=spark.read.json(profiles_path).select("profile_id","is_kids")
        scd=spark.read.format("delta").load(content_path).select("content_id","age_rating","valid_from","valid_to")
        plays=(playback.filter(F.col("event_type")=="start").withColumn("event_ts",F.to_timestamp("event_ts_corrected"))
            .withColumn("event_date",F.to_date("event_ts")))
        unsafe=(plays.join(profiles,"profile_id").filter(F.col("is_kids") == True)
            .join(scd,(plays.content_id==scd.content_id)&(plays.event_date>=scd.valid_from)&
                  (scd.valid_to.isNull()|(plays.event_date<scd.valid_to)),"left")
            .filter(F.col("age_rating").isin("16+","18+")))
        (unsafe.groupBy("app_version","age_rating","content_id").agg(F.countDistinct("event_id").alias("violating_plays"))
            .write.format("delta").mode("overwrite").save(output_root+"/kids_safety_violations"))
        watch=spark.read.format("delta").load(watch_path)
        daily=watch.groupBy("profile_id","watch_date").agg(F.sum("watch_minutes").alias("minutes"))
        watch_violations=daily.filter(F.col("minutes")>1440).withColumn("check_name",F.lit("max_24h_watch"))
        known= scd.select("content_id").distinct()
        unknown=(plays.join(known,"content_id","left_anti").withColumn("check_name",F.lit("unknown_content_id")))
        roy=spark.read.format("delta").load(royalties_path)
        reconciliation=roy.groupBy("quarter").agg(F.sum("total_payable_inr").alias("total_payable_inr"),
            F.sum("premium_royalty_inr").alias("premium_pool_allocated_inr"),F.sum("free_ad_revenue_inr").alias("free_ad_share_inr"),
            F.sum("guarantee_top_up_inr").alias("guarantee_top_up_inr"))
        recon=(reconciliation.withColumn("expected_total_inr",F.col("premium_pool_allocated_inr")+
            F.col("free_ad_share_inr")+F.col("guarantee_top_up_inr"))
            .withColumn("passed",F.abs(F.col("total_payable_inr")-F.col("expected_total_inr"))<=0.01))
        recon.write.format("delta").mode("overwrite").save(output_root+"/royalty_reconciliation")
        watch_violations.write.format("delta").mode("overwrite").save(output_root+"/watch_time_violations")
        (unknown.select("event_id","user_id","profile_id","content_id","event_ts","check_name")
            .write.format("delta").mode("overwrite").save(output_root+"/unknown_content_plays"))
        published=spark.read.option("header",True).option("inferSchema",True).csv(published_path)
        # Recalculated metrics are supplied by Q9 comparison; compare all matching values
        # and retain an append-only audit snapshot rather than overwriting published numbers.
        recomputed=spark.read.format("delta").load(recomputed_path) if __import__("os").path.exists(recomputed_path) else None
        drift_alert_count=0
        metric_freeze_status="NOT_RUN"
        if recomputed is not None:
            drift=(published.join(recomputed,["month","metric"],"inner")
                .withColumn("relative_drift",F.abs(F.col("recalculated_value")-F.col("value"))/F.greatest(F.abs("value"),F.lit(1.0)))
                .withColumn("drift_alert",F.col("relative_drift")>0.005)
                .withColumn("checked_at",F.current_timestamp()))
            drift_alert_count=drift.filter(F.col("drift_alert")).count()
            metric_freeze_status="FAIL" if drift_alert_count else "PASS"
            drift.write.format("delta").mode("append").save(output_root+"/published_metric_audit")
        checks=[
            ("kids_safety",unsafe.count(),"FAIL" if unsafe.limit(1).count() else "PASS"),
            ("watch_time_under_24h",watch_violations.count(),"FAIL" if watch_violations.limit(1).count() else "PASS"),
            ("known_content_ids",unknown.count(),"FAIL" if unknown.limit(1).count() else "PASS"),
            ("royalty_reconciliation",recon.filter(~F.col("passed")).count(),"FAIL" if recon.filter(~F.col("passed")).limit(1).count() else "PASS"),
            ("published_metric_freeze",drift_alert_count,metric_freeze_status)]
        result=spark.createDataFrame(checks,["check_name","exception_count","status"]).withColumn("checked_at",F.current_timestamp())
        result.write.format("delta").mode("overwrite").save(output_root+"/quality_gate_results")
        if any(row[2]=="FAIL" for row in checks): raise RuntimeError("Q19 quality gate failed; see data/delta/quality/quality_gate_results")
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--playback",default="data/delta/silver/playback_clean"); a=p.parse_args(); run_q19(playback_path=a.playback)
