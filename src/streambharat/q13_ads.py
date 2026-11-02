"""Q13 — Ad fill, campaign delivery, frequency cap credits and genre targeting."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q13(breaks_path="data/raw/ads/ad_breaks.csv", impressions_path="data/raw/ads/ad_impressions.csv",
            campaigns_path="data/raw/ads/ad_campaigns.csv", content_genres_path="data/delta/silver/catalog/content_genre",
            daily_path="data/delta/gold/ad_delivery_daily", violations_path="data/delta/gold/frequency_violations",
            pacing_path="data/delta/gold/campaign_pacing"):
    spark=create_spark("StreamBharat-Q13-Advertising")
    try:
        breaks=(spark.read.option("header",True).option("inferSchema",True).csv(breaks_path)
                .withColumn("break_date",F.to_date("break_ts")))
        imps=(spark.read.option("header",True).option("inferSchema",True).csv(impressions_path)
              .withColumn("ad_date",F.to_date("ad_break_ts")))
        campaigns=(spark.read.option("header",True).option("inferSchema",True).csv(campaigns_path)
            .withColumn("campaign_days",F.datediff("end_date","start_date")+1)
            .withColumn("daily_budget_inr",F.col("budget_inr")/F.greatest("campaign_days",F.lit(1))))
        # The supplied ad_breaks file has no campaign_id on unfilled rows. Therefore
        # fill rate is reported at platform/day level and joined to each campaign/day;
        # it is not falsely presented as a campaign-specific fill rate.
        platform_fill=(breaks.groupBy("break_date").agg(F.count("break_id").alias("platform_ad_breaks"),
            F.sum(F.col("filled").cast("int")).alias("platform_filled_breaks"))
            .withColumn("platform_fill_rate",F.col("platform_filled_breaks")/
                        F.greatest("platform_ad_breaks",F.lit(1))))
        daily=(imps.groupBy("campaign_id","ad_date").agg(
            F.countDistinct("impression_id").alias("impressions"),
            F.avg(F.col("completed").cast("double")).alias("completion_rate"),
            (F.sum("cpm_inr")/F.lit(1000.0)).alias("gross_revenue_inr"))
            .join(platform_fill, F.col("ad_date")==F.col("break_date"), "left")
            .drop("break_date")
            .join(campaigns.select("campaign_id","daily_budget_inr"),"campaign_id","left")
            .withColumn("budget_pacing_pct",F.when(F.col("daily_budget_inr")>0,
                F.col("gross_revenue_inr")/F.col("daily_budget_inr")*100)))
        daily.write.format("delta").mode("overwrite").save(daily_path)
        # Per-user excess impressions are credited at campaign CPM, one CPM/1000
        # for each excess impression; all calculations are rounded to paise.
        cap=(imps.groupBy("user_id","campaign_id","ad_date").agg(
            F.countDistinct("impression_id").alias("user_campaign_impressions"))
            .join(campaigns.select("campaign_id","daily_frequency_cap","cpm_inr"),"campaign_id")
            .withColumn("excess_impressions",F.greatest(F.col("user_campaign_impressions")-
                F.col("daily_frequency_cap"),F.lit(0)))
            .withColumn("credit_back_inr",F.round(F.col("excess_impressions")*F.col("cpm_inr")/1000,2))
            .filter(F.col("excess_impressions")>0))
        cap.write.format("delta").mode("overwrite").save(violations_path)
        # Title-level net revenue is retained for partner royalties.
        title_revenue=(imps.groupBy(F.date_trunc("month","ad_date").alias("month"),"ad_date","content_id")
            .agg((F.sum("cpm_inr")/F.lit(1000.0)).alias("ad_revenue_inr"),
                 F.countDistinct("impression_id").alias("impressions")))
        title_revenue.write.format("delta").mode("overwrite").save("data/delta/gold/ad_revenue_by_title_day")
        genres=spark.read.format("delta").load(content_genres_path)
        targeted=(imps.join(campaigns.select("campaign_id","target_genres"),"campaign_id")
            .join(genres,"content_id","left")
            .withColumn("target_list",F.split(F.col("target_genres"),"\\|"))
            .withColumn("target_mismatch",(F.upper(F.col("target_genres"))!="ALL")&
                        ~F.array_contains(F.transform(F.col("target_list"),lambda x:F.lower(x)),F.lower(F.col("genre")))))
        targeted.filter("target_mismatch").write.format("delta").mode("overwrite").save("data/delta/quality/ad_targeting_violations")
        pacing=(daily.groupBy("campaign_id").agg(F.sum("gross_revenue_inr").alias("total_spend_inr"),
            F.avg("budget_pacing_pct").alias("avg_daily_pacing_pct")))
        pacing.write.format("delta").mode("overwrite").save(pacing_path)
    finally:
        spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--breaks",default="data/raw/ads/ad_breaks.csv"); a=p.parse_args(); run_q13(breaks_path=a.breaks)
