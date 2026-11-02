"""Q15 — Partner royalty allocation, exact paise reconciliation and guarantees."""
from __future__ import annotations
import argparse
from decimal import Decimal, ROUND_DOWN
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def allocate_cents(total_inr: Decimal, weights: dict[str, Decimal]) -> dict[str, Decimal]:
    """Pure-Python largest-remainder allocation for exact currency unit tests."""
    total_cents=int((total_inr*100).quantize(Decimal("1")))
    positive={k:max(Decimal(0),v) for k,v in weights.items()}; denom=sum(positive.values())
    if not positive or denom==0: return {k:Decimal("0.00") for k in weights}
    raw={k:Decimal(total_cents)*v/denom for k,v in positive.items()}
    base={k:int(v.to_integral_value(rounding=ROUND_DOWN)) for k,v in raw.items()}
    remaining=total_cents-sum(base.values())
    for key in sorted(raw,key=lambda k:(raw[k]-base[k],k),reverse=True)[:remaining]: base[key]+=1
    return {k:(Decimal(v)/100).quantize(Decimal("0.01")) for k,v in base.items()}


def run_q15(watch_path="data/delta/gold/watch_time_daily", scd_path="data/delta/gold/dim_content_scd2",
            subscribers_path="data/delta/gold/subscribers_daily", ad_path="data/delta/gold/ad_revenue_by_title_day",
            partners_path="data/raw/reference/content_partners.csv", output_path="data/delta/gold/partner_royalties_quarterly"):
    spark=create_spark("StreamBharat-Q15-Partner-Royalties")
    try:
        watch0=spark.read.format("delta").load(watch_path).withColumn("month",F.date_trunc("month","watch_date"))
        scd=spark.read.format("delta").load(scd_path).select("content_id","partner_id","tier","valid_from","valid_to")
        watch=(watch0.alias("w").join(scd.alias("c"),
            (F.col("w.content_id")==F.col("c.content_id"))&(F.col("w.watch_date")>=F.col("c.valid_from"))&
            (F.col("c.valid_to").isNull()|(F.col("w.watch_date")<F.col("c.valid_to"))),"left")
            .select("w.*",F.col("c.partner_id").alias("partner_id_asof"),F.col("c.tier").alias("tier_asof")))
        premium=(watch.filter(F.col("tier_asof")=="premium").groupBy("month","partner_id_asof")
            .agg(F.sum("watch_minutes").alias("partner_premium_minutes")))
        total_minutes=premium.groupBy("month").agg(F.sum("partner_premium_minutes").alias("all_premium_minutes"))
        partners=spark.read.option("header",True).option("inferSchema",True).csv(partners_path)
        subscriber_daily=spark.read.format("delta").load(subscribers_path).filter(F.col("status")=="paid")
        user_month=(subscriber_daily.withColumn("month",F.date_trunc("month","snapshot_date"))
            .groupBy("month","user_id").agg(F.max("monthly_price_inr").alias("user_mrr")))
        sub=user_month.groupBy("month").agg(F.sum("user_mrr").alias("premium_subscription_revenue_inr"))
        base=(premium.join(total_minutes,"month").join(sub,"month").join(
                partners,F.col("partner_id_asof")==partners.partner_id,"left")
            .withColumn("minute_share",F.col("partner_premium_minutes")/F.greatest("all_premium_minutes",F.lit(1.0)))
            .withColumn("premium_royalty_unrounded",F.col("premium_subscription_revenue_inr")*
                        F.col("revenue_share_pct")/100*F.col("minute_share")))
        # The effective pool is the sum of each partner's contractual percentage
        # multiplied by its premium-minute allocation. Round the month only once;
        # distribute residual paise by largest fractional remainder.
        base=(base.withColumn("base_cents",F.floor(F.col("premium_royalty_unrounded")*100).cast("long"))
            .withColumn("fractional_cent",F.col("premium_royalty_unrounded")*100-F.col("base_cents")))
        monthly_order=Window.partitionBy("month").orderBy(F.col("fractional_cent").desc(),F.col("partner_id_asof"))
        monthly_total=Window.partitionBy("month")
        base=(base.withColumn("rounding_rank",F.row_number().over(monthly_order))
            .withColumn("target_pool_cents",F.round(F.sum("premium_royalty_unrounded").over(monthly_total)*100).cast("long"))
            .withColumn("base_pool_cents",F.sum("base_cents").over(monthly_total))
            .withColumn("residual_cents",F.col("target_pool_cents")-F.col("base_pool_cents"))
            .withColumn("premium_royalty_inr",(F.col("base_cents")+
                F.when(F.col("rounding_rank")<=F.col("residual_cents"),1).otherwise(0))/100.0))
        ads=spark.read.format("delta").load(ad_path)
        # Assign free-tier ad income to the partner owning the title at the viewing date.
        free=(watch.filter(F.col("tier_asof")=="free")
            .groupBy("month","partner_id_asof","content_id").agg(F.sum("watch_minutes").alias("free_watch_minutes")))
        title_ads=(ads.groupBy(F.date_trunc("month","ad_date").alias("month"),"content_id")
            .agg(F.sum("ad_revenue_inr").alias("title_ad_revenue_inr")))
        free_titles=(free.join(title_ads,["month","content_id"],"left")
            .join(partners,F.col("partner_id_asof")==partners.partner_id,"left")
            .withColumn("free_ad_share_inr",F.round(F.coalesce(F.col("title_ad_revenue_inr"),F.lit(0.0))*F.col("revenue_share_pct")/100,2)))
        free_partner=(free_titles.groupBy(F.col("month"),F.col("partner_id_asof").alias("partner_id"))
            .agg(F.sum("free_ad_share_inr").alias("free_ad_revenue_inr")))
        premium_partner=(base.groupBy("month",F.col("partner_id_asof").alias("partner_id"),"name","revenue_share_pct",
                    "minimum_guarantee_inr_per_quarter")
            .agg(F.sum("premium_royalty_inr").alias("premium_royalty_inr"),
                 F.max("premium_subscription_revenue_inr").alias("premium_subscription_revenue_inr"),
                 F.max("target_pool_cents").alias("premium_pool_cents")))
        free_partner_meta=(free_partner.join(partners.select("partner_id","name","revenue_share_pct",
                    "minimum_guarantee_inr_per_quarter"),"partner_id","left"))
        monthly=(premium_partner.alias("p").join(free_partner_meta.alias("f"),
                    (F.col("p.month")==F.col("f.month"))&(F.col("p.partner_id")==F.col("f.partner_id")),"full")
            .select(F.coalesce(F.col("p.month"),F.col("f.month")).alias("month"),
                F.coalesce(F.col("p.partner_id"),F.col("f.partner_id")).alias("partner_id"),
                F.coalesce(F.col("p.name"),F.col("f.name")).alias("name"),
                F.coalesce(F.col("p.revenue_share_pct"),F.col("f.revenue_share_pct")).alias("revenue_share_pct"),
                F.coalesce(F.col("p.minimum_guarantee_inr_per_quarter"),F.col("f.minimum_guarantee_inr_per_quarter")).alias("minimum_guarantee_inr_per_quarter"),
                F.coalesce(F.col("p.premium_royalty_inr"),F.lit(0.0)).alias("premium_royalty_inr"),
                F.coalesce(F.col("p.premium_subscription_revenue_inr"),F.lit(0.0)).alias("premium_subscription_revenue_inr"),
                F.coalesce(F.col("p.premium_pool_cents"),F.lit(0)).alias("premium_pool_cents"),
                F.coalesce(F.col("f.free_ad_revenue_inr"),F.lit(0.0)).alias("free_ad_revenue_inr"))
            .withColumn("earned_royalty_inr",F.col("premium_royalty_inr")+F.col("free_ad_revenue_inr")))
        quarterly=(monthly.withColumn("quarter",F.concat(F.year("month"),F.lit("-Q"),F.quarter("month")))
            .groupBy("quarter","partner_id","name","minimum_guarantee_inr_per_quarter")
            .agg(F.sum("premium_royalty_inr").alias("premium_royalty_inr"),
                 F.sum("free_ad_revenue_inr").alias("free_ad_revenue_inr"),
                 F.sum("earned_royalty_inr").alias("earned_royalty_inr"))
            .withColumn("guarantee_top_up_inr",F.greatest(F.col("minimum_guarantee_inr_per_quarter")-
                         F.col("earned_royalty_inr"),F.lit(0.0)))
            .withColumn("total_payable_inr",F.round(F.col("earned_royalty_inr")+F.col("guarantee_top_up_inr"),2)))
        quarterly.write.format("delta").mode("overwrite").save(output_path)
        # Per-title audit statement: premium royalty is allocated within each partner
        # by title watch-minute share; free ad revenue is attributed to the title directly.
        premium_titles=(watch.filter(F.col("tier_asof")=="premium")
            .groupBy("month",F.col("partner_id_asof").alias("partner_id"),"content_id")
            .agg(F.sum("watch_minutes").alias("watch_minutes")))
        partner_minutes=(premium_titles.groupBy("month","partner_id")
            .agg(F.sum("watch_minutes").alias("partner_minutes")))
        partner_monthly=(base.groupBy("month",F.col("partner_id_asof").alias("partner_id"))
            .agg(F.sum("premium_royalty_inr").alias("premium_royalty_inr")))
        premium_detail=(premium_titles.join(partner_minutes,["month","partner_id"])
            .join(partner_monthly,["month","partner_id"])
            .withColumn("unrounded_title_royalty",F.col("premium_royalty_inr")*
                F.col("watch_minutes")/F.greatest("partner_minutes",F.lit(1.0)))
            .withColumn("base_cents",F.floor(F.col("unrounded_title_royalty")*100).cast("long"))
            .withColumn("fractional_cent",F.col("unrounded_title_royalty")*100-F.col("base_cents")))
        title_window=Window.partitionBy("month","partner_id").orderBy(F.col("fractional_cent").desc(),F.col("content_id"))
        title_totals=Window.partitionBy("month","partner_id")
        premium_statement=(premium_detail.withColumn("_rank",F.row_number().over(title_window))
            .withColumn("_target_cents",F.round(F.max("premium_royalty_inr").over(title_totals)*100).cast("long"))
            .withColumn("_base_cents",F.sum("base_cents").over(title_totals))
            .withColumn("premium_title_royalty_inr",(F.col("base_cents")+
                F.when(F.col("_rank")<=F.col("_target_cents")-F.col("_base_cents"),1).otherwise(0))/100.0)
            .withColumn("tier",F.lit("premium"))
            .withColumn("free_ad_share_inr",F.lit(0.0))
            .select("month","partner_id","content_id","tier","watch_minutes",
                    "premium_title_royalty_inr","free_ad_share_inr"))
        free_statement=(free_titles.select("month",F.col("partner_id_asof").alias("partner_id"),
                "content_id",F.lit("free").alias("tier"),F.col("free_watch_minutes").alias("watch_minutes"),
                F.lit(0.0).alias("premium_title_royalty_inr"),F.round("free_ad_share_inr",2).alias("free_ad_share_inr")))
        title_statement=premium_statement.unionByName(free_statement)
        title_statement.write.format("delta").mode("overwrite").save(output_path+"_title_statement")
        (quarterly.agg(F.sum("total_payable_inr").alias("partner_total_payable_inr"),
                      F.sum("premium_royalty_inr").alias("premium_royalties_inr"),
                      F.sum("free_ad_revenue_inr").alias("free_ad_shares_inr"),
                      F.sum("guarantee_top_up_inr").alias("guarantee_topups_inr"))
            .write.format("delta").mode("overwrite").save(output_path+"_reconciliation"))
    finally:
        spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--watch",default="data/delta/gold/watch_time_daily"); a=p.parse_args(); run_q15(watch_path=a.watch)
