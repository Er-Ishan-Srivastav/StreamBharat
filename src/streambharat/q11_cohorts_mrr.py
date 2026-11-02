"""Q11 — Subscriber cohort retention and monthly recurring revenue bridge."""
from __future__ import annotations
import argparse
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q11(periods_path="data/delta/gold/subscription_periods", users_path="data/raw/app/users.json",
            retention_path="data/delta/gold/cohort_retention", bridge_path="data/delta/gold/mrr_bridge_monthly"):
    spark=create_spark("StreamBharat-Q11-Cohorts-MRR")
    try:
        periods=spark.read.format("delta").load(periods_path)
        users=spark.read.json(users_path).select("user_id","acquisition_channel")
        paid=periods.filter(F.col("status")=="paid")
        cohort=(paid.groupBy("user_id").agg(F.min("start_date").alias("first_paid_date"))
            .withColumn("cohort_month",F.date_trunc("month","first_paid_date")))
        active_months=(paid.withColumn("month",F.explode(F.sequence(F.date_trunc("month","start_date"),
                F.date_trunc("month",F.coalesce(F.date_sub("end_date",1),F.current_date())),F.expr("INTERVAL 1 MONTH"))))
            .groupBy("user_id","month").agg(F.max("monthly_price_inr").alias("mrr")))
        month_bounds=active_months.agg(F.min("month").alias("min_month"),F.max("month").alias("max_month"))
        calendar=(month_bounds.select(F.explode(F.sequence("min_month","max_month",F.expr("INTERVAL 1 MONTH"))).alias("month")))
        cohort_users=cohort.join(users,"user_id","left")
        retention_grid=(cohort_users.crossJoin(calendar).filter(F.col("month")>=F.col("cohort_month"))
            .join(active_months,["user_id","month"],"left")
            .withColumn("mrr",F.coalesce("mrr",F.lit(0.0)))
            .withColumn("months_since_joining",F.floor(F.months_between("month","cohort_month")).cast("int")))
        retention=(retention_grid.groupBy("cohort_month","months_since_joining","acquisition_channel")
            .agg(F.countDistinct(F.when(F.col("mrr")>0,F.col("user_id"))).alias("retained_subscribers")))
        cohort_size=cohort_users.groupBy("cohort_month","acquisition_channel").agg(F.countDistinct("user_id").alias("cohort_size"))
        retention=(retention.join(cohort_size,["cohort_month","acquisition_channel"])
            .withColumn("retention_rate",F.col("retained_subscribers")/F.col("cohort_size")))
        retention.write.format("delta").mode("overwrite").save(retention_path)
        pivot=(retention.groupBy("cohort_month","acquisition_channel")
            .pivot("months_since_joining",list(range(0,19))).agg(F.first("retention_rate")))
        pivot.write.format("delta").mode("overwrite").save(retention_path+"_pivot")
        # A complete user-month spine is needed to observe churned MRR as a zero row.
        user_ids=active_months.select("user_id").distinct()
        mrr_grid=(user_ids.crossJoin(calendar).join(active_months,["user_id","month"],"left")
            .withColumn("mrr",F.coalesce("mrr",F.lit(0.0))))
        order=Window.partitionBy("user_id").orderBy("month")
        prior_window=Window.partitionBy("user_id").orderBy("month").rowsBetween(Window.unboundedPreceding,-1)
        bridge=(mrr_grid.withColumn("previous_mrr",F.lag("mrr").over(order))
            .withColumn("previous_mrr",F.coalesce("previous_mrr",F.lit(0.0)))
            .withColumn("had_prior_paid",F.max(F.when(F.col("mrr")>0,1).otherwise(0)).over(prior_window))
            .withColumn("component",F.when((F.col("previous_mrr")==0)&(F.col("mrr")>0)&(F.coalesce("had_prior_paid",F.lit(0))==1),"reactivated")
                .when((F.col("previous_mrr")==0)&(F.col("mrr")>0),"new")
                .when((F.col("previous_mrr")>0)&(F.col("mrr")==0),"churned")
                .when(F.col("mrr")>F.col("previous_mrr"),"expansion")
                .when(F.col("mrr")<F.col("previous_mrr"),"contraction").otherwise("no_change"))
            .withColumn("mrr_change",F.when(F.col("component").isin("new","reactivated"),F.col("mrr"))
                .when(F.col("component").isin("expansion","contraction"),F.col("mrr")-F.col("previous_mrr"))
                .when(F.col("component")=="churned",-F.col("previous_mrr")).otherwise(F.lit(0.0))))
        summary=(bridge.groupBy("month").pivot("component",["new","expansion","contraction","churned","reactivated","no_change"])
            .agg(F.sum("mrr_change")))
        summary.write.format("delta").mode("overwrite").save(bridge_path)
    finally:
        spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--periods",default="data/delta/gold/subscription_periods"); a=p.parse_args(); run_q11(periods_path=a.periods)
