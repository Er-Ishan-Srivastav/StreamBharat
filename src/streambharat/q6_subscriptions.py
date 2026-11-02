"""Q6 — Event-to-state subscription periods, grace handling and gateway reconciliation."""
from __future__ import annotations
import argparse
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q6(events_path="data/raw/billing/subscription_events.json", settlements_path="data/raw/billing/gateway_settlements.csv",
           periods_path="data/delta/gold/subscription_periods", daily_path="data/delta/gold/subscribers_daily"):
    spark=create_spark("StreamBharat-Q6-Subscription-Lifecycle")
    try:
        events=(spark.read.json(events_path).withColumn("event_ts",F.to_timestamp("event_ts"))
            .filter(F.col("user_id").isNotNull()&F.col("event_ts").isNotNull())
            .dropDuplicates(["user_id","event_type","event_ts","plan","gateway"]))
        failures=events.filter(F.col("event_type")=="payment_failed").alias("f")
        renewals=events.filter(F.col("event_type")=="renew").alias("r")
        failure_resolution=(failures.join(renewals,
            (F.col("f.user_id")==F.col("r.user_id"))&(F.col("r.event_ts")>F.col("f.event_ts"))&
            (F.col("r.event_ts")<=F.col("f.event_ts")+F.expr("INTERVAL 7 DAYS")),"left")
            .groupBy(F.col("f.user_id").alias("user_id"),F.col("f.event_ts").alias("failure_ts"),
                     F.col("f.plan").alias("plan"),F.col("f.price").alias("price"),F.col("f.gateway").alias("gateway"))
            .agg(F.min("r.event_ts").alias("recovery_ts")))
        grace=(failure_resolution.select("user_id",F.col("failure_ts").alias("start_ts"),"plan","price","gateway")
            .withColumn("status",F.lit("grace")))
        failed_terminal=(failure_resolution.filter(F.col("recovery_ts").isNull())
            .select("user_id",(F.col("failure_ts")+F.expr("INTERVAL 7 DAYS")).alias("start_ts"),
                    "plan","price","gateway")
            .withColumn("status",F.lit("churned")))
        ordinary=(events.filter(F.col("event_type")!="payment_failed")
            .withColumn("status",F.when(F.col("event_type")=="trial_start","trial")
                .when(F.col("event_type").isin("subscribe","renew","upgrade","downgrade","resume"),"paid")
                .when(F.col("event_type")=="pause","paused")
                .when(F.col("event_type")=="cancel","churned"))
            .filter(F.col("status").isNotNull())
            .select("user_id",F.col("event_ts").alias("start_ts"),"plan","price","gateway","status"))
        all_states=ordinary.unionByName(grace).unionByName(failed_terminal)
        # If several events share a timestamp, terminal pause/cancel takes precedence;
        # an explicit later resume/renew starts a fresh paid state.
        priority=F.when(F.col("status")=="churned",3).when(F.col("status")=="paused",2).otherwise(1)
        w=Window.partitionBy("user_id","start_ts").orderBy(priority.desc())
        all_states=(all_states.withColumn("_rn",F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn"))
        order=Window.partitionBy("user_id").orderBy("start_ts")
        periods=(all_states.withColumn("end_ts",F.lead("start_ts").over(order))
            .withColumn("start_date",F.to_date("start_ts"))
            .withColumn("end_date",F.to_date("end_ts"))
            .withColumn("monthly_price_inr",F.coalesce(F.col("price").cast("double"),F.lit(0.0)))
            .withColumn("end_exclusive",F.lit(True)))
        periods.write.format("delta").mode("overwrite").save(periods_path)
        # Daily state uses an end-exclusive interval and resolves same-day transitions
        # to the event with the latest effective start timestamp.
        dated=(periods.withColumn("last_day",F.when(F.col("end_date").isNull(),F.current_date())
                   .otherwise(F.date_sub("end_date",1)))
            .filter(F.col("last_day")>=F.col("start_date"))
            .withColumn("snapshot_date",F.explode(F.sequence("start_date","last_day",F.expr("INTERVAL 1 DAY")))))
        day_order=Window.partitionBy("user_id","snapshot_date").orderBy(F.col("start_ts").desc())
        daily=(dated.withColumn("_rn",F.row_number().over(day_order)).filter(F.col("_rn")==1).drop("_rn","last_day")
            .select("snapshot_date","user_id","plan","status","monthly_price_inr","gateway"))
        daily.write.format("delta").mode("overwrite").save(daily_path)
        paid=daily.filter(F.col("status")=="paid").groupBy("snapshot_date","plan").agg(
            F.countDistinct("user_id").alias("paid_subscribers"),F.sum("monthly_price_inr").alias("mrr_inr"))
        paid.write.format("delta").mode("overwrite").save("data/delta/gold/paid_subscriber_counts")
        settlements=(spark.read.option("header",True).option("inferSchema",True).csv(settlements_path)
            .withColumn("settled_date",F.to_date("settled_ts"))
            .withColumn("month",F.date_trunc("month","settled_date")))
        settled=settlements.groupBy("month","gateway").agg(F.sum("amount_inr").alias("settled_revenue_inr"))
        # Build one expected paid amount per user/month before aggregating by gateway.
        # Renewal/upgrade events can create multiple paid periods inside one month;
        # summing each period independently would double-count the same subscriber.
        expected_user_month=(periods.filter(F.col("status")=="paid")
            .withColumn("month",F.explode(F.sequence(F.date_trunc("month","start_date"),
                F.date_trunc("month",F.coalesce(F.date_sub("end_date",1),F.current_date())),F.expr("INTERVAL 1 MONTH"))))
            .withColumn("_rn",F.row_number().over(Window.partitionBy("month","user_id").orderBy(F.col("start_ts").desc())))
            .filter(F.col("_rn")==1).select("month","user_id","gateway",F.col("monthly_price_inr").alias("expected_user_mrr_inr")))
        expected=expected_user_month.groupBy("month","gateway").agg(
            F.sum("expected_user_mrr_inr").alias("expected_mrr_inr"))
        rec=(expected.join(settled,["month","gateway"],"full")
            .withColumn("settled_revenue_inr",F.coalesce("settled_revenue_inr",F.lit(0.0)))
            .withColumn("expected_mrr_inr",F.coalesce("expected_mrr_inr",F.lit(0.0)))
            .withColumn("variance_inr",F.col("settled_revenue_inr")-F.col("expected_mrr_inr")))
        rec.write.format("delta").mode("overwrite").save("data/delta/gold/gateway_reconciliation")
    finally:
        spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--events",default="data/raw/billing/subscription_events.json"); p.add_argument("--settlements",default="data/raw/billing/gateway_settlements.csv"); a=p.parse_args(); run_q6(a.events,a.settlements)
