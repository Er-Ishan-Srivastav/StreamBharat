"""Q10 — Regional daily Top 10 with series roll-up, kids split and national fallback."""
from __future__ import annotations
import argparse
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q10(watch_path="data/delta/gold/watch_time_daily", content_path="data/delta/silver/catalog/dim_content",
            profiles_path="data/raw/app/profiles.json", users_path="data/raw/app/users.json",
            output_path="data/delta/gold/top10_daily", min_viewers=50):
    spark=create_spark("StreamBharat-Q10-Regional-Top10")
    try:
        watch=spark.read.format("delta").load(watch_path)
        content=spark.read.format("delta").load(content_path).select("content_id","parent_series_id","title")
        profiles=spark.read.json(profiles_path).select("profile_id","user_id","is_kids")
        users=spark.read.json(users_path).select("user_id","state")
        enriched=(watch.join(content,"content_id","left").join(profiles,"profile_id","left").join(users,"user_id","left")
            .withColumn("rank_content_id",F.coalesce("parent_series_id","content_id"))
            .filter(F.col("watch_minutes")>=2))
        title=(enriched.groupBy("watch_date","state","is_kids","rank_content_id")
            .agg(F.countDistinct("user_id").alias("viewers"),F.sum("watch_minutes").alias("watch_minutes")))
        # Compute national distinct viewers from profile/user-level evidence, not a sum of states.
        national=(enriched.groupBy("watch_date","is_kids","rank_content_id")
            .agg(F.countDistinct("user_id").alias("viewers"),F.sum("watch_minutes").alias("watch_minutes")))
        state_size=(enriched.groupBy("watch_date","state","is_kids")
            .agg(F.countDistinct("user_id").alias("state_viewers")))
        local=(title.join(state_size,["watch_date","state","is_kids"],"inner")
            .filter(F.col("state_viewers")>=min_viewers).withColumn("fallback_used",F.lit(False)))
        fallback_states=(state_size.filter(F.col("state_viewers")<min_viewers)
            .select("watch_date",F.col("state").alias("fallback_state"),"is_kids"))
        national_copy=(national.join(fallback_states,["watch_date","is_kids"],"inner")
            .withColumnRenamed("fallback_state","state").withColumn("fallback_used",F.lit(True)))
        national_storefront=national.withColumn("state",F.lit("NATIONAL")).withColumn("fallback_used",F.lit(False))
        result=local.unionByName(national_copy,allowMissingColumns=True).unionByName(national_storefront,allowMissingColumns=True)
        order=Window.partitionBy("watch_date","state","is_kids").orderBy(F.col("viewers").desc(),F.col("watch_minutes").desc())
        result=result.withColumn("rank",F.row_number().over(order)).filter(F.col("rank")<=10)
        prior=Window.partitionBy("state","is_kids","rank_content_id").orderBy("watch_date")
        result=(result.withColumn("previous_rank",F.lag("rank").over(prior))
            .withColumn("rank_change",F.when(F.col("previous_rank").isNull(),F.lit(None).cast("int"))
                .otherwise(F.col("previous_rank")-F.col("rank"))))
        title_lookup=content.select(F.col("content_id").alias("rank_content_id"),F.col("title").alias("content_title")).dropDuplicates(["rank_content_id"])
        result=result.join(title_lookup,"rank_content_id","left")
        result.write.format("delta").mode("overwrite").save(output_path)
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--min-viewers",type=int,default=50); a=p.parse_args(); run_q10(min_viewers=a.min_viewers)
