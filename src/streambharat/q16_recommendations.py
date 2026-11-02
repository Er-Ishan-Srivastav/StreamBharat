"""Q16 — Implicit-feedback ALS recommendations with licensing/kids filters and a language baseline."""
from __future__ import annotations
import argparse
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from .spark_session import create_spark


def _precision_at_10(recommendations, held):
    """Include zero-hit held-out profiles in the macro Precision@10 denominator."""
    profiles=held.select("profile_id").distinct()
    hits=(recommendations.join(held,["profile_id","content_id"],"inner")
        .groupBy("profile_id").agg(F.countDistinct("content_id").alias("hits")))
    return (profiles.join(hits,"profile_id","left").fillna({"hits":0})
        .withColumn("precision_at_10",F.col("hits")/10.0))


def run_q16(watch_path="data/delta/gold/watch_time_daily", ratings_path="data/raw/app/ratings.json",
            content_path="data/delta/silver/catalog/dim_content", languages_path="data/delta/silver/catalog/content_language",
            profiles_path="data/raw/app/profiles.json", users_path="data/raw/app/users.json",
            licences_path="data/raw/content/licences.csv", as_of="2026-10-31",
            output_path="data/delta/gold/recommendations"):
    spark=create_spark("StreamBharat-Q16-ALS-Recommendations")
    try:
        watch=spark.read.format("delta").load(watch_path)
        content=spark.read.format("delta").load(content_path).select("content_id","title","age_rating")
        languages=spark.read.format("delta").load(languages_path).dropDuplicates(["content_id","language"])
        interactions=(watch.groupBy("profile_id","content_id")
            .agg(F.sum("watch_minutes").alias("watch_minutes"),F.max("completion").alias("completion"),
                 F.countDistinct("watch_date").alias("rewatch_days"),F.max("watch_date").alias("last_watch_date"))
            .withColumn("implicit_score",F.log1p(F.col("watch_minutes")/10)+
                F.coalesce("completion",F.lit(0.0))*2+F.log1p("rewatch_days")))
        ratings=spark.read.json(ratings_path).groupBy("profile_id","content_id").agg(F.avg("rating").alias("rating"))
        interactions=(interactions.join(ratings,["profile_id","content_id"],"left")
            .withColumn("implicit_score",F.col("implicit_score")+
                F.when(F.col("rating")>=4,F.lit(1.0)).when(F.col("rating")<=2,F.lit(-0.25)).otherwise(F.lit(0.0)))
            .withColumn("implicit_score",F.greatest(F.col("implicit_score"),F.lit(0.01))))
        item_order=Window.partitionBy("profile_id").orderBy(F.col("last_watch_date").desc(),F.col("content_id"))
        split=interactions.withColumn("holdout_rank",F.row_number().over(item_order))
        train=split.filter(F.col("holdout_rank")>5)
        test=split.filter(F.col("holdout_rank")<=5)
        # String identifiers are explicitly mapped to stable integer keys for MLlib ALS.
        users_map=spark.createDataFrame([(r[0],i) for i,r in enumerate(
            train.select("profile_id").distinct().orderBy("profile_id").collect())],"profile_id string, user_idx int")
        items_map=spark.createDataFrame([(r[0],i) for i,r in enumerate(
            train.select("content_id").distinct().orderBy("content_id").collect())],"content_id string, item_idx int")
        train_i=(train.join(users_map,"profile_id").join(items_map,"content_id")
            .select(F.col("user_idx").cast("int"),F.col("item_idx").cast("int"),
                    F.col("implicit_score").cast("float").alias("rating")))
        model=(ALS(userCol="user_idx",itemCol="item_idx",ratingCol="rating",implicitPrefs=True,
            coldStartStrategy="drop",nonnegative=True,rank=20,maxIter=10,regParam=0.1,alpha=1.0)
            .fit(train_i))
        profiles=spark.read.json(profiles_path).select("profile_id","user_id","is_kids")
        userdim=spark.read.json(users_path).select("user_id","state")
        licences=(spark.read.option("header",True).option("inferSchema",True).csv(licences_path)
            .withColumn("licence_start",F.to_date("licence_start")).withColumn("licence_end",F.to_date("licence_end")))
        # A per-profile language preference is the language with the most watch minutes.
        profile_language=(train.join(languages,"content_id").groupBy("profile_id","language")
            .agg(F.sum("watch_minutes").alias("language_minutes")))
        lang_rank=Window.partitionBy("profile_id").orderBy(F.col("language_minutes").desc(),F.col("language"))
        profile_language=(profile_language.withColumn("_rn",F.row_number().over(lang_rank))
            .filter(F.col("_rn")==1).select("profile_id","language"))
        # Use training-only history for offline candidate generation so held-out
        # positives remain recommendable during evaluation. Production outputs below
        # are filtered against the complete interaction history.
        train_seen=train.select("profile_id","content_id").distinct()
        all_seen=interactions.select("profile_id","content_id").distinct()
        # ALS produces a broad candidate pool; business filters then choose the best ten.
        recs=(model.recommendForAllUsers(500).select("user_idx",F.explode("recommendations").alias("rec"))
            .select("user_idx",F.col("rec.item_idx").alias("item_idx"),F.col("rec.rating").alias("score"))
            .join(users_map,"user_idx").join(items_map,"item_idx").join(profiles,"profile_id")
            .join(userdim,"user_id").join(content,"content_id"))
        r=recs.alias("r"); l=licences.alias("l")
        candidates=(r.join(l,(F.col("r.content_id")==F.col("l.content_id"))&
                  (F.col("r.state")==F.col("l.region"))&
                  F.lit(as_of).cast("date").between(F.col("l.licence_start"),F.col("l.licence_end")),"inner")
            .select("r.*")
            .filter(~(F.col("is_kids")&F.col("age_rating").isin("16+","18+")))
            .join(train_seen,["profile_id","content_id"],"left_anti"))
        rec_rank=Window.partitionBy("profile_id").orderBy(F.col("score").desc(),F.col("content_id"))
        eval_als=(candidates.withColumn("rank",F.row_number().over(rec_rank)).filter(F.col("rank")<=10)
            .select("profile_id","user_id","content_id","title","score","rank"))
        production_als=(candidates.join(all_seen,["profile_id","content_id"],"left_anti")
            .withColumn("rank",F.row_number().over(rec_rank)).filter(F.col("rank")<=10)
            .select("profile_id","user_id","content_id","title","score","rank"))
        # Popularity baseline: choose the most watched titles in each profile's preferred language,
        # then apply the same watched/licence/kids exclusions.
        popular=(train.join(languages,"content_id").groupBy("language","content_id")
            .agg(F.countDistinct("profile_id").alias("viewers"),F.sum("watch_minutes").alias("minutes"))
            .join(content,"content_id"))
        b=(profile_language.join(profiles,"profile_id").join(userdim,"user_id")
            .join(popular,"language")).alias("b")
        bl=licences.alias("l")
        baseline_candidates=(b.join(bl,(F.col("b.content_id")==F.col("l.content_id"))&
                (F.col("b.state")==F.col("l.region"))&
                F.lit(as_of).cast("date").between(F.col("l.licence_start"),F.col("l.licence_end")),"inner")
            .select("b.*")
            .filter(~(F.col("is_kids")&F.col("age_rating").isin("16+","18+")))
            .join(train_seen,["profile_id","content_id"],"left_anti"))
        base_window=Window.partitionBy("profile_id").orderBy(F.col("viewers").desc(),F.col("minutes").desc(),F.col("content_id"))
        eval_baseline=(baseline_candidates.withColumn("rank",F.row_number().over(base_window)).filter(F.col("rank")<=10)
            .select("profile_id","user_id","content_id","title","viewers","rank"))
        production_baseline=(baseline_candidates.join(all_seen,["profile_id","content_id"],"left_anti")
            .withColumn("rank",F.row_number().over(base_window)).filter(F.col("rank")<=10)
            .select("profile_id","user_id","content_id","title","viewers","rank"))
        production_als.write.format("delta").mode("overwrite").save(output_path)
        production_baseline.write.format("delta").mode("overwrite").save(output_path+"_popularity_baseline")
        held=test.select("profile_id","content_id").distinct()
        als_eval=_precision_at_10(eval_als.select("profile_id","content_id"),held)
        baseline_eval=_precision_at_10(eval_baseline.select("profile_id","content_id"),held)
        eval_summary=(als_eval.agg(F.avg("precision_at_10").alias("mean_precision_at_10"),
                F.count("*").alias("profiles_evaluated"))
            .withColumn("model",F.lit("ALS implicit feedback")))
        base_summary=(baseline_eval.agg(F.avg("precision_at_10").alias("mean_precision_at_10"),
                F.count("*").alias("profiles_evaluated"))
            .withColumn("model",F.lit("Most popular by language")))
        eval_summary.unionByName(base_summary).write.format("delta").mode("overwrite").save(output_path+"_evaluation")
    finally:
        spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--as-of",default="2026-10-31"); a=p.parse_args(); run_q16(as_of=a.as_of)
