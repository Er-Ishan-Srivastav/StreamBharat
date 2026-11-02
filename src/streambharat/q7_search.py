"""Q7 — Normalise searches, group near-duplicates and measure search-to-play conversion."""
from __future__ import annotations
import argparse, re
from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_session import create_spark


def run_q7(search_path="data/raw/search/search_logs.json", playback_path="data/delta/silver/playback_clean",
           content_path="data/delta/silver/catalog/dim_content", clean_path="data/delta/silver/search_clean",
           conversion_path="data/delta/gold/search_conversion", gaps_path="data/delta/gold/weekly_content_gaps"):
    spark=create_spark("StreamBharat-Q7-Search-Analytics")
    try:
        logs=(spark.read.json(search_path).withColumn("search_ts",F.to_timestamp("ts"))
            .withColumn("query_norm",F.lower(F.trim(F.regexp_replace("query_text",r"[^\p{L}\p{N} ]"," "))))
            .withColumn("query_norm",F.regexp_replace("query_norm",r"(.)\1{1,}","$1"))
            .withColumn("query_norm",F.regexp_replace("query_norm",r"\s+"," "))
            .filter(F.length("query_norm")>0))
        terms=logs.select("query_norm").distinct().withColumn("first_char",F.substring("query_norm",1,1)).withColumn("term_len",F.length("query_norm"))
        left=terms.alias("l"); right=terms.alias("r")
        pairs=(left.join(right,(F.col("l.first_char")==F.col("r.first_char"))&
              (F.abs(F.col("l.term_len")-F.col("r.term_len"))<=2)&
              (F.col("l.query_norm")<=F.col("r.query_norm")),"inner")
              .withColumn("distance",F.levenshtein("l.query_norm","r.query_norm"))
              .filter(F.col("distance")<=2))
        neighbours=(pairs.select(F.col("l.query_norm").alias("query_norm"),F.col("r.query_norm").alias("candidate"))
            .unionByName(pairs.select(F.col("r.query_norm").alias("query_norm"),F.col("l.query_norm").alias("candidate")))
            .groupBy("query_norm").agg(F.min("candidate").alias("query_group")))
        clean=logs.join(neighbours,"query_norm","left").withColumn("query_group",F.coalesce("query_group","query_norm"))
        clean.write.format("delta").mode("overwrite").save(clean_path)
        plays=(spark.read.format("delta").load(playback_path).filter(F.col("event_type")=="start")
            .select("profile_id",F.to_timestamp("event_ts_corrected").alias("play_ts"),"content_id"))
        joined=clean.alias("s").join(plays.alias("p"),
            (F.col("s.profile_id")==F.col("p.profile_id"))&
            (F.col("p.play_ts")>=F.col("s.search_ts"))&
            (F.col("p.play_ts")<=F.col("s.search_ts")+F.expr("INTERVAL 10 MINUTES")),"left")
        conv=(joined.groupBy(F.col("s.search_id").alias("search_id"),F.col("s.profile_id").alias("profile_id"),
            F.col("s.query_group").alias("query_group"),F.col("s.search_ts").alias("search_ts"))
            .agg(F.max(F.when(F.col("p.play_ts").isNotNull(),1).otherwise(0)).alias("converted")))
        conv.write.format("delta").mode("overwrite").save(conversion_path)
        catalog=(spark.read.format("delta").load(content_path).select(F.lower(F.regexp_replace("title",r"[^\p{L}\p{N} ]"," ")).alias("title_norm"),"title","content_id"))
        groups=(clean.filter(F.col("results_count")==0).withColumn("week_start",F.to_date(F.date_trunc("week","search_ts")))
            .groupBy("week_start","query_group").agg(F.countDistinct("search_id").alias("zero_result_searches")))
        # A close match against the current catalogue suggests spelling/discovery failure, not a true gap.
        q=groups.select("query_group").distinct().alias("q"); t=catalog.alias("t")
        match=(q.crossJoin(t).withColumn("distance",F.levenshtein(F.col("q.query_group"),F.col("t.title_norm")))
            .filter(F.col("distance")<=2).groupBy(F.col("q.query_group").alias("query_group"))
            .agg(F.min(F.struct("distance","title")).alias("title_match")))
        ranked=(groups.join(match,"query_group","left").withColumn("likely_existing_title",F.col("title_match.title"))
            .withColumn("gap_type",F.when(F.col("likely_existing_title").isNotNull(),"possible_spelling_or_search_gap").otherwise("candidate_content_gap")))
        w=Window.partitionBy("week_start").orderBy(F.col("zero_result_searches").desc(),F.col("query_group"))
        ranked=ranked.withColumn("rank",F.row_number().over(w)).filter(F.col("rank")<=50)
        ranked.write.format("delta").mode("overwrite").save(gaps_path)
    finally: spark.stop()

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--search",default="data/raw/search/search_logs.json"); a=p.parse_args(); run_q7(search_path=a.search)
