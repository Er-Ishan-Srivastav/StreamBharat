"""Q1 — Flatten nested catalogue snapshots and build many-to-many bridges."""
from __future__ import annotations
import argparse, os, re
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StringType, IntegerType, StructField,
    StructType)
from .spark_session import create_spark

CAST_SCHEMA = StructType([StructField("name", StringType()), StructField("role", StringType())])
EPISODE_SCHEMA = StructType([
    StructField("content_id", StringType()), StructField("episode_no", IntegerType()),
    StructField("title", StringType()), StructField("duration_sec", IntegerType()),
    StructField("release_date", StringType())])
SEASON_SCHEMA = StructType([
    StructField("season", IntegerType()), StructField("episodes", ArrayType(EPISODE_SCHEMA))])
CATALOG_SCHEMA = StructType([
    StructField("content_id", StringType()), StructField("title", StringType()),
    StructField("type", StringType()), StructField("languages", ArrayType(StringType())),
    StructField("genres", ArrayType(StringType())), StructField("cast", ArrayType(CAST_SCHEMA)),
    StructField("release_date", StringType()), StructField("age_rating", StringType()),
    StructField("partner_id", StringType()), StructField("tier", StringType()),
    StructField("duration_sec", IntegerType()), StructField("seasons", ArrayType(SEASON_SCHEMA))])


def read_catalog_snapshot(spark, path: str, snapshot_date: str | None = None):
    """Return one row per title and episode with a stable parent-series key."""
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    date_value = snapshot_date or os.path.basename(path).split("_")[-1].replace(".json", "")
    raw = spark.read.option("multiLine", True).schema(CATALOG_SCHEMA).json(path)
    titles = raw.select(
        "content_id", "title", "type", "languages", "genres", "cast", "release_date",
        "age_rating", "partner_id", "tier", "duration_sec",
        F.col("content_id").alias("parent_series_id"),
        F.lit(None).cast("int").alias("season_no"),
        F.lit(None).cast("int").alias("episode_no"),
        F.lit(date_value).cast("date").alias("snapshot_date"))
    episodes = (raw.filter(F.col("type") == "series")
        .select(F.col("content_id").alias("parent_series_id"), "title", "type", "languages",
                "genres", "cast", "release_date", "age_rating", "partner_id", "tier",
                F.explode_outer("seasons").alias("season"))
        .select("parent_series_id", "title", "type", "languages", "genres", "cast",
                "release_date", "age_rating", "partner_id", "tier", "season.*")
        .select("parent_series_id", "title", "type", "languages", "genres", "cast",
                "release_date", "age_rating", "partner_id", "tier", "season",
                F.explode_outer("episodes").alias("episode"))
        .select(F.col("episode.content_id").alias("content_id"),
                F.col("episode.title").alias("title"), F.lit("episode").alias("type"),
                "languages", "genres", "cast", F.col("episode.release_date").alias("release_date"),
                "age_rating", "partner_id", "tier", F.col("episode.duration_sec").alias("duration_sec"),
                "parent_series_id", F.col("season").alias("season_no"),
                F.col("episode.episode_no").alias("episode_no"),
                F.lit(date_value).cast("date").alias("snapshot_date")))
    # The series row is retained as the parent; each episode is an independent playable item.
    return titles.unionByName(episodes)


def run_q1(catalog_glob: str = "data/raw/content/catalog_2026-10-01.json",
           genre_mapping_path: str = "data/raw/reference/genre_mapping.csv",
           output_root: str = "data/delta/silver/catalog") -> None:
    spark = create_spark("StreamBharat-Q1-Catalogue")
    try:
        snapshot = read_catalog_snapshot(spark, catalog_glob)
        mapping = (spark.read.option("header", True).option("inferSchema", True)
                   .csv(genre_mapping_path)
                   .select(F.lower(F.trim("raw_genre")).alias("raw_genre_key"),
                           F.col("standard_genre")))
        dim = snapshot.drop("languages", "genres", "cast")
        languages = (snapshot.select("content_id", F.explode_outer("languages").alias("language"))
                     .dropDuplicates())
        genres = (snapshot.select("content_id", F.explode_outer("genres").alias("raw_genre"))
                  .withColumn("raw_genre_key", F.lower(F.trim("raw_genre")))
                  .join(mapping, "raw_genre_key", "left")
                  .withColumn("genre", F.coalesce("standard_genre", "raw_genre"))
                  .select("content_id", "genre").dropDuplicates())
        cast = (snapshot.select("content_id", F.explode_outer("cast").alias("cast_member"))
                .select("content_id", F.col("cast_member.name").alias("name"),
                        F.col("cast_member.role").alias("role")).dropDuplicates())
        for name, frame in {"dim_content": dim, "content_language": languages,
                            "content_genre": genres, "content_cast": cast}.items():
            frame.write.format("delta").mode("overwrite").save(os.path.join(output_root, name))
        language_hours=(snapshot.select("content_id","duration_sec",F.explode_outer("languages").alias("language"))
            .groupBy("language").agg(F.countDistinct("content_id").alias("items"),
                (F.sum("duration_sec")/3600).alias("catalog_hours")))
        genre_hours=(snapshot.select("content_id","duration_sec",F.explode_outer("genres").alias("raw_genre"))
            .withColumn("raw_genre_key",F.lower(F.trim("raw_genre"))).join(mapping,"raw_genre_key","left")
            .withColumn("genre",F.coalesce("standard_genre","raw_genre"))
            .groupBy("genre").agg(F.countDistinct("content_id").alias("items"),
                (F.sum("duration_sec")/3600).alias("catalog_hours")))
        language_hours.write.format("delta").mode("overwrite").save(os.path.join(output_root,"catalog_hours_by_language"))
        genre_hours.write.format("delta").mode("overwrite").save(os.path.join(output_root,"catalog_hours_by_genre"))
    finally:
        spark.stop()

if __name__ == "__main__":
    p=argparse.ArgumentParser(); p.add_argument("--catalog",default="data/raw/content/catalog_2026-10-01.json")
    p.add_argument("--genre-mapping",default="data/raw/reference/genre_mapping.csv"); p.add_argument("--output",default="data/delta/silver/catalog")
    a=p.parse_args(); run_q1(a.catalog,a.genre_mapping,a.output)
