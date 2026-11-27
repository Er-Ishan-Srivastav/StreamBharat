"""Batch master-data preparation for catalog, licences, users, profiles and CDN."""
from __future__ import annotations

import argparse
import glob
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType, StringType, StructField, StructType,
)

from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


USER_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("signup_ts", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("age_band", StringType(), True),
    StructField("acquisition_channel", StringType(), True),
])
PROFILE_SCHEMA = StructType([
    StructField("profile_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("is_kids", BooleanType(), True),
])
LICENCE_SCHEMA = StructType([
    StructField("content_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("licence_start", StringType(), True),
    StructField("licence_end", StringType(), True),
])
PARTNER_SCHEMA = StructType([
    StructField("partner_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("revenue_share_pct", StringType(), True),
    StructField("minimum_guarantee_inr_per_quarter", StringType(), True),
])
PLAN_SCHEMA = StructType([
    StructField("plan", StringType(), True),
    StructField("price_inr_per_month", StringType(), True),
    StructField("max_screens", IntegerType(), True),
])
CDN_RATE_SCHEMA = StructType([
    StructField("cdn", StringType(), True),
    StructField("region", StringType(), True),
    StructField("rate_inr_per_gb", StringType(), True),
])


def build_catalog_history(spark, catalog_dir: str, genre_mapping_path: str):
    """Flatten all monthly snapshots and retain partner/tier validity intervals."""
    paths = sorted(glob.glob(os.path.join(catalog_dir, "catalog_*.json")))
    if not paths:
        raise FileNotFoundError(f"No catalog_YYYY-MM-01.json files found in {catalog_dir}")
    snapshots = [read_catalog_snapshot(spark, path) for path in paths]
    all_rows = snapshots[0]
    for frame in snapshots[1:]:
        all_rows = all_rows.unionByName(frame)

    ordering = Window.partitionBy("content_id").orderBy("snapshot_date")
    title_history = (
        all_rows.select(
            "content_id", "title", "type", "duration_sec", "parent_series_id",
            "season_no", "episode_no", "languages", "genres", "age_rating",
            "partner_id", "tier", "snapshot_date",
        )
        .dropDuplicates(["content_id", "snapshot_date"])
        .withColumn("valid_from", F.col("snapshot_date"))
        .withColumn("valid_to", F.lead("snapshot_date").over(ordering))
        .select(
            "content_id", "title", "type", "duration_sec", "parent_series_id",
            "season_no", "episode_no", "languages", "genres", "age_rating",
            "partner_id", "tier", "valid_from", "valid_to",
        )
    )
    episodes = title_history.filter(F.col("type") == "episode")
    mapping = (
        spark.read.schema(
            StructType([
                StructField("raw_genre", StringType(), True),
                StructField("standard_genre", StringType(), True),
            ])
        )
        .option("header", True)
        .csv(genre_mapping_path)
        .select(
            F.lower(F.trim("raw_genre")).alias("raw_genre_key"),
            F.col("standard_genre"),
        )
    )
    genres = (
        title_history.select("content_id", "valid_from", "valid_to", F.explode_outer("genres").alias("raw_genre"))
        .withColumn("raw_genre_key", F.lower(F.trim("raw_genre")))
        .join(mapping, "raw_genre_key", "left")
        .withColumn("genre", F.coalesce("standard_genre", "raw_genre"))
        .select("content_id", "valid_from", "valid_to", "genre")
        .dropDuplicates()
    )
    return title_history, episodes, genres


def parse_cdn_logs(spark, data_root: str, edges_path: str) -> tuple[DataFrame, DataFrame]:
    """Unify AkashCDN space-separated and NimbusEdge tab-separated logs."""
    akash_path = os.path.join(data_root, "cdn", "akashcdn_2026-09.log")
    nimbus_path = os.path.join(data_root, "cdn", "nimbusedge_2026-09.tsv")
    if not os.path.isfile(akash_path) or not os.path.isfile(nimbus_path):
        raise FileNotFoundError("Both September CDN source logs are required")

    akash_raw = spark.read.text(akash_path).withColumnRenamed("value", "raw_line")
    pattern = (
        r'^(\S+)\s+(\S+)\s+(\S+)\s+"(\S+)\s+([^\s]+)\s+[^"]+"\s+'
        r'(\d{3})\s+(\d+)\s+"([^"]+)"$'
    )
    akash = akash_raw.select(
        F.lit("AkashCDN").alias("cdn"),
        F.regexp_extract("raw_line", pattern, 2).alias("edge"),
        F.regexp_extract("raw_line", pattern, 3).alias("client_hash"),
        F.to_timestamp(F.regexp_extract("raw_line", pattern, 1)).alias("request_ts"),
        F.regexp_extract("raw_line", pattern, 5).alias("path"),
        F.regexp_extract("raw_line", pattern, 6).cast("int").alias("status"),
        F.regexp_extract("raw_line", pattern, 7).cast("long").alias("bytes"),
        F.regexp_extract("raw_line", pattern, 8).alias("user_agent"),
    ).withColumn("content_id", F.regexp_extract("path", r"/v/([^/]+)/", 1)) \
     .withColumn("quality", F.regexp_extract("path", r"/v/[^/]+/([^/]+)/", 1)) \
     .withColumn("segment", F.regexp_extract("path", r"/(seg_[^/]+)$", 1))

    nimbus_raw = spark.read.text(nimbus_path).withColumnRenamed("value", "raw_line")
    fields = F.split(F.col("raw_line"), "\\t", -1)
    nimbus = nimbus_raw.select(
        F.lit("NimbusEdge").alias("cdn"),
        fields.getItem(1).alias("edge"),
        fields.getItem(2).alias("client_hash"),
        F.to_timestamp(F.from_unixtime(fields.getItem(0).cast("long"))).alias("request_ts"),
        fields.getItem(3).alias("path"),
        fields.getItem(4).cast("int").alias("status"),
        fields.getItem(5).cast("long").alias("bytes"),
        fields.getItem(6).alias("user_agent"),
    ).withColumn("content_id", F.regexp_extract("path", r"^vod/([^/]+)/", 1)) \
     .withColumn("quality", F.regexp_extract("path", r"^vod/[^/]+/([^/]+)/", 1)) \
     .withColumn("segment", F.regexp_extract("path", r"^vod/[^/]+/[^/]+/(.+)$", 1))

    unified = akash.unionByName(nimbus)
    edge_map = spark.read.option("header", True).option("inferSchema", True).csv(edges_path)
    mapping = (
        edge_map.select(F.col("edge").alias("edge_code"), "region")
        .unionByName(edge_map.select(F.col("pop_code").alias("edge_code"), "region"))
        .filter(F.col("edge_code").isNotNull())
        .dropDuplicates(["edge_code"])
    )
    unified = unified.join(mapping, "edge_code", "left") if "edge_code" in unified.columns else unified
    if "edge_code" not in unified.columns:
        unified = unified.join(mapping, unified.edge == mapping.edge_code, "left").drop("edge_code")
    malformed = unified.filter(
        F.col("request_ts").isNull()
        | F.col("content_id").isNull()
        | (F.length("content_id") == 0)
        | F.col("status").isNull()
    ).withColumn("reason_code", F.lit("MALFORMED_CDN_LOG"))
    valid = unified.filter(
        F.col("request_ts").isNotNull()
        & F.col("content_id").isNotNull()
        & (F.length("content_id") > 0)
        & F.col("status").isNotNull()
    )
    return valid, malformed


def run_batch_master(
    data_root: str = "data/raw",
    output_root: str = "lake/silver",
) -> None:
    spark = create_spark("StreamBharat-Batch-Master-Data")
    try:
        catalog_dir = os.path.join(data_root, "content")
        title_history, episodes, genres = build_catalog_history(
            spark,
            catalog_dir,
            os.path.join(data_root, "reference", "genre_mapping.csv"),
        )
        title_history.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "titles_history"))
        episodes.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "episodes"))
        genres.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "content_genres"))

        licences = (
            spark.read.schema(LICENCE_SCHEMA).option("header", True)
            .csv(os.path.join(data_root, "content", "licences.csv"))
            .withColumn("licence_start", F.to_date("licence_start"))
            .withColumn("licence_end", F.to_date("licence_end"))
        )
        licences.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "licences"))

        users = (
            spark.read.schema(USER_SCHEMA)
            .json(os.path.join(data_root, "app", "users.json"))
            .withColumn("signup_ts", F.to_timestamp("signup_ts"))
        )
        users.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "users"))
        profiles = spark.read.schema(PROFILE_SCHEMA).json(
            os.path.join(data_root, "app", "profiles.json")
        )
        profiles.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "profiles"))
        city_state = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(os.path.join(data_root, "reference", "city_state.csv"))
            .select(F.trim("city").alias("city"), F.trim("state").alias("state"))
            .dropDuplicates(["city"])
        )
        city_state.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "city_state"))

        partners = (
            spark.read.schema(PARTNER_SCHEMA).option("header", True)
            .csv(os.path.join(data_root, "reference", "content_partners.csv"))
            .withColumn("revenue_share_pct", F.col("revenue_share_pct").cast("double"))
            .withColumn(
                "minimum_guarantee_inr_per_quarter",
                F.col("minimum_guarantee_inr_per_quarter").cast("decimal(18,2)"),
            )
        )
        partners.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "content_partners"))
        plans = (
            spark.read.schema(PLAN_SCHEMA).option("header", True)
            .csv(os.path.join(data_root, "reference", "plans.csv"))
            .withColumn("price_inr_per_month", F.col("price_inr_per_month").cast("decimal(18,2)"))
        )
        plans.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "plans"))
        cdn_rates = (
            spark.read.schema(CDN_RATE_SCHEMA).option("header", True)
            .csv(os.path.join(data_root, "reference", "cdn_rates.csv"))
            .withColumn("rate_inr_per_gb", F.col("rate_inr_per_gb").cast("double"))
        )
        cdn_rates.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "cdn_rates"))

        cdn, malformed = parse_cdn_logs(
            spark,
            data_root,
            os.path.join(data_root, "reference", "cdn_edges.csv"),
        )
        cdn.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "cdn_requests"))
        malformed.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(os.path.join(output_root, "cdn_requests_quarantine"))
        metrics = spark.createDataFrame(
            [("cdn_log_rows", cdn.count()), ("cdn_malformed_rows", malformed.count())],
            ["metric_name", "metric_value"],
        ).withColumn("measured_at", F.current_timestamp())
        metrics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save("lake/gold/master_data_metrics")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load StreamBharat master and reference data")
    parser.add_argument("--data-root", default="data/raw")
    parser.add_argument("--output-root", default="lake/silver")
    args = parser.parse_args()
    run_batch_master(args.data_root, args.output_root)


if __name__ == "__main__":
    main()
