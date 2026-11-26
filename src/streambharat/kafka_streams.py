"""Spark Structured Streaming readers for StreamBharat Kafka topics.

Kafka values are JSON payloads produced by kafka_producer.py. This module owns
transport parsing and delegates business calculations to the existing Q17/Q18
foreachBatch functions.
"""
from __future__ import annotations

import argparse
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType
)

from .q17_trending import PLAY_SCHEMA, _process_batch as process_plays_batch
from .q18_live_ad_pacing import AD_SCHEMA, _process_ads_batch
from .spark_session import create_spark

PLAY_SCHEMA_WITH_ID = StructType(PLAY_SCHEMA.fields)
AD_SCHEMA_WITH_ID = StructType(AD_SCHEMA.fields)

def kafka_json_stream(spark, topic: str, schema: StructType, bootstrap_servers: str,
                      starting_offsets: str = "earliest"):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("value").cast("string").alias("_json"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("_payload", F.from_json(F.col("_json"), schema))
        .filter(F.col("_payload").isNotNull())
    )
    return parsed.select(
        "_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset",
        "_kafka_timestamp", "_json", "_payload.*"
    )

def run_plays(bootstrap_servers: str, topic: str, checkpoint: str,
              output_paths: dict[str, str], starting_offsets: str = "earliest"):
    spark = create_spark("StreamBharat-Kafka-Q17-Trending")
    try:
        stream = kafka_json_stream(spark, topic, PLAY_SCHEMA_WITH_ID,
                                   bootstrap_servers, starting_offsets)
        # Keep the same transformation and sink logic used by the original file source.
        query = (
            stream.writeStream
            .foreachBatch(lambda df, batch_id: process_plays_batch(
                df, batch_id, spark, **output_paths
            ))
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_kafka_q17_trending")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()

def run_ads(bootstrap_servers: str, topic: str, checkpoint: str,
            campaigns_path: str, ledger_path: str, cap_state_path: str,
            spend_state_path: str, capped_path: str, alerts_path: str,
            match_start: str | None = None, match_hours: float = 3,
            starting_offsets: str = "earliest"):
    from datetime import datetime
    spark = create_spark("StreamBharat-Kafka-Q18-Ad-Pacing")
    try:
        campaigns = spark.read.option("header", True).option("inferSchema", True).csv(campaigns_path)
        stream = kafka_json_stream(spark, topic, AD_SCHEMA_WITH_ID,
                                   bootstrap_servers, starting_offsets)
        query = (
            stream.writeStream
            .foreachBatch(lambda df, batch_id: _process_ads_batch(
                df, batch_id, spark, campaigns, ledger_path, cap_state_path,
                spend_state_path, capped_path, alerts_path,
                match_start or datetime.now().replace(hour=18, minute=0,
                    second=0, microsecond=0).isoformat(sep=" "), match_hours,
            ))
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_kafka_q18_ad_pacing")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Run a StreamBharat Kafka → Spark streaming job")
    parser.add_argument("--stream", choices=["plays", "ads"], required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic")
    parser.add_argument("--checkpoint")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--campaigns", default="data/raw/ads/ad_campaigns.csv")
    parser.add_argument("--match-start")
    parser.add_argument("--match-hours", type=float, default=3)
    parser.add_argument("--baseline", default="data/delta/reference/trending_baseline")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--bronze", default="data/delta/bronze/playback")
    parser.add_argument("--buffer", default="data/delta/silver/trending_event_buffer")
    parser.add_argument("--output", default="data/delta/gold/trending")
    parser.add_argument("--ledger", default="data/delta/silver/live_ad_impression_ledger")
    parser.add_argument("--cap-state", default="data/delta/silver/live_ad_caps_state")
    parser.add_argument("--spend-state", default="data/delta/silver/live_ad_spend_state")
    parser.add_argument("--capped", default="data/delta/gold/capped_users_live")
    parser.add_argument("--alerts", default="data/delta/gold/pacing_alerts_live")
    args = parser.parse_args()

    if args.stream == "plays":
        run_plays(
            args.bootstrap_servers, args.topic or "streambharat.plays",
            args.checkpoint or "data/checkpoints/kafka_q17",
            {"baseline_path": args.baseline, "catalog_path": args.catalog,
             "bronze_path": args.bronze, "buffer_path": args.buffer,
             "output_path": args.output},
            args.starting_offsets,
        )
    else:
        run_ads(
            args.bootstrap_servers, args.topic or "streambharat.ads",
            args.checkpoint or "data/checkpoints/kafka_q18",
            args.campaigns, args.ledger, args.cap_state, args.spend_state,
            args.capped, args.alerts, args.match_start, args.match_hours,
            args.starting_offsets,
        )

if __name__ == "__main__":
    main()
