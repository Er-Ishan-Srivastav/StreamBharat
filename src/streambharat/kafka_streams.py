"""Kafka sources for Spark Q17/Q18, preserving existing Delta business sinks."""
from __future__ import annotations

import argparse
import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .q17_trending import PLAYS_SCHEMA, _persist_and_recompute_trends, build_historical_baseline
from .q18_live_ad_pacing import AD_SCHEMA, _process_ads_batch
from .spark_session import create_spark


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
    decoded = raw.select(
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("value").cast("string").alias("_json"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    ).withColumn("_payload", F.from_json(F.col("_json"), schema))
    invalid_path = os.getenv("KAFKA_DLQ_PATH", "data/delta/quality/kafka_invalid_events")
    def quarantine_invalid(batch_df, batch_id):
        invalid = batch_df.filter(F.col("_payload").isNull()).select(
            F.col("_kafka_key"),
            F.col("_json"),
            F.col("_kafka_topic"),
            F.col("_kafka_partition").cast("string").alias("_kafka_partition"),
            F.col("_kafka_offset").cast("string").alias("_kafka_offset"),
            F.col("_kafka_timestamp").cast("string").alias("_kafka_timestamp"),
        ).withColumn("batch_id", F.lit(batch_id)).withColumn("quarantined_at", F.current_timestamp())
        if not invalid.isEmpty():
            invalid.write.format("delta").mode("append").save(invalid_path)

    # Keep invalid payloads on a parallel query so valid business records continue.
    invalid_stream = decoded.filter(F.col("_payload").isNull())
    invalid_query = (
        invalid_stream.writeStream.foreachBatch(quarantine_invalid)
        .option("checkpointLocation", os.getenv(
            "KAFKA_DLQ_CHECKPOINT", "data/checkpoints/kafka_dlq_" + topic.replace(".", "_")
        ))
        .queryName("streambharat_kafka_dlq_" + topic.replace(".", "_"))
        .start()
    )
    return decoded.filter(F.col("_payload").isNotNull()).select(
        "_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset",
        "_kafka_timestamp", "_json", "_payload.*"
    )


def run_plays(bootstrap_servers: str, topic: str, checkpoint: str,
              bronze: str, catalog: str, baseline: str, output: str,
              starting_offsets: str = "earliest", as_of: str | None = None,
              min_viewers: int = 200, top_n: int = 20, late_minutes: int = 15):
    as_of = as_of or datetime.now().date().isoformat()
    spark = create_spark("StreamBharat-Kafka-Q17-Trending")
    try:
        build_historical_baseline(spark, bronze, catalog, baseline, as_of)
        baseline_df = spark.read.format("delta").load(baseline)
        live_path = os.path.join(os.path.dirname(output), "_kafka_live_play_events")
        stream = kafka_json_stream(spark, topic, PLAYS_SCHEMA, bootstrap_servers, starting_offsets)
        query = (
            stream.writeStream
            .foreachBatch(lambda df, batch_id: _persist_and_recompute_trends(
                df, batch_id, live_path, output, baseline_df, min_viewers, top_n,
                spark, late_minutes
            ))
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_kafka_q17_trending")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


def run_ads(bootstrap_servers: str, topic: str, checkpoint: str, campaigns_path: str,
            ledger_path: str, cap_state_path: str, spend_state_path: str,
            capped_path: str, alerts_path: str, match_start: str | None = None,
            match_hours: float = 3, starting_offsets: str = "earliest"):
    if match_hours <= 0:
        raise ValueError("match_hours must be positive")
    spark = create_spark("StreamBharat-Kafka-Q18-Ad-Pacing")
    try:
        campaigns = spark.read.option("header", True).option("inferSchema", True).csv(campaigns_path)
        stream = kafka_json_stream(spark, topic, AD_SCHEMA, bootstrap_servers, starting_offsets)
        match_start = match_start or datetime.now().replace(
            hour=18, minute=0, second=0, microsecond=0
        ).isoformat(sep=" ")
        query = (
            stream.writeStream
            .foreachBatch(lambda df, batch_id: _process_ads_batch(
                df, batch_id, spark, campaigns, ledger_path, cap_state_path,
                spend_state_path, capped_path, alerts_path, match_start, match_hours
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
    parser = argparse.ArgumentParser(description="Run a StreamBharat Kafka-to-Spark streaming job")
    parser.add_argument("--stream", choices=["plays", "ads"], required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic")
    parser.add_argument("--checkpoint")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--as-of")
    parser.add_argument("--min-viewers", type=int, default=200)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--late-minutes", type=int, default=15)
    parser.add_argument("--campaigns", default="data/raw/ads/ad_campaigns.csv")
    parser.add_argument("--match-start")
    parser.add_argument("--match-hours", type=float, default=3)
    parser.add_argument("--bronze", default="data/delta/bronze/playback")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--baseline", default="data/delta/reference/trending_baseline")
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
            args.bronze, args.catalog, args.baseline, args.output,
            args.starting_offsets, args.as_of, args.min_viewers, args.top_n, args.late_minutes
        )
    else:
        run_ads(
            args.bootstrap_servers, args.topic or "streambharat.ads",
            args.checkpoint or "data/checkpoints/kafka_q18", args.campaigns,
            args.ledger, args.cap_state, args.spend_state, args.capped, args.alerts,
            args.match_start, args.match_hours, args.starting_offsets
        )


if __name__ == "__main__":
    main()
