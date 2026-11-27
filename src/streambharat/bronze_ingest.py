"""Append-only Kafka-to-Delta Bronze ingestion with one query per topic."""
from __future__ import annotations

import argparse
import os
from typing import Iterable

from pyspark.sql import functions as F

from streambharat.monitoring import attach_streaming_metrics
from streambharat.spark_session import create_spark

SOURCE_TOPICS = {
    "player_events": "ott.player.events",
    "billing_subscr": "ott.billing.subscr",
    "billing_settle": "ott.billing.settle",
    "app_search": "ott.app.search",
    "app_ratings": "ott.app.ratings",
    "ads_breaks": "ott.ads.breaks",
    "ads_impressions": "ott.ads.impressions",
}


def build_bronze_stream(
    spark,
    topic: str,
    bootstrap_servers: str,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 50000,
):
    """Read the raw Kafka envelope without parsing or changing its payload."""
    if max_offsets_per_trigger < 1:
        raise ValueError("max_offsets_per_trigger must be positive")
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("headers").alias("kafka_headers"),
            F.to_json(F.col("headers")).alias("kafka_headers_json"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingested_at"))
    )


def start_topic_query(
    spark,
    topic_name: str,
    topic: str,
    bootstrap_servers: str,
    base_path: str,
    checkpoint_root: str,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 50000,
    trigger_mode: str = "processing",
):
    output_path = os.path.join(base_path, topic_name)
    checkpoint = os.path.join(checkpoint_root, "bronze_" + topic_name)
    stream = build_bronze_stream(
        spark, topic, bootstrap_servers, starting_offsets, max_offsets_per_trigger
    )
    writer = (
        stream.writeStream.format("delta")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("ingestion_date")
        .queryName("streambharat_bronze_" + topic_name)
    )
    if trigger_mode == "availableNow":
        return writer.trigger(availableNow=True).start()
    if trigger_mode == "processing":
        return writer.trigger(processingTime="10 seconds").start()
    raise ValueError("trigger_mode must be availableNow or processing")


def run_bronze(
    topics: Iterable[str] | None = None,
    bootstrap_servers: str = "localhost:9092",
    base_path: str = "lake/bronze",
    checkpoint_root: str = "lake/_checkpoints",
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int = 50000,
    trigger_mode: str = "processing",
) -> None:
    selected = list(topics) if topics else list(SOURCE_TOPICS)
    invalid = set(selected) - set(SOURCE_TOPICS)
    if invalid:
        raise ValueError(f"Unknown Bronze topic aliases: {sorted(invalid)}")
    spark = create_spark("StreamBharat-Kafka-Bronze-Ingestion")
    metrics_listener = attach_streaming_metrics(spark, bootstrap_servers)
    queries = []
    try:
        for alias in selected:
            queries.append(
                start_topic_query(
                    spark,
                    alias,
                    SOURCE_TOPICS[alias],
                    bootstrap_servers,
                    base_path,
                    checkpoint_root,
                    starting_offsets,
                    max_offsets_per_trigger,
                    trigger_mode,
                )
            )
        if trigger_mode == "availableNow":
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.streams.removeListener(metrics_listener)
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest all seven source topics into append-only Bronze Delta tables"
    )
    parser.add_argument("--topics", help="Comma-separated aliases; default all seven")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--base-path", default="lake/bronze")
    parser.add_argument("--checkpoint-root", default="lake/_checkpoints")
    parser.add_argument(
        "--starting-offsets", choices=["earliest", "latest"], default="earliest"
    )
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50000)
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    aliases = [v.strip() for v in args.topics.split(",") if v.strip()] if args.topics else None
    run_bronze(
        aliases,
        args.bootstrap_servers,
        args.base_path,
        args.checkpoint_root,
        args.starting_offsets,
        args.max_offsets_per_trigger,
        "availableNow" if args.available_now else "processing",
    )


if __name__ == "__main__":
    main()
