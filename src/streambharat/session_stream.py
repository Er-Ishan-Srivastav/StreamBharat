"""Near-real-time player sessionization with 30-minute event-time session windows."""
from __future__ import annotations

import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.monitoring import attach_streaming_metrics
from streambharat.q1_catalog import read_catalog_snapshot
from streambharat.spark_session import create_spark


def build_session_stream(spark, bootstrap_servers, topic, catalog_path,
                         watermark="12 minutes", session_gap="30 minutes",
                         starting_offsets="latest"):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    payload = F.from_json(
        F.col("value").cast("string"),
        MapType(StringType(), StringType()),
    )
    events = raw.select(
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        payload.alias("_payload"),
    )
    events = (
        events
        .withColumn("event_id", F.col("_payload")["event_id"])
        .withColumn("user_id", F.col("_payload")["user_id"])
        .withColumn("profile_id", F.col("_payload")["profile_id"])
        .withColumn("device_id", F.col("_payload")["device_id"])
        .withColumn("content_id", F.col("_payload")["content_id"])
        .withColumn("event_type", F.lower(F.col("_payload")["event_type"]))
        .withColumn("client_ts", F.to_timestamp(F.col("_payload")["client_ts"]))
        .withColumn("server_ts", F.to_timestamp(F.col("_payload")["server_ts"]))
        .withColumn("download_id", F.col("_payload")["download_id"])
        .withColumn(
            "event_ts",
            F.when(
                F.col("download_id").isNotNull() & (F.trim("download_id") != ""),
                F.col("client_ts"),
            ).otherwise(F.col("server_ts")),
        )
        .withColumn(
            "position_sec",
            F.when(
                F.col("_payload")["position_ms"].isNotNull(),
                F.col("_payload")["position_ms"].cast("double") / F.lit(1000.0),
            ).otherwise(F.col("_payload")["position_sec"].cast("double")),
        )
        .filter(
            F.col("event_id").isNotNull()
            & F.col("profile_id").isNotNull()
            & F.col("device_id").isNotNull()
            & F.col("content_id").isNotNull()
            & F.col("event_ts").isNotNull()
            & F.col("event_type").isin(
                "start", "play", "resume", "heartbeat", "pause",
                "seek", "buffer", "stop", "complete", "end",
            )
        )
        .withWatermark("event_ts", watermark)
        .dropDuplicates(["event_id"])
    )

    catalog = read_catalog_snapshot(spark, catalog_path).select(
        "content_id",
        "title",
        "duration_sec",
        "parent_series_id",
        F.element_at("languages", 1).alias("language"),
    ).dropDuplicates(["content_id"])

    return events.join(F.broadcast(catalog), "content_id", "left").groupBy(
        F.session_window(F.col("event_ts"), session_gap).alias("session_window"),
        "user_id", "profile_id", "device_id", "content_id",
        "title", "duration_sec", "parent_series_id", "language",
    ).agg(
        F.min("event_ts").alias("session_start_ts"),
        F.max("event_ts").alias("session_end_ts"),
        F.count("event_id").cast("long").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "heartbeat", 1).otherwise(0))
         .cast("long").alias("heartbeat_count"),
        F.max("position_sec").alias("max_position_sec"),
        F.max(F.when(F.col("event_type") == "complete", 1).otherwise(0))
         .alias("completion_event"),
        F.max("kafka_offset").alias("last_kafka_offset"),
    ).withColumn(
        "session_id",
        F.sha2(
            F.concat_ws(
                "|", "profile_id", "device_id", "content_id",
                F.col("session_start_ts").cast("string"),
            ),
            256,
        ),
    ).withColumn(
        "session_duration_seconds",
        F.greatest(
            F.col("session_end_ts").cast("long")
            - F.col("session_start_ts").cast("long"),
            F.lit(0),
        ),
    ).withColumn(
        "watched_seconds",
        F.least(
            F.col("heartbeat_count") * F.lit(300),
            F.coalesce(F.col("max_position_sec"), F.lit(0.0)),
            F.coalesce(F.col("duration_sec").cast("double"), F.lit(1.0e12)),
            F.col("session_duration_seconds") + F.lit(300),
        ).cast("long"),
    ).withColumn(
        "completed", F.col("completion_event") == 1
    ).withColumn(
        "viewing_date", F.to_date("session_start_ts")
    ).withColumn(
        "report_content_id",
        F.coalesce(F.col("parent_series_id"), F.col("content_id")),
    ).drop("session_window").drop("completion_event")


def merge_sessions(batch_df, batch_id, output_path):
    if batch_df.isEmpty():
        return
    if DeltaTable.isDeltaTable(batch_df.sparkSession, output_path):
        (
            DeltaTable.forPath(batch_df.sparkSession, output_path).alias("target")
            .merge(batch_df.alias("source"), "target.session_id = source.session_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").mode("append").save(output_path)


def run_session_stream(
    bootstrap_servers=None,
    topic="ott.player.events",
    catalog_path="data/raw/content/catalog_2026-10-01.json",
    output_path="lake/silver/viewing_sessions_stream",
    checkpoint="lake/_checkpoints/session_window_stream",
    starting_offsets="latest",
    watermark="12 minutes",
    session_gap="30 minutes",
):
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    spark = create_spark(
        "StreamBharat-RealTime-Viewing-Sessions",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"],
    )
    listener = attach_streaming_metrics(spark, bootstrap_servers)
    try:
        sessions = build_session_stream(
            spark, bootstrap_servers, topic, catalog_path, watermark, session_gap,
            starting_offsets,
        )
        query = (
            sessions.writeStream.outputMode("append")
            .foreachBatch(lambda frame, batch_id: merge_sessions(frame, batch_id, output_path))
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_session_windows")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.streams.removeListener(listener)
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run event-time player sessionization")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="ott.player.events")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--output", default="lake/silver/viewing_sessions_stream")
    parser.add_argument("--checkpoint", default="lake/_checkpoints/session_window_stream")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--watermark", default="12 minutes")
    parser.add_argument("--session-gap", default="30 minutes")
    args = parser.parse_args()
    run_session_stream(
        args.bootstrap_servers, args.topic, args.catalog, args.output,
        args.checkpoint, args.starting_offsets, args.watermark, args.session_gap,
    )


if __name__ == "__main__":
    main()
