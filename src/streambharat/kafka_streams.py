"""Kafka Structured Streaming adapters for Q17/Q18 and live DLQ routing."""
from __future__ import annotations

import argparse
import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

from streambharat.q17_trending import (
    PLAYS_SCHEMA,
    _persist_and_recompute_trends,
    build_historical_baseline,
)
from streambharat.q18_live_ad_pacing import AD_SCHEMA, _process_ads_batch
from streambharat.monitoring import attach_streaming_metrics
from streambharat.spark_session import create_spark

LIVE_REQUIRED_FIELDS = {
    "plays": (
        "event_id", "user_id", "profile_id", "content_id", "language", "event_ts"
    ),
    "ads": (
        "impression_id", "user_id", "campaign_id", "ad_break_ts",
        "duration_sec", "cpm_inr",
    ),
}


def _blank_or_null(column):
    return column.isNull() | (
        (column.cast("string").isNotNull())
        & (F.length(F.trim(column.cast("string"))) == 0)
    )


def _merge_live_dlq(spark, invalid: DataFrame, delta_path: str) -> None:
    if DeltaTable.isDeltaTable(spark, delta_path):
        (
            DeltaTable.forPath(spark, delta_path)
            .alias("target")
            .merge(invalid.alias("source"), "target.dlq_id = source.dlq_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        invalid.write.format("delta").mode("append").save(delta_path)


def kafka_json_stream(
    spark,
    topic: str,
    schema: StructType,
    bootstrap_servers: str,
    stream_alias: str,
    starting_offsets: str = "earliest",
    dlq_path: str | None = None,
    dlq_checkpoint: str | None = None,
):
    """Parse one live stream, validate required fields and return (valid, DLQ query)."""
    if stream_alias not in LIVE_REQUIRED_FIELDS:
        raise ValueError(f"Unknown live stream alias: {stream_alias}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    decoded = (
        raw.select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("value").cast("string").alias("_json"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.to_json(F.col("headers")).alias("_kafka_headers_json"),
        )
        .withColumn("_payload", F.from_json(F.col("_json"), schema))
    )

    reason = F.when(F.col("_payload").isNull(), F.lit("MALFORMED_JSON"))
    for field in LIVE_REQUIRED_FIELDS[stream_alias]:
        value = F.col("_payload." + field)
        reason = reason.when(
            _blank_or_null(value),
            F.lit("MISSING_OR_INVALID_" + field.upper()),
        )

    timestamp_field = "event_ts" if stream_alias == "plays" else "ad_break_ts"
    reason = reason.when(
        F.to_timestamp(F.col("_payload." + timestamp_field)).isNull(),
        F.lit("INVALID_EVENT_TIMESTAMP"),
    )
    if stream_alias == "ads":
        duration = F.col("_payload.duration_sec")
        cpm = F.col("_payload.cpm_inr")
        reason = reason.when(
            duration.isNull() | (duration <= 0),
            F.lit("INVALID_DURATION"),
        ).when(
            cpm.isNull() | (cpm < 0),
            F.lit("INVALID_CPM_INR"),
        )

    decoded = decoded.withColumn("_reason_code", reason)
    invalid_path = dlq_path or os.getenv(
        "KAFKA_LIVE_DLQ_PATH", "lake/bronze/dlq/live_events"
    )
    checkpoint = dlq_checkpoint or os.getenv(
        "KAFKA_LIVE_DLQ_CHECKPOINT",
        "lake/_checkpoints/dlq_live_" + stream_alias,
    )

    def quarantine_invalid(batch_df: DataFrame, batch_id: int) -> None:
        invalid = (
            batch_df.filter(F.col("_reason_code").isNotNull())
            .select(
                F.concat_ws(
                    ":",
                    F.col("_kafka_topic"),
                    F.col("_kafka_partition"),
                    F.col("_kafka_offset"),
                ).alias("dlq_id"),
                F.col("_kafka_topic").alias("source_topic"),
                F.col("_kafka_partition").alias("kafka_partition"),
                F.col("_kafka_offset").alias("kafka_offset"),
                F.col("_kafka_timestamp").alias("kafka_timestamp"),
                F.col("_kafka_key").alias("kafka_key"),
                F.col("_kafka_headers_json").alias("kafka_headers_json"),
                F.col("_json").alias("raw_value"),
                F.col("_reason_code").alias("reason_code"),
            )
            .withColumn("batch_id", F.lit(str(batch_id)))
            .withColumn("quarantined_at", F.current_timestamp())
        )
        if invalid.isEmpty():
            return

        _merge_live_dlq(spark, invalid, invalid_path)
        kafka_rows = invalid.select(
            F.col("dlq_id").alias("key"),
            F.to_json(
                F.struct(
                    "dlq_id", "source_topic", "kafka_partition", "kafka_offset",
                    "kafka_timestamp", "kafka_key", "kafka_headers_json",
                    "raw_value", "reason_code", "batch_id", "quarantined_at",
                )
            ).alias("value"),
        )
        (
            kafka_rows.write.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", "ott.dlq")
            .save()
        )

    invalid_query = (
        decoded.filter(F.col("_reason_code").isNotNull())
        .writeStream.foreachBatch(quarantine_invalid)
        .option("checkpointLocation", checkpoint)
        .queryName("streambharat_live_dlq_" + stream_alias)
        .trigger(processingTime="5 seconds")
        .start()
    )
    valid = decoded.filter(F.col("_reason_code").isNull()).select(
        "_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset",
        "_kafka_timestamp", "_kafka_headers_json", "_json", "_payload.*",
    )
    return valid, invalid_query


def run_plays(
    bootstrap_servers: str,
    topic: str,
    checkpoint: str,
    bronze: str,
    catalog: str,
    baseline: str,
    output: str,
    starting_offsets: str = "earliest",
    as_of: str = "2026-11-01",
    min_viewers: int = 1,
    top_n: int = 10,
    late_minutes: int = 12,
    window_minutes: int = 5,
    slide_seconds: int = 30,
    output_topic: str = "ott.metrics.trending",
):
    if min_viewers < 1 or top_n < 1 or late_minutes < 0:
        raise ValueError("min_viewers/top_n must be positive; late_minutes cannot be negative")
    if window_minutes < 1 or slide_seconds < 1:
        raise ValueError("window_minutes and slide_seconds must be positive")

    spark = create_spark("StreamBharat-Kafka-Q17-Trending")
    metrics_listener = attach_streaming_metrics(spark, bootstrap_servers)
    queries = []
    try:
        build_historical_baseline(
            spark, bronze, catalog, baseline, as_of, window_minutes=window_minutes
        )
        baseline_df = spark.read.format("delta").load(baseline)
        live_path = os.path.join(os.path.dirname(output), "_kafka_live_play_events")
        stream, dlq_query = kafka_json_stream(
            spark, topic, PLAYS_SCHEMA, bootstrap_servers,
            "plays", starting_offsets,
        )
        queries.append(dlq_query)
        query = (
            stream.writeStream
            .foreachBatch(
                lambda df, batch_id: _persist_and_recompute_trends(
                    df,
                    batch_id,
                    live_path,
                    output,
                    baseline_df,
                    min_viewers,
                    top_n,
                    spark,
                    late_minutes,
                    window_minutes,
                    f"{slide_seconds} seconds",
                    bootstrap_servers,
                    output_topic,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_kafka_q17_trending")
            .trigger(processingTime="5 seconds")
            .start()
        )
        queries.append(query)
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.streams.removeListener(metrics_listener)
        spark.stop()


def run_ads(
    bootstrap_servers: str,
    topic: str,
    checkpoint: str,
    campaigns_path: str,
    ledger_path: str,
    cap_state_path: str,
    spend_state_path: str,
    capped_path: str,
    alerts_path: str,
    match_start: str | None = None,
    match_hours: float = 3,
    starting_offsets: str = "earliest",
    output_topic: str = "ott.alerts.ad_pacing",
):
    if match_hours <= 0:
        raise ValueError("match_hours must be positive")

    spark = create_spark("StreamBharat-Kafka-Q18-Ad-Pacing")
    metrics_listener = attach_streaming_metrics(spark, bootstrap_servers)
    queries = []
    try:
        campaigns = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(campaigns_path)
        )
        stream, dlq_query = kafka_json_stream(
            spark, topic, AD_SCHEMA, bootstrap_servers, "ads", starting_offsets
        )
        queries.append(dlq_query)
        match_start = match_start or datetime.now().replace(
            hour=18, minute=0, second=0, microsecond=0
        ).isoformat(sep=" ")
        query = (
            stream.writeStream
            .foreachBatch(
                lambda df, batch_id: _process_ads_batch(
                    df,
                    batch_id,
                    spark,
                    campaigns,
                    ledger_path,
                    cap_state_path,
                    spend_state_path,
                    capped_path,
                    alerts_path,
                    match_start,
                    match_hours,
                    bootstrap_servers,
                    output_topic,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("streambharat_kafka_q18_ad_pacing")
            .trigger(processingTime="5 seconds")
            .start()
        )
        queries.append(query)
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a StreamBharat Kafka-to-Spark streaming job"
    )
    parser.add_argument("--stream", choices=["plays", "ads"], required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic")
    parser.add_argument("--checkpoint")
    parser.add_argument(
        "--starting-offsets", choices=["earliest", "latest"], default="earliest"
    )
    parser.add_argument("--as-of", default="2026-11-01")
    parser.add_argument("--min-viewers", type=int, default=1)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--late-minutes", type=int, default=12)
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--slide-seconds", type=int, default=30)
    parser.add_argument("--output-topic", default="ott.metrics.trending")
    parser.add_argument("--campaigns", default="data/raw/ads/ad_campaigns.csv")
    parser.add_argument("--match-start")
    parser.add_argument("--match-hours", type=float, default=3)
    parser.add_argument("--bronze", default="lake/silver/player_events")
    parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
    parser.add_argument("--baseline", default="lake/gold/trending_baseline")
    parser.add_argument("--output", default="lake/gold/trending")
    parser.add_argument("--ledger", default="lake/silver/live_ad_impression_ledger")
    parser.add_argument("--cap-state", default="lake/silver/live_ad_caps_state")
    parser.add_argument("--spend-state", default="lake/silver/live_ad_spend_state")
    parser.add_argument("--capped", default="lake/gold/capped_users_live")
    parser.add_argument("--alerts", default="lake/gold/pacing_alerts_live")
    args = parser.parse_args()

    if args.stream == "plays":
        run_plays(
            args.bootstrap_servers,
            args.topic or "ott.live.plays",
            args.checkpoint or "lake/_checkpoints/kafka_q17",
            args.bronze,
            args.catalog,
            args.baseline,
            args.output,
            args.starting_offsets,
            args.as_of,
            args.min_viewers,
            args.top_n,
            args.late_minutes,
            args.window_minutes,
            args.slide_seconds,
            args.output_topic,
        )
    else:
        run_ads(
            args.bootstrap_servers,
            args.topic or "ott.live.ads",
            args.checkpoint or "lake/_checkpoints/kafka_q18",
            args.campaigns,
            args.ledger,
            args.cap_state,
            args.spend_state,
            args.capped,
            args.alerts,
            args.match_start,
            args.match_hours,
            args.starting_offsets,
            "ott.alerts.ad_pacing",
        )


if __name__ == "__main__":
    main()
