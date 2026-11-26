"""Kafka source validation and dead-letter routing for the seven batch topics."""
from __future__ import annotations

import argparse
import os
from typing import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from streambharat.bronze_ingest import SOURCE_TOPICS
from streambharat.monitoring import attach_streaming_metrics
from streambharat.spark_session import create_spark

# Required fields and event-time columns follow the Kafka ETL workbook's raw
# source contracts. Player position is versioned: v1 uses position_sec and v2
# uses position_ms, so it is validated as an either/or field.
REQUIRED_FIELDS = {
    "player_events": ("event_id", "user_id", "profile_id", "device_id",
                      "content_id", "event_type", "server_ts"),
    "billing_subscr": ("user_id", "event_type", "plan", "event_ts"),
    "billing_settle": ("gateway", "user_id", "amount_inr", "settled_ts", "event_type"),
    "app_search": ("search_id", "profile_id", "query_text", "results_count", "ts"),
    "app_ratings": ("profile_id", "content_id", "rating", "ts"),
    "ads_breaks": ("break_id", "user_id", "profile_id", "content_id", "break_ts", "filled"),
    "ads_impressions": ("impression_id", "user_id", "profile_id", "content_id",
                        "campaign_id", "ad_break_ts", "duration_sec", "completed", "cpm_inr"),
}

TIMESTAMP_FIELDS = {
    "player_events": "server_ts",
    "billing_subscr": "event_ts",
    "billing_settle": "settled_ts",
    "app_search": "ts",
    "app_ratings": "ts",
    "ads_breaks": "break_ts",
    "ads_impressions": "ad_break_ts",
}


def _field(name: str):
    return F.element_at(F.col("_payload"), F.lit(name))


def _missing(name: str):
    value = _field(name)
    return value.isNull() | (F.length(F.trim(value)) == 0)


def add_reason_code(parsed: DataFrame, alias: str) -> DataFrame:
    """Classify malformed, incomplete and selected semantically invalid rows."""
    if alias not in REQUIRED_FIELDS:
        raise ValueError(f"Unknown topic alias: {alias}")

    reason = F.when(F.col("_payload").isNull(), F.lit("MALFORMED_JSON"))
    for name in REQUIRED_FIELDS[alias]:
        reason = reason.when(_missing(name), F.lit("MISSING_" + name.upper()))

    timestamp_field = TIMESTAMP_FIELDS[alias]
    timestamp_invalid = (
        _missing(timestamp_field)
        | F.to_timestamp(_field(timestamp_field)).isNull()
    )
    reason = reason.when(timestamp_invalid, F.lit("INVALID_TIMESTAMP"))

    if alias == "player_events":
        position_sec = _field("position_sec")
        position_ms = _field("position_ms")
        has_sec = position_sec.isNotNull() & (F.length(F.trim(position_sec)) > 0)
        has_ms = position_ms.isNotNull() & (F.length(F.trim(position_ms)) > 0)
        reason = reason.when(~(has_sec | has_ms), F.lit("MISSING_POSITION"))
        bad_sec = has_sec & F.expr(
            "try_cast(element_at(_payload, 'position_sec') AS DOUBLE) IS NULL"
        )
        bad_ms = has_ms & F.expr(
            "try_cast(element_at(_payload, 'position_ms') AS DOUBLE) IS NULL"
        )
        reason = reason.when(bad_sec | bad_ms, F.lit("INVALID_POSITION"))
    elif alias == "billing_settle":
        amount_invalid = F.expr(
            "try_cast(element_at(_payload, 'amount_inr') AS DECIMAL(18,2)) IS NULL"
        ) | (
            F.expr("try_cast(element_at(_payload, 'amount_inr') AS DECIMAL(18,2))") < 0
        )
        reason = reason.when(amount_invalid, F.lit("INVALID_AMOUNT_INR"))
    elif alias == "billing_subscr":
        charged_event = F.lower(_field("event_type")).isin(
            "subscribe", "renew", "upgrade", "downgrade"
        )
        price_missing = charged_event & _missing("price")
        price_invalid = F.expr(
            "try_cast(element_at(_payload, 'price') AS DECIMAL(18,2)) IS NULL"
        ) | (
            F.expr("try_cast(element_at(_payload, 'price') AS DECIMAL(18,2))") < 0
        )
        reason = reason.when(price_missing, F.lit("MISSING_PRICE"))
        reason = reason.when(
            charged_event & ~_missing("price") & price_invalid,
            F.lit("INVALID_PRICE"),
        )
    elif alias == "app_ratings":
        rating = F.expr("try_cast(element_at(_payload, 'rating') AS DOUBLE)")
        reason = reason.when(
            rating.isNull() | (rating < 1) | (rating > 5),
            F.lit("INVALID_RATING"),
        )

    if alias == "ads_impressions":
        cpm = F.expr("try_cast(element_at(_payload, 'cpm_inr') AS DECIMAL(18,2))")
        amount_invalid = cpm.isNull() | (cpm < 0)
        reason = reason.when(amount_invalid, F.lit("INVALID_CPM_INR"))
        duration = F.expr("try_cast(element_at(_payload, 'duration_sec') AS INT)")
        duration_invalid = duration.isNull() | (duration <= 0)
        reason = reason.when(duration_invalid, F.lit("INVALID_DURATION"))

    if alias == "ads_breaks":
        filled = F.expr("try_cast(element_at(_payload, 'filled') AS INT)")
        reason = reason.when(
            filled.isNull() | ~filled.isin(0, 1),
            F.lit("INVALID_FILLED_FLAG"),
        )

    return parsed.withColumn("reason_code", reason)


def build_validation_stream(
    spark,
    topic: str,
    alias: str,
    bootstrap_servers: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Read topic values as raw strings, then validate a scalar JSON object map."""
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    parsed = source.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.to_json(F.col("headers")).alias("kafka_headers_json"),
    ).withColumn(
        "_payload",
        F.from_json(F.col("raw_value"), MapType(StringType(), StringType())),
    )
    return add_reason_code(parsed, alias)


def _merge_dlq(delta_path: str, invalid: DataFrame) -> None:
    if DeltaTable.isDeltaTable(invalid.sparkSession, delta_path):
        (
            DeltaTable.forPath(invalid.sparkSession, delta_path).alias("target")
            .merge(invalid.alias("source"), "target.dlq_id = source.dlq_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        invalid.write.format("delta").mode("append").save(delta_path)


def start_topic_validation(
    spark,
    alias: str,
    topic: str,
    bootstrap_servers: str,
    delta_path: str,
    checkpoint_root: str,
    starting_offsets: str = "earliest",
    trigger_mode: str = "processing",
):
    stream = build_validation_stream(
        spark, topic, alias, bootstrap_servers, starting_offsets
    )
    checkpoint = os.path.join(checkpoint_root, "dlq_" + alias)

    def route_invalid(batch_df: DataFrame, batch_id: int) -> None:
        invalid = (
            batch_df.filter(F.col("reason_code").isNotNull())
            .select(
                F.concat_ws(
                    ":", F.col("source_topic"), F.col("kafka_partition"),
                    F.col("kafka_offset"),
                ).alias("dlq_id"),
                "source_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "kafka_key",
                "kafka_headers_json",
                "raw_value",
                "reason_code",
            )
            .withColumn("batch_id", F.lit(str(batch_id)))
            .withColumn("quarantined_at", F.current_timestamp())
        )
        if invalid.isEmpty():
            return

        # The stable source offset is the idempotency key for the Delta DLQ.
        _merge_dlq(delta_path, invalid)

        # Re-publishing the same DLQ key is safe for downstream compaction or
        # deduplication; the Kafka key is topic:partition:offset.
        kafka_rows = invalid.select(
            F.col("dlq_id").cast("string").alias("key"),
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

    writer = (
        stream.writeStream.foreachBatch(route_invalid)
        .option("checkpointLocation", checkpoint)
        .queryName("streambharat_dlq_" + alias)
    )
    if trigger_mode == "availableNow":
        return writer.trigger(availableNow=True).start()
    if trigger_mode == "processing":
        return writer.trigger(processingTime="10 seconds").start()
    raise ValueError("trigger_mode must be availableNow or processing")


def run_dlq(
    topics: Iterable[str] | None = None,
    bootstrap_servers: str = "localhost:9092",
    delta_path: str = "lake/bronze/dlq",
    checkpoint_root: str = "lake/_checkpoints",
    starting_offsets: str = "earliest",
    trigger_mode: str = "processing",
) -> None:
    selected = list(topics) if topics else list(SOURCE_TOPICS)
    invalid = set(selected) - set(SOURCE_TOPICS)
    if invalid:
        raise ValueError(f"Unknown DLQ topic aliases: {sorted(invalid)}")
    spark = create_spark("StreamBharat-Kafka-DLQ")
    metrics_listener = attach_streaming_metrics(spark, bootstrap_servers)
    queries = []
    try:
        for alias in selected:
            queries.append(
                start_topic_validation(
                    spark, alias, SOURCE_TOPICS[alias], bootstrap_servers,
                    delta_path, checkpoint_root, starting_offsets, trigger_mode,
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
        description="Validate seven Kafka source topics and route invalid records to ott.dlq"
    )
    parser.add_argument("--topics", help="Comma-separated aliases; default all seven")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--delta-path", default="lake/bronze/dlq")
    parser.add_argument("--checkpoint-root", default="lake/_checkpoints")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    aliases = [v.strip() for v in args.topics.split(",") if v.strip()] if args.topics else None
    run_dlq(
        aliases,
        args.bootstrap_servers,
        args.delta_path,
        args.checkpoint_root,
        args.starting_offsets,
        "availableNow" if args.available_now else "processing",
    )


if __name__ == "__main__":
    main()
