"""Reconcile Bronze offsets against the currently retained Kafka offset range."""
from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from .bronze_ingest import SOURCE_TOPICS
from .spark_session import create_spark


def read_offset_ranges(bootstrap_servers: str, topics: list[str]):
    """Return the broker's current [low, high) offset range per partition."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "streambharat-offset-audit",
        "enable.auto.commit": False,
    })
    metadata = consumer.list_topics(timeout=20)
    rows = []
    try:
        for topic in topics:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None:
                raise RuntimeError(f"Kafka topic does not exist: {topic}")
            for partition_id in topic_meta.partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=20, cached=False
                )
                rows.append((topic, partition_id, low, high, max(high - low, 0)))
    finally:
        consumer.close()
    return rows


def reconcile(
    bootstrap_servers: str = "localhost:9092",
    bronze_root: str = "lake/bronze",
    output_path: str = "lake/gold/bronze_offset_reconciliation",
    topics: list[str] | None = None,
):
    selected = topics or list(SOURCE_TOPICS)
    invalid = set(selected) - set(SOURCE_TOPICS)
    if invalid:
        raise ValueError(f"Unknown topic aliases: {sorted(invalid)}")

    kafka_topics = [SOURCE_TOPICS[name] for name in selected]
    offset_rows = read_offset_ranges(bootstrap_servers, kafka_topics)
    spark = create_spark("StreamBharat-Bronze-Offset-Reconciliation")
    try:
        ranges = spark.createDataFrame(
            offset_rows,
            [
                "source_topic", "kafka_partition", "low_offset",
                "high_offset", "retained_messages",
            ],
        )

        bronze_frames = []
        for alias in selected:
            path = os.path.join(bronze_root, alias)
            if os.path.exists(path):
                bronze_frames.append(
                    spark.read.format("delta").load(path).select(
                        "source_topic", "kafka_partition", "kafka_offset"
                    )
                )
        if bronze_frames:
            bronze = bronze_frames[0]
            for frame in bronze_frames[1:]:
                bronze = bronze.unionByName(frame)
        else:
            bronze = spark.createDataFrame(
                [],
                "source_topic string, kafka_partition int, kafka_offset long",
            )

        retained = bronze.join(
            ranges.select("source_topic", "kafka_partition", "low_offset", "high_offset"),
            ["source_topic", "kafka_partition"],
            "inner",
        ).filter(
            (F.col("kafka_offset") >= F.col("low_offset"))
            & (F.col("kafka_offset") < F.col("high_offset"))
        )
        observed = retained.groupBy("source_topic", "kafka_partition").agg(
            F.count(F.lit(1)).alias("bronze_retained_rows"),
            F.min("kafka_offset").alias("bronze_first_retained_offset"),
            F.max("kafka_offset").alias("bronze_last_retained_offset"),
        )

        report = (
            ranges.join(observed, ["source_topic", "kafka_partition"], "left")
            .fillna({"bronze_retained_rows": 0})
            .withColumn(
                "count_delta",
                F.col("bronze_retained_rows") - F.col("retained_messages"),
            )
            .withColumn(
                "reconciled",
                F.col("count_delta") == 0,
            )
            .withColumn(
                "retention_has_advanced",
                F.col("low_offset") > 0,
            )
            .withColumn("checked_at", F.current_timestamp())
        )
        report.write.format("delta").mode("overwrite").save(output_path)
        report.orderBy("source_topic", "kafka_partition").show(100, False)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--bronze-root", default="lake/bronze")
    parser.add_argument("--output", default="lake/gold/bronze_offset_reconciliation")
    parser.add_argument("--topics", help="Comma-separated Bronze aliases")
    args = parser.parse_args()
    aliases = [value.strip() for value in args.topics.split(",") if value.strip()] if args.topics else None
    reconcile(args.bootstrap_servers, args.bronze_root, args.output, aliases)


if __name__ == "__main__":
    main()
