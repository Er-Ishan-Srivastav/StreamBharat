"""Structured Streaming progress, Kafka partition lag and event freshness metrics."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

LOG = logging.getLogger("streambharat.monitoring")

METRIC_SCHEMA = StructType([
    StructField("metric_ts", TimestampType(), False),
    StructField("query_name", StringType(), False),
    StructField("topic", StringType(), True),
    StructField("batch_id", LongType(), True),
    StructField("input_rows_per_second", DoubleType(), True),
    StructField("processed_rows_per_second", DoubleType(), True),
    StructField("batch_duration_ms", LongType(), True),
    StructField("event_time_max", TimestampType(), True),
    StructField("consumer_lag", LongType(), True),
    StructField("freshness_seconds", DoubleType(), True),
])


def parse_end_offsets(value: Any) -> list[tuple[str, int, int]]:
    """Parse Spark Kafka endOffset JSON into (topic, partition, next_offset) rows."""
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    rows = []
    if not isinstance(value, dict):
        return rows
    for topic, partitions in value.items():
        if not isinstance(partitions, dict):
            continue
        for partition, offset in partitions.items():
            try:
                rows.append((str(topic), int(partition), int(offset)))
            except (TypeError, ValueError):
                continue
    return rows


def _parse_timestamp(value: Any):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except (TypeError, ValueError):
        return None


class StreamingMetricsListener(StreamingQueryListener):
    """Append one progress row per Kafka topic/partition to a Delta metrics table.

    Spark's Kafka source records offsets in its streaming progress, not a normal
    Kafka consumer group. Lag is therefore computed as broker high watermark
    minus Spark's processed end offset for that topic/partition.
    """

    def __init__(
        self,
        spark,
        bootstrap_servers: str,
        output_path: str = "lake/gold/stream_metrics",
    ):
        super().__init__()
        self._spark_session = spark
        self.bootstrap_servers = bootstrap_servers
        self.output_path = output_path

    def onQueryStarted(self, event) -> None:
        LOG.info("Streaming query started: %s", getattr(event, "name", event.id))

    def onQueryProgress(self, event) -> None:
        try:
            progress = event.progress
            offsets = []
            for source in progress.sources:
                offsets.extend(parse_end_offsets(source.endOffset))
            if not offsets:
                return

            from confluent_kafka import Consumer, TopicPartition

            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "streambharat-stream-metrics",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
            })
            try:
                metric_ts = _parse_timestamp(progress.timestamp) or datetime.now(timezone.utc).replace(tzinfo=None)
                event_time = _parse_timestamp(
                    (progress.eventTime or {}).get("max")
                )
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                freshness = (
                    max((now - event_time).total_seconds(), 0.0)
                    if event_time is not None else None
                )
                duration_map = progress.durationMs or {}
                trigger_duration = int(duration_map.get("triggerExecution", 0) or 0)
                query_name = progress.name or str(progress.id)
                topic_lags: dict[str, int] = {}
                topic_lag_complete: dict[str, bool] = {}
                for topic, partition, processed_end_offset in offsets:
                    try:
                        _, high_watermark = consumer.get_watermark_offsets(
                            TopicPartition(topic, partition),
                            timeout=3,
                            cached=False,
                        )
                        lag = max(int(high_watermark) - processed_end_offset, 0)
                        topic_lags[topic] = topic_lags.get(topic, 0) + lag
                        topic_lag_complete[topic] = topic_lag_complete.get(topic, True)
                    except Exception as exc:  # broker metadata may be unavailable during recovery
                        LOG.warning(
                            "Could not obtain Kafka watermark for %s-%s: %s",
                            topic, partition, exc,
                        )
                        topic_lag_complete[topic] = False

                rows = []
                for topic, lag_sum in topic_lags.items():
                    rows.append((
                        metric_ts,
                        query_name,
                        topic,
                        int(progress.batchId),
                        float(progress.inputRowsPerSecond or 0.0),
                        float(progress.processedRowsPerSecond or 0.0),
                        trigger_duration,
                        event_time,
                        lag_sum if topic_lag_complete.get(topic, False) else None,
                        freshness,
                    ))
            finally:
                consumer.close()

            if rows:
                self._spark_session.createDataFrame(rows, METRIC_SCHEMA).write.format(
                    "delta"
                ).mode("append").save(self.output_path)
        except Exception:
            # Monitoring must not terminate the business stream if metrics cannot be written.
            LOG.exception("Failed to persist Structured Streaming progress metrics")

    def onQueryTerminated(self, event) -> None:
        LOG.info("Streaming query terminated: %s", getattr(event, "id", "unknown"))

    def onQueryIdle(self, event) -> None:
        return None


def attach_streaming_metrics(
    spark,
    bootstrap_servers: str | None = None,
    output_path: str = "lake/gold/stream_metrics",
) -> StreamingMetricsListener:
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    listener = StreamingMetricsListener(spark, servers, output_path)
    spark.streams.addListener(listener)
    return listener
