import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from streambharat.dlq_metrics import build_dlq_metrics


def test_dlq_metrics_group_by_source_topic_reason_and_day():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.master("local[1]").appName(
        "streambharat-dlq-metrics-test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        schema = StructType([
            StructField("dlq_id", StringType(), False),
            StructField("source_topic", StringType(), False),
            StructField("kafka_partition", IntegerType(), False),
            StructField("kafka_offset", LongType(), False),
            StructField("reason_code", StringType(), False),
            StructField("quarantined_at", TimestampType(), False),
        ])
        frame = spark.createDataFrame([
            ("A", "ott.player.events", 0, 10, "MALFORMED_JSON", datetime(2026, 10, 1, 10)),
            ("B", "ott.player.events", 1, 11, "MALFORMED_JSON", datetime(2026, 10, 1, 11)),
            ("C", "ott.billing.subscr", 0, 12, "UNKNOWN_SUBSCRIPTION_EVENT", datetime(2026, 10, 1, 12)),
        ], schema)
        rows = {
            (row.source_topic, row.reason_code): row
            for row in build_dlq_metrics(frame).collect()
        }
        player = rows[("ott.player.events", "MALFORMED_JSON")]
        assert player.invalid_records == 2
        assert player.affected_partitions == 2
        assert player.first_offset == 10
        assert player.last_offset == 11
        assert rows[("ott.billing.subscr", "UNKNOWN_SUBSCRIPTION_EVENT")].invalid_records == 1
    finally:
        spark.stop()
