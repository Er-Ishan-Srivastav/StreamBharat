import json
import os
from datetime import datetime

from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from streambharat.silver_player import transform_player_events
from streambharat.spark_session import create_spark


def test_online_clock_offset_is_corrected_and_offline_time_is_preserved():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-player-clock-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        schema = StructType([
            StructField("raw_value", StringType(), False),
            StructField("source_topic", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
        ])
        rows = [
            (
                json.dumps({
                    "event_id": "ONLINE-1",
                    "user_id": "U1",
                    "profile_id": "P1",
                    "device_id": "D1",
                    "content_id": "C1",
                    "event_type": "start",
                    "position_sec": 0,
                    "client_ts": "2026-08-04T10:00:00",
                    "server_ts": "2026-08-04T10:05:00",
                    "download_id": None,
                }),
                "ott.player.events", 0, 1,
                datetime(2026, 8, 4, 10, 5), datetime(2026, 8, 4, 10, 5),
            ),
            (
                json.dumps({
                    "event_id": "OFFLINE-1",
                    "user_id": "U1",
                    "profile_id": "P1",
                    "device_id": "D1",
                    "content_id": "C1",
                    "event_type": "start",
                    "position_sec": 0,
                    "client_ts": "2026-08-01T08:00:00",
                    "server_ts": "2026-08-04T10:05:00",
                    "download_id": "DOWNLOAD-1",
                }),
                "ott.player.events", 0, 2,
                datetime(2026, 8, 4, 10, 5), datetime(2026, 8, 4, 10, 5),
            ),
        ]
        bronze = spark.createDataFrame(rows, schema)
        conformed, offsets = transform_player_events(bronze)
        by_id = {
            row["event_id"]: row
            for row in conformed.select(
                "event_id", "client_time", "server_time", "event_ts_corrected"
            ).collect()
        }
        assert by_id["ONLINE-1"]["event_ts_corrected"] == datetime(2026, 8, 4, 10, 5)
        assert by_id["OFFLINE-1"]["event_ts_corrected"] == datetime(2026, 8, 1, 8, 0)
        assert offsets.first()["median_clock_offset_seconds"] == -300
    finally:
        spark.stop()
