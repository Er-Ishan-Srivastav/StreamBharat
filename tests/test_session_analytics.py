import os

from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from streambharat.session_analytics import build_viewing_sessions
from streambharat.spark_session import create_spark


def test_30_minute_sessions_credit_heartbeats_and_exclude_seek_overrun():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-session-analytics-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        player_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("profile_id", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("content_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("position_sec", DoubleType(), True),
            StructField("event_ts_corrected", StringType(), False),
        ])
        rows = [
            ("E1", "U1", "P1", "D1", "C1", "start", 0.0, "2026-10-01 10:00:00"),
            ("E2", "U1", "P1", "D1", "C1", "heartbeat", 60.0, "2026-10-01 10:01:00"),
            ("E3", "U1", "P1", "D1", "C1", "heartbeat", 120.0, "2026-10-01 10:02:00"),
            ("E4", "U1", "P1", "D1", "C1", "pause", 120.0, "2026-10-01 10:02:30"),
            ("E5", "U1", "P1", "D1", "C1", "start", 0.0, "2026-10-01 10:40:00"),
            ("E6", "U1", "P1", "D1", "C1", "heartbeat", 60.0, "2026-10-01 10:41:00"),
        ]
        player = spark.createDataFrame(rows, player_schema)

        catalog_schema = StructType([
            StructField("content_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("duration_sec", IntegerType(), True),
            StructField("parent_series_id", StringType(), True),
            StructField("languages", ArrayType(StringType()), True),
        ])
        catalog = spark.createDataFrame(
            [("C1", "Episode One", 1800, "S1", ["Hindi"])],
            catalog_schema,
        )

        sessions, dropoff = build_viewing_sessions(player, catalog)
        rows = sessions.orderBy("session_start_ts").collect()

        assert len(rows) == 2
        assert [row["watched_seconds"] for row in rows] == [120, 60]
        assert rows[0]["report_content_id"] == "S1"
        assert rows[0]["completion_rate"] == 120 / 1800
        assert all(row["completed"] is False for row in rows)

        summary = dropoff.first()
        assert summary["sessions"] == 2
        assert summary["viewers"] == 1
        assert summary["completed_sessions"] == 0
    finally:
        spark.stop()
