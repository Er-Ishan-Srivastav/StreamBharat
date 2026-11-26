import json
import os
from datetime import datetime

from pyspark.sql import functions as F

from streambharat.gold_analytics import build_engagement_gold
from streambharat.spark_session import create_spark


def test_gold_engagement_outputs_watch_time_dau_mau_and_top_titles(tmp_path):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-gold-analytics-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        events = [
            {
                "event_id": "E1", "user_id": "U1", "profile_id": "P1",
                "device_id": "D1", "content_id": "C1", "event_type": "start",
                "position_sec": 0, "event_ts_corrected": "2026-10-01 10:00:00",
            },
            {
                "event_id": "E2", "user_id": "U1", "profile_id": "P1",
                "device_id": "D1", "content_id": "C1", "event_type": "heartbeat",
                "position_sec": 60, "event_ts_corrected": "2026-10-01 10:01:00",
            },
            {
                "event_id": "E3", "user_id": "U1", "profile_id": "P1",
                "device_id": "D1", "content_id": "C1", "event_type": "heartbeat",
                "position_sec": 120, "event_ts_corrected": "2026-10-01 10:02:00",
            },
            {
                "event_id": "E4", "user_id": "U1", "profile_id": "P1",
                "device_id": "D1", "content_id": "C1", "event_type": "pause",
                "position_sec": 120, "event_ts_corrected": "2026-10-01 10:02:30",
            },
        ]
        player_path = str(tmp_path / "silver_player")
        spark.createDataFrame(events).write.format("delta").save(player_path)

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps([
            {
                "content_id": "C1",
                "title": "Example Film",
                "type": "movie",
                "languages": ["Hindi"],
                "genres": ["Drama"],
                "cast": [{"name": "Actor A", "role": "lead"}],
                "release_date": "2025-01-01",
                "age_rating": "13+",
                "partner_id": "PARTNER_1",
                "tier": "premium",
                "duration_sec": 1800,
                "seasons": [],
            }
        ]), encoding="utf-8")

        gold_root = str(tmp_path / "gold")
        build_engagement_gold(spark, player_path, str(catalog_path), gold_root)

        watch = spark.read.format("delta").load(f"{gold_root}/watch_time_daily")
        active = spark.read.format("delta").load(f"{gold_root}/active_users_daily")
        top = spark.read.format("delta").load(f"{gold_root}/top_titles_daily")

        watch_row = watch.first()
        assert watch_row["watched_seconds"] == 120
        assert round(watch_row["completion_rate"], 4) == 0.0667

        counts = {
            row["metric_type"]: row["active_users"]
            for row in active.collect()
        }
        assert counts == {"DAU": 1, "MAU": 1}

        top_row = top.first()
        assert top_row["language"] == "Hindi"
        assert top_row["title"] == "Example Film"
        assert top_row["viewers"] == 1
    finally:
        spark.stop()
