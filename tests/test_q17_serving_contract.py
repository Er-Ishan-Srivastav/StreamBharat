from __future__ import annotations

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streambharat.q17_trending import _merge_trends
from streambharat.serve_postgres import TABLES


def test_q17_delta_output_has_flattened_postgres_serving_columns(spark, tmp_path):
    window_schema = StructType([
        StructField("start", TimestampType(), False),
        StructField("end", TimestampType(), False),
    ])
    batch_schema = StructType([
        StructField("window", window_schema, False),
        StructField("language", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("unique_viewers", LongType(), False),
    ])
    batch = spark.createDataFrame(
        [(
            (datetime(2026, 11, 27, 10, 0), datetime(2026, 11, 27, 10, 5)),
            "Hindi",
            "CONTENT-1",
            12,
        )],
        batch_schema,
    )
    baseline = spark.createDataFrame(
        [("CONTENT-1", "Hindi", 10, 3.0)],
        ["content_id", "language", "hour_of_day", "baseline_avg_5m_viewers"],
    )
    target = str(tmp_path / "trending")

    _merge_trends(
        batch,
        batch_id=1,
        target_path=target,
        baseline_df=baseline,
        min_viewers=1,
        top_n=5,
        spark=spark,
        window_minutes=5,
    )

    result = spark.read.format("delta").load(target)
    required = {
        "window_start", "window_end", "language", "content_id",
        "unique_viewers", "hour_of_day", "baseline_avg_window_viewers",
        "growth_ratio", "rank_in_language", "processed_at",
    }
    assert required.issubset(set(result.columns))
    row = result.first()
    assert row["language"] == "Hindi"
    assert row["content_id"] == "CONTENT-1"
    assert row["unique_viewers"] == 12
    assert row["baseline_avg_window_viewers"] == 3.0
    assert row["rank_in_language"] == 1


def test_q17_postgres_serving_contract_matches_flattened_delta_schema():
    config = TABLES["live_trending"]
    assert config["path"] == "lake/gold/trending"
    assert config["keys"] == [
        "window_start", "language", "rank_in_language"
    ]
    assert config["replace_all"] is True
    assert {
        "window_start", "window_end", "language", "content_id",
        "unique_viewers", "hour_of_day", "baseline_avg_window_viewers",
        "growth_ratio", "rank_in_language", "processed_at",
    }.issubset(set(config["columns"]))
