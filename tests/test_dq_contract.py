import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

from streambharat.dq import classify_player_quality, classify_subscription_quality


def test_licence_rule_uses_viewer_state_and_event_date():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.master("local[1]").appName(
        "streambharat-dq-license-test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        player_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("profile_id", StringType(), False),
            StructField("content_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_ts_corrected", StringType(), False),
            StructField("position_sec", DoubleType(), False),
            StructField("ip_city", StringType(), False),
        ])
        player = spark.createDataFrame([
            ("E1", "P1", "C1", "heartbeat", "2026-09-15 10:00:00", 100.0, "Amritsar"),
            ("E2", "P1", "C1", "heartbeat", "2026-10-15 10:00:00", 100.0, "Amritsar"),
        ], player_schema)
        titles = spark.createDataFrame(
            [("C1", 1800, "13+")],
            ["content_id", "duration_sec", "age_rating"],
        )
        profiles = spark.createDataFrame(
            [("P1", False)],
            ["profile_id", "is_kids"],
        )
        city_state = spark.createDataFrame(
            [("Amritsar", "Punjab")],
            ["city", "state"],
        )
        licences = spark.createDataFrame(
            [("C1", "Punjab", "2026-09-01", "2026-09-30")],
            ["content_id", "region", "licence_start", "licence_end"],
        )

        violations = classify_player_quality(
            player, titles, profiles, licences, city_state
        )
        # The classifier returns frames in RULES order:
        # UNKNOWN_CONTENT, KIDS_AGE_RESTRICTION, LICENCE_EXPIRED_FOR_STATE, ...
        licence_rows = violations[2]
        assert [row.event_id for row in licence_rows.select("event_id").collect()] == ["E2"]
    finally:
        spark.stop()


def test_charged_downgrade_price_mismatch_is_flagged():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = SparkSession.builder.master("local[1]").appName(
        "streambharat-dq-price-test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bronze = spark.createDataFrame([
            (json.dumps({
                "user_id": "U1",
                "event_type": "downgrade",
                "plan": "basic",
                "price": "199.00",
                "event_ts": "2026-10-01T10:00:00",
            }), 0, 10),
        ], ["raw_value", "kafka_partition", "kafka_offset"])
        plans = spark.createDataFrame(
            [("basic", 99.00)],
            ["plan", "price_inr_per_month"],
        )
        result = classify_subscription_quality(bronze, plans)
        row = result.first()
        assert row.rule_name == "PRICE_PLAN_MISMATCH"
        assert row.severity == "FAIL"
        assert row.price == 199.00
        assert row.reference_price == 99.00
    finally:
        spark.stop()
