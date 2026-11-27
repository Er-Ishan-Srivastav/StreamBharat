import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from streambharat.ad_matching import build_ad_delivery
from streambharat.finance_royalties import (
    build_partner_royalties,
    build_revenue_reconciliation,
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from streambharat.spark_session import create_spark

    session = create_spark("streambharat-ad-finance-contract-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_ad_break_matching_fill_rate_and_campaign_metrics(spark):
    break_schema = StructType([
        StructField("break_id", StringType(), False),
        StructField("impression_id", StringType(), True),
        StructField("user_id", StringType(), False),
        StructField("profile_id", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("break_ts", StringType(), False),
        StructField("filled", IntegerType(), False),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
    ])
    breaks = spark.createDataFrame([
        ("B1", "I1", "U1", "P1", "C1", "2026-10-01 10:00:00", 1, 0, 1),
        ("B2", None, "U2", "P2", "C2", "2026-10-01 10:05:00", 0, 0, 2),
    ], break_schema)

    impression_schema = StructType([
        StructField("impression_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("profile_id", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("campaign_id", StringType(), False),
        StructField("ad_break_ts", StringType(), False),
        StructField("duration_sec", IntegerType(), False),
        StructField("completed", IntegerType(), False),
        StructField("cpm_inr", DoubleType(), False),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
    ])
    impressions = spark.createDataFrame([
        ("I1", "U1", "P1", "C1", "CAMP1", "2026-10-01 10:00:02", 30, 1, 20.0, 0, 5),
        ("I2", "U3", "P3", "C3", "CAMP1", "2026-10-01 10:10:00", 20, 1, 20.0, 0, 6),
    ], impression_schema)

    campaign_schema = StructType([
        StructField("campaign_id", StringType(), False),
        StructField("budget_inr", DoubleType(), False),
        StructField("start_date", StringType(), False),
        StructField("end_date", StringType(), False),
        StructField("daily_frequency_cap", IntegerType(), False),
        StructField("target_genres", StringType(), False),
    ])
    campaigns = spark.createDataFrame([
        ("CAMP1", 3100.0, "2026-10-01", "2026-10-31", 1, "Action|Thriller"),
    ], campaign_schema)
    genre_schema = StructType([
        StructField("content_id", StringType(), False),
        StructField("genres", ArrayType(StringType()), True),
    ])
    genres = spark.createDataFrame([
        ("C1", ["Action"]),
        ("C2", ["Drama"]),
        ("C3", ["Action"]),
    ], genre_schema)

    delivery, campaign_day, hourly = build_ad_delivery(
        breaks, impressions, campaigns, genres
    )
    statuses = {
        row["delivery_status"]: row["delivery_id"]
        for row in delivery.select("delivery_status", "delivery_id").collect()
    }
    assert set(statuses) == {"DELIVERED", "UNFILLED_BREAK", "ORPHAN_IMPRESSION"}
    delivered = delivery.filter(delivery.delivery_status == "DELIVERED").first()
    assert delivered["genre_eligible"] is True
    assert round(float(delivered["spend_inr"]), 4) == 0.02
    daily = campaign_day.first()
    assert daily["impressions"] == 1
    assert daily["orphan_impressions"] == 1
    assert daily["unfilled_breaks"] == 0
    fill = hourly.first()
    assert fill["total_breaks"] == 2
    assert fill["filled_breaks"] == 1
    assert fill["fill_rate"] == 0.5


def test_revenue_reconciliation_matches_once_and_flags_exceptions(spark):
    subscription_schema = StructType([
        StructField("raw_value", StringType(), False),
    ])
    subscription = spark.createDataFrame([
        ('{"event_id":"CH1","user_id":"U1","event_type":"subscribe","price":"100.00","event_ts":"2026-10-01T10:00:00"}',),
        ('{"event_id":"CH2","user_id":"U2","event_type":"renew","price":"200.00","event_ts":"2026-10-02T10:00:00"}',),
        ('{"event_id":"CH3","user_id":"U4","event_type":"downgrade","price":"100.00","event_ts":"2026-10-03T10:00:00"}',),
    ], subscription_schema)
    settlement_schema = StructType([
        StructField("raw_value", StringType(), False),
    ])
    settlements = spark.createDataFrame([
        ('{"gateway":"G1","user_id":"U1","amount_inr":"100.00","settled_ts":"2026-10-01T11:00:00","event_type":"subscribe"}',),
        ('{"gateway":"G1","user_id":"U1","amount_inr":"100.00","settled_ts":"2026-10-01T12:00:00","event_type":"subscribe"}',),
        ('{"gateway":"G2","user_id":"U3","amount_inr":"500.00","settled_ts":"2026-10-01T12:00:00","event_type":"subscribe"}',),
        ('{"gateway":"G3","user_id":"U4","amount_inr":"100.00","settled_ts":"2026-10-03T11:00:00","event_type":"downgrade"}',),
    ], settlement_schema)

    result = build_revenue_reconciliation(subscription, settlements)
    statuses = {
        row["charge_id"]: row["settlement_status"]
        for row in result.select("charge_id", "settlement_status").collect()
    }
    assert statuses["CH1"] == "SETTLED"
    assert statuses["CH2"] == "UNSETTLED_CHARGE"
    assert statuses["CH3"] == "SETTLED"
    assert list(statuses.values()).count("DUPLICATE_SETTLEMENT") == 1
    assert list(statuses.values()).count("ORPHAN_SETTLEMENT") == 1


def test_partner_royalties_use_point_in_time_watch_share_and_minimum(spark):
    watch_schema = StructType([
        StructField("viewing_date", StringType(), False),
        StructField("content_id", StringType(), False),
        StructField("watched_seconds", LongType(), False),
    ])
    watch = spark.createDataFrame([
        ("2026-10-01", "C1", 80),
        ("2026-10-02", "C2", 20),
    ], watch_schema)
    history_schema = StructType([
        StructField("content_id", StringType(), False),
        StructField("partner_id", StringType(), False),
        StructField("valid_from", StringType(), False),
        StructField("valid_to", StringType(), True),
    ])
    history = spark.createDataFrame([
        ("C1", "P1", "2026-07-01", None),
        ("C2", "P2", "2026-07-01", None),
    ], history_schema)
    revenue_schema = StructType([
        StructField("charge_ts", StringType(), True),
        StructField("charge_amount", DoubleType(), True),
        StructField("settlement_status", StringType(), False),
    ])
    revenue = spark.createDataFrame([
        ("2026-10-15 12:00:00", 1000.00, "SETTLED"),
        ("2026-10-15 13:00:00", 900.00, "UNSETTLED_CHARGE"),
    ], revenue_schema)
    partner_schema = StructType([
        StructField("partner_id", StringType(), False),
        StructField("revenue_share_pct", DoubleType(), False),
        StructField("minimum_guarantee_inr_per_quarter", DoubleType(), False),
    ])
    partners = spark.createDataFrame([
        ("P1", 50.0, 100.00),
        ("P2", 25.0, 200.00),
        ("P3", 30.0, 75.00),
    ], partner_schema)

    royalties = build_partner_royalties(watch, history, revenue, partners)
    values = {
        row["partner_id"]: (
            float(row["revenue_share_inr"]),
            float(row["guarantee_topup_inr"]),
            float(row["payout_inr"]),
        )
        for row in royalties.collect()
    }
    assert values["P1"] == (400.0, 0.0, 400.0)
    assert values["P2"] == (50.0, 150.0, 200.0)
    assert values["P3"] == (0.0, 75.0, 75.0)
