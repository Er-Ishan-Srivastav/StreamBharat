import os

import pytest
from pyspark.sql.types import MapType, StringType, StructField, StructType

from streambharat.dlq_ingest import REQUIRED_FIELDS, add_reason_code
from streambharat.spark_session import create_spark


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = create_spark("streambharat-dlq-contract-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _validated(spark, alias, rows):
    schema = StructType([
        StructField("_payload", MapType(StringType(), StringType()), True)
    ])
    return [
        row["reason_code"]
        for row in add_reason_code(spark.createDataFrame(rows, schema), alias)
        .select("reason_code")
        .collect()
    ]


def test_all_seven_source_contracts_are_registered():
    assert set(REQUIRED_FIELDS) == {
        "player_events", "billing_subscr", "billing_settle",
        "app_search", "app_ratings", "ads_breaks", "ads_impressions",
    }


def test_player_schema_v1_and_v2_positions_are_accepted(spark):
    common = {
        "event_id": "E1", "user_id": "U1", "profile_id": "U1-P1",
        "device_id": "D1", "content_id": "C1", "event_type": "heartbeat",
        "server_ts": "2026-10-15T10:00:00",
    }
    v1 = {**common, "position_sec": "30"}
    v2 = {**common, "position_ms": "30000", "network": "wifi"}
    assert _validated(spark, "player_events", [(v1,), (v2,)]) == [None, None]


def test_subscription_price_format_is_quarantined(spark):
    valid = {
        "user_id": "U1", "event_type": "renew", "plan": "premium",
        "event_ts": "2026-10-15T10:00:00", "price": "1299.00",
    }
    invalid = {**valid, "price": "1,299.00"}
    missing = {**valid}
    del missing["price"]
    assert _validated(
        spark, "billing_subscr", [(valid,), (invalid,), (missing,)]
    ) == [None, "INVALID_PRICE", "MISSING_PRICE"]


def test_invalid_rating_is_classified(spark):
    valid = {
        "profile_id": "U1-P1", "content_id": "C1",
        "rating": "5", "ts": "2026-10-15T10:00:00",
    }
    invalid = {**valid, "rating": "6"}
    assert _validated(spark, "app_ratings", [(valid,), (invalid,)]) == [
        None, "INVALID_RATING"
    ]


def test_invalid_ad_duration_is_checked_in_addition_to_cpm(spark):
    valid = {
        "impression_id": "I1", "user_id": "U1", "profile_id": "U1-P1",
        "content_id": "C1", "campaign_id": "CMP1",
        "ad_break_ts": "2026-10-15T10:00:00", "duration_sec": "15",
        "completed": "1", "cpm_inr": "250",
    }
    bad_duration = {**valid, "duration_sec": "0"}
    bad_cpm = {**valid, "cpm_inr": "1,299.00"}
    assert _validated(
        spark, "ads_impressions", [(valid,), (bad_duration,), (bad_cpm,)]
    ) == [None, "INVALID_DURATION", "INVALID_CPM_INR"]


def test_unknown_subscription_transition_is_routed_to_dlq(spark):
    valid = {
        "user_id": "U7", "event_type": "downgrade", "plan": "basic",
        "event_ts": "2026-10-15T10:00:00", "price": "99.00",
    }
    invalid = {**valid, "event_type": "mystery_transition"}
    assert _validated(
        spark, "billing_subscr", [(valid,), (invalid,)]
    ) == [None, "UNKNOWN_SUBSCRIPTION_EVENT"]



def test_invalid_player_client_timestamp_is_quarantined(spark):
    valid = {
        "event_id": "E2", "user_id": "U1", "profile_id": "U1-P1",
        "device_id": "D1", "content_id": "C1", "event_type": "heartbeat",
        "server_ts": "2026-10-15T10:00:00", "client_ts": "2026-10-15T09:59:58",
        "position_sec": "30",
    }
    invalid = {**valid, "client_ts": "2026/10/15 09:59:58"}
    assert _validated(spark, "player_events", [(valid,), (invalid,)]) == [
        None, "INVALID_CLIENT_TIMESTAMP"
    ]
