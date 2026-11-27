import json
import os
from datetime import datetime

from streambharat.spark_session import create_spark
from streambharat.subscription_state import (
    build_subscription_history,
    parse_subscription_events,
)


def test_subscription_scd2_grace_period_and_churn_classification():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-subscription-state-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        raw_events = [
            ("U1", "trial_started", "basic", "2026-10-01T09:00:00", "E1"),
            ("U1", "payment_failed", "basic", "2026-10-10T09:00:00", "E2"),
            ("U1", "renewal", "premium", "2026-10-14T09:00:00", "E3"),
            ("U2", "subscribe", "premium", "2026-10-01T10:00:00", "E4"),
            ("U2", "payment_failed", "premium", "2026-10-10T10:00:00", "E5"),
            ("U3", "subscribe", "basic", "2026-10-01T11:00:00", "E6"),
            ("U3", "cancelled", "basic", "2026-10-05T11:00:00", "E7"),
        ]
        rows = []
        for index, (user_id, event_type, plan, event_ts, event_id) in enumerate(raw_events):
            value = {
                "user_id": user_id,
                "event_type": event_type,
                "plan": plan,
                "event_ts": event_ts,
                "event_id": event_id,
            }
            rows.append({
                "raw_value": json.dumps(value),
                "source_topic": "ott.billing.subscr",
                "kafka_partition": 0,
                "kafka_offset": index,
                "kafka_timestamp": datetime.fromisoformat(event_ts),
            })
        bronze = spark.createDataFrame(rows)
        events, invalid = parse_subscription_events(bronze)
        assert invalid.count() == 0

        history = build_subscription_history(events)
        states = {
            user: [
                (row.status, row.churn_type, row.is_synthetic, row.valid_from)
                for row in group
            ]
            for user, group in (
                (u, history.filter(history.user_id == u).orderBy("valid_from").collect())
                for u in ("U1", "U2", "U3")
            )
        }

        assert [item[0] for item in states["U1"]] == [
            "trial", "payment_failed", "active"
        ]
        assert all(item[1] is None for item in states["U1"])

        u2_last = states["U2"][-1]
        assert u2_last[0] == "cancelled"
        assert u2_last[1] == "INVOLUNTARY"
        assert u2_last[2] is True
        assert str(u2_last[3]) == "2026-10-17 10:00:00"

        u3_last = states["U3"][-1]
        assert u3_last[0] == "cancelled"
        assert u3_last[1] == "VOLUNTARY"
        assert u3_last[2] is False
    finally:
        spark.stop()


def test_downgrade_keeps_subscription_active_and_updates_plan():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = create_spark("streambharat-downgrade-state-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows = [
            {
                "raw_value": json.dumps({
                    "user_id": "U4",
                    "event_type": "subscribe",
                    "plan": "premium",
                    "event_ts": "2026-10-01T09:00:00",
                    "event_id": "E40",
                }),
                "source_topic": "ott.billing.subscr",
                "kafka_partition": 0,
                "kafka_offset": 40,
                "kafka_timestamp": datetime(2026, 10, 1, 9, 0),
            },
            {
                "raw_value": json.dumps({
                    "user_id": "U4",
                    "event_type": "downgrade",
                    "plan": "basic",
                    "event_ts": "2026-10-05T09:00:00",
                    "event_id": "E41",
                }),
                "source_topic": "ott.billing.subscr",
                "kafka_partition": 0,
                "kafka_offset": 41,
                "kafka_timestamp": datetime(2026, 10, 5, 9, 0),
            },
        ]
        events, invalid = parse_subscription_events(spark.createDataFrame(rows))
        assert invalid.count() == 0
        history = build_subscription_history(events).orderBy("valid_from").collect()
        assert [row.status for row in history] == ["active", "active"]
        assert [row.plan for row in history] == ["premium", "basic"]
        assert history[-1].is_current is True
    finally:
        spark.stop()
