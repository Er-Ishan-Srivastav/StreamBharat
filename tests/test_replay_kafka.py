from datetime import datetime, timezone

from streambharat.replay_kafka import SOURCE_SPECS, parse_event_time, record_time


def test_seven_source_topic_and_key_contracts():
    assert len(SOURCE_SPECS) == 7
    assert SOURCE_SPECS["player_events"].topic == "ott.player.events"
    assert SOURCE_SPECS["player_events"].key_field == "user_id"
    assert SOURCE_SPECS["app_search"].key_field == "profile_id"
    assert SOURCE_SPECS["app_ratings"].topic == "ott.app.ratings"
    assert SOURCE_SPECS["ads_impressions"].key_field == "user_id"


def test_parse_event_time_normalizes_utc():
    value = parse_event_time("2026-10-01T05:30:00+05:30")
    assert value == datetime(2026, 10, 1, 0, 0, 0)


def test_impression_is_scheduled_when_the_ad_ends():
    spec = SOURCE_SPECS["ads_impressions"]
    fallback = datetime(2026, 10, 1)
    result = record_time(
        {"ad_break_ts": "2026-10-02T12:00:00", "duration_sec": "30"},
        spec,
        fallback,
    )
    assert result == datetime(2026, 10, 2, 12, 0, 30)


def test_missing_timestamp_falls_back_for_quarantine_replay():
    spec = SOURCE_SPECS["player_events"]
    fallback = datetime(2026, 10, 1)
    assert record_time({"event_id": "bad"}, spec, fallback) == fallback
