from datetime import datetime, timezone

import pytest

from streambharat.replay_kafka import (
    SOURCE_SPECS,
    mark_replay_record_published,
    open_replay_state,
    parse_event_time,
    prepare_kafka_payload,
    record_time,
    replay_record_published,
)


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



def test_replay_ledger_skips_acknowledged_source_record(tmp_path):
    state = open_replay_state(str(tmp_path / "replay.sqlite3"))
    try:
        assert not replay_record_published(
            state, "ott.player.events", "player/day.json", 12, '{"event_id":"E12"}'
        )
        mark_replay_record_published(
            state, "ott.player.events", "player/day.json", 12,
            '{"event_id":"E12"}', 2, 101,
        )
        state.commit()
        assert replay_record_published(
            state, "ott.player.events", "player/day.json", 12, '{"event_id":"E12"}'
        )
    finally:
        state.close()


def test_replay_ledger_rejects_mutated_source_row(tmp_path):
    state = open_replay_state(str(tmp_path / "replay.sqlite3"))
    try:
        mark_replay_record_published(
            state, "ott.player.events", "player/day.json", 12,
            '{"event_id":"E12"}', 2, 101,
        )
        state.commit()
        with pytest.raises(RuntimeError, match="Source row changed"):
            replay_record_published(
                state, "ott.player.events", "player/day.json", 12,
                '{"event_id":"E12","position_sec":5}',
            )
    finally:
        state.close()



def test_android_73_player_events_switch_to_schema_v2_on_october_15():
    raw = '{"event_id":"E1","device_id":"D1","device_type":"android","app_version":"7.3.0","position_sec":12}'
    payload, version = prepare_kafka_payload(
        "player_events", raw, datetime(2026, 10, 15, 0, 0, 0)
    )
    parsed = __import__("json").loads(payload)
    assert version == "2"
    assert "position_sec" not in parsed
    assert parsed["position_ms"] == 12000
    assert parsed["network"] in {"wifi", "cellular"}


def test_non_android_and_pre_rollout_player_events_remain_schema_v1():
    raw = '{"event_id":"E1","device_id":"D1","device_type":"android","app_version":"7.2.9","position_sec":12}'
    payload, version = prepare_kafka_payload(
        "player_events", raw, datetime(2026, 10, 14, 23, 59, 59)
    )
    assert version == "1"
    assert payload == raw
