import json

from streambharat.kafka_producer import TOPICS, event_files, event_identity


def test_live_topic_names_match_platform_contract():
    assert TOPICS["plays"] == "ott.live.plays"
    assert TOPICS["ads"] == "ott.live.ads"


def test_event_files_ignores_temporary_simulator_files(tmp_path):
    (tmp_path / "plays_20261126_120000.json").write_text('{"event_id":"e1"}\n')
    (tmp_path / ".tmp_partial.json").write_text('{"event_id":"e2"}\n')
    assert [p.name for p in event_files(tmp_path)] == ["plays_20261126_120000.json"]


def test_event_identity_uses_event_or_impression_id():
    assert event_identity({"event_id": "LP0001"}) == "LP0001"
    assert event_identity({"impression_id": "LI0001"}) == "LI0001"
    assert event_identity({"event_id": ""}) is None


def test_play_and_ad_payloads_have_stable_partition_keys():
    play = {"event_id": "LP000000001", "user_id": "U1-P1"}
    ad = {"impression_id": "LI000000001", "user_id": "U1"}
    assert json.loads(json.dumps(play))["user_id"] == "U1-P1"
    assert json.loads(json.dumps(ad))["user_id"] == "U1"
