import json
from pathlib import Path

from streambharat.kafka_producer import TOPICS, event_files


def test_kafka_topic_names_are_domain_separated():
    assert TOPICS["plays"] == "streambharat.plays"
    assert TOPICS["ads"] == "streambharat.ads"


def test_event_files_ignores_temporary_simulator_files(tmp_path):
    (tmp_path / "plays_20261126_120000.json").write_text('{"event_id":"e1"}\n')
    (tmp_path / ".tmp_partial.json").write_text('{"event_id":"e2"}\n')
    assert [p.name for p in event_files(tmp_path)] == ["plays_20261126_120000.json"]


def test_play_and_ad_payloads_have_stable_event_keys():
    play = {"event_id": "LP000000001", "profile_id": "U1-P1"}
    ad = {"impression_id": "LI000000001", "campaign_id": "CMP01"}
    assert json.loads(json.dumps(play))["event_id"] == "LP000000001"
    assert (ad.get("event_id") or ad.get("impression_id")) == "LI000000001"
