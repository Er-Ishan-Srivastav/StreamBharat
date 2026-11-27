import json

from streambharat.create_topics import TOPICS as KAFKA_TOPIC_PARTITIONS
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



def test_all_kafka_topics_match_the_supplied_etl_workbook():
    assert KAFKA_TOPIC_PARTITIONS == {
        "ott.player.events": 6,
        "ott.billing.subscr": 3,
        "ott.billing.settle": 3,
        "ott.app.search": 3,
        "ott.app.ratings": 1,
        "ott.ads.breaks": 3,
        "ott.ads.impressions": 3,
        "ott.live.plays": 6,
        "ott.live.ads": 6,
        "ott.dlq": 1,
        "ott.metrics.trending": 1,
        "ott.alerts.ad_pacing": 1,
        "ott.ref.subscription_state": 3,
    }
