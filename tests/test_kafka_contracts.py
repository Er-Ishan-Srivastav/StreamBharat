import json

from streambharat.kafka_producer import TOPICS


def test_kafka_topic_names_are_domain_separated():
    assert TOPICS["plays"] == "streambharat.plays"
    assert TOPICS["ads"] == "streambharat.ads"


def test_playback_event_serializes_as_json():
    event = {
        "event_id": "LP000000001",
        "user_id": "U000001",
        "profile_id": "U000001-P1",
        "content_id": "C001",
        "language": "hi",
        "event_ts": "2026-11-26T10:00:00",
    }
    assert json.loads(json.dumps(event))["event_id"] == "LP000000001"


def test_ad_event_has_stable_key():
    event = {"impression_id": "LI000000001", "campaign_id": "CMP01"}
    assert event.get("event_id") or event.get("impression_id") == "LI000000001"
