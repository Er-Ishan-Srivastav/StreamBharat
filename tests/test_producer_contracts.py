import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producers"))

import sources


def test_source_producer_uses_all_seven_workbook_topics_and_keys():
    contracts = {source.name: source for source in sources.SOURCES}
    assert set(contracts) == {
        "player", "subscriptions", "settlements", "search",
        "ratings", "ad_breaks", "impressions",
    }
    assert contracts["player"].topic == "ott.player.events"
    assert contracts["player"].key_fields == ("user_id", "profile_id")
    assert contracts["subscriptions"].topic == "ott.billing.subscr"
    assert contracts["settlements"].topic == "ott.billing.settle"
    assert contracts["search"].key_fields == ("profile_id",)
    assert contracts["ratings"].topic == "ott.app.ratings"
    assert contracts["ad_breaks"].topic == "ott.ads.breaks"
    assert contracts["impressions"].topic == "ott.ads.impressions"


def test_android_73_player_schema_switches_at_2026_10_15():
    source = sources.SOURCES[0]
    record = {
        "event_id": "E1",
        "device_id": "D1",
        "device_type": "android",
        "app_version": "7.3.0",
        "position_sec": 12,
    }
    before, version_before = source.version(datetime(2026, 10, 14, 23, 59), record)
    after, version_after = source.version(datetime(2026, 10, 15, 0, 0), record)
    assert version_before == "1"
    assert before["position_sec"] == 12
    assert version_after == "2"
    assert "position_sec" not in after
    assert after["position_ms"] == 12000
    assert after["network"] in {"wifi", "cellular"}
