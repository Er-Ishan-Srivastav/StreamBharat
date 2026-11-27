from __future__ import annotations

import json
from datetime import date

from scripts.generate_demo_data import generate


def test_synthetic_demo_generator_writes_replayable_sources(tmp_path):
    counts = generate(tmp_path, date(2026, 10, 1), users=2)

    assert counts["player_events"] == 7
    assert counts["subscription_events"] == 6
    assert counts["ad_breaks"] == 2
    assert counts["ad_impressions"] == 2

    player_path = tmp_path / "player" / "playback_2026-10-01.json"
    player_rows = [json.loads(line) for line in player_path.read_text().splitlines()]
    invalid = next(row for row in player_rows if row["event_id"].endswith("PLAY-INVALID"))
    assert invalid["position_sec"] is None
    assert invalid["position_ms"] is None
    assert invalid["device_type"] == "ios"

    subscription_path = tmp_path / "billing" / "subscription_events.json"
    subscription_rows = [
        json.loads(line) for line in subscription_path.read_text().splitlines()
    ]
    assert all(row["event_ts"].startswith("2026-10-01") for row in subscription_rows)

    catalogue = json.loads(
        (tmp_path / "content" / "catalog_2026-10-01.json").read_text()
    )
    assert catalogue[0]["content_id"] == "DEMO-CONTENT-001"
