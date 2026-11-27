from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path


GENERATOR_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_demo_data.py"
SPEC = importlib.util.spec_from_file_location("streambharat_demo_data_generator", GENERATOR_PATH)
GENERATOR = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(GENERATOR)
generate = GENERATOR.generate

PRODUCERS_PATH = Path(__file__).resolve().parents[1] / "producers"
sys.path.insert(0, str(PRODUCERS_PATH))
import sources as replay_sources
from replay_engine import Context


def test_synthetic_demo_generator_writes_replayable_sources(tmp_path):
    counts = generate(tmp_path, date(2026, 10, 15), users=2)

    assert counts["player_events"] == 7
    assert counts["subscription_events"] == 6
    assert counts["settlements"] == 4
    assert counts["campaigns"] == 1
    assert counts["content_partners"] == 1
    assert counts["ad_breaks"] == 2
    assert counts["ad_impressions"] == 2

    player_path = tmp_path / "player" / "playback_2026-10-15.json"
    player_rows = [json.loads(line) for line in player_path.read_text().splitlines()]
    invalid = next(row for row in player_rows if row["event_id"].endswith("PLAY-INVALID"))
    assert invalid["position_sec"] is None
    assert invalid["position_ms"] is None
    assert invalid["device_type"] == "ios"
    valid = next(row for row in player_rows if row["event_type"] == "start")
    assert valid["device_type"] == "android"
    assert valid["app_version"].startswith("7.3")

    subscription_path = tmp_path / "billing" / "subscription_events.json"
    subscription_rows = [
        json.loads(line) for line in subscription_path.read_text().splitlines()
    ]
    assert all(row["event_ts"].startswith("2026-10-15") for row in subscription_rows)

    settlements_path = tmp_path / "billing" / "gateway_settlements.csv"
    import csv
    with settlements_path.open(encoding="utf-8", newline="") as handle:
        settlement_rows = list(csv.DictReader(handle))
    assert {row["event_type"] for row in settlement_rows} == {"subscribe", "renew"}
    assert all(row["amount_inr"] == "299.00" for row in settlement_rows)

    breaks_path = tmp_path / "ads" / "ad_breaks.csv"
    with breaks_path.open(encoding="utf-8", newline="") as handle:
        break_rows = list(csv.DictReader(handle))
    assert all(row["impression_id"] for row in break_rows)

    campaign_path = tmp_path / "ads" / "ad_campaigns.csv"
    with campaign_path.open(encoding="utf-8", newline="") as handle:
        campaign_rows = list(csv.DictReader(handle))
    assert len(campaign_rows) == 1
    assert float(campaign_rows[0]["cpm_inr"]) == 120.0
    assert campaign_rows[0]["start_date"] <= "2026-10-15" <= campaign_rows[0]["end_date"]
    partner_path = tmp_path / "reference" / "content_partners.csv"
    assert partner_path.is_file()
    assert (tmp_path / "reference" / "genre_mapping.csv").is_file()

    catalogue = json.loads(
        (tmp_path / "content" / "catalog_2026-10-15.json").read_text()
    )
    assert catalogue["content_id"] == "DEMO-CONTENT-001"


def test_synthetic_feed_files_are_readable_by_all_seven_producer_sources(tmp_path):
    demo_date = date(2026, 10, 15)
    generate(tmp_path, demo_date, users=2)
    context = Context(str(tmp_path), demo_date, demo_date)

    expected_counts = {
        "player": 7,
        "subscriptions": 6,
        "settlements": 4,
        "search": 2,
        "ratings": 2,
        "ad_breaks": 2,
        "impressions": 2,
    }
    for source in replay_sources.SOURCES:
        rows = list(source.reader(context))
        assert len(rows) == expected_counts[source.name], source.name
        assert all(len(row) == 3 for row in rows), source.name
