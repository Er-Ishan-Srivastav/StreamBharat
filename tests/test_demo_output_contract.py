from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from verify_demo_outputs import DELTA_OUTPUTS, expected_source_rows


def test_demo_verifier_covers_all_seven_bronze_sources():
    expected = expected_source_rows(5)
    assert set(expected) == {
        "bronze_player_events",
        "bronze_subscriptions",
        "bronze_settlements",
        "bronze_search",
        "bronze_ratings",
        "bronze_ad_breaks",
        "bronze_ad_impressions",
        "bronze_dlq",
    }
    assert set(expected).issubset(DELTA_OUTPUTS)


def test_demo_source_minimums_scale_with_user_count():
    assert expected_source_rows(1) == {
        "bronze_player_events": 4,
        "bronze_subscriptions": 3,
        "bronze_settlements": 2,
        "bronze_search": 1,
        "bronze_ratings": 1,
        "bronze_ad_breaks": 1,
        "bronze_ad_impressions": 1,
        "bronze_dlq": 1,
    }
    assert expected_source_rows(10)["bronze_player_events"] == 31


def test_demo_verifier_rejects_invalid_user_count():
    import pytest

    with pytest.raises(ValueError):
        expected_source_rows(0)
    with pytest.raises(ValueError):
        expected_source_rows(101)
