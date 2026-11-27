from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate_run_config.py"
SPEC = importlib.util.spec_from_file_location("streambharat_run_config", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_daily_run_must_match_logical_date():
    result = MODULE.validate_run_config(
        "2026-10-15", "false", "2026-10-15", "2026-10-15"
    )
    assert result == {
        "logical_date": "2026-10-15",
        "full_refresh": False,
        "replay_start": "2026-10-15",
        "replay_end": "2026-10-15",
    }


def test_full_refresh_accepts_multi_day_range():
    result = MODULE.validate_run_config(
        "2026-11-27", "TRUE", "2026-10-01", "2026-10-31"
    )
    assert result["full_refresh"] is True
    assert result["replay_start"] == "2026-10-01"
    assert result["replay_end"] == "2026-10-31"


@pytest.mark.parametrize(
    "args",
    [
        ("2026-02-30", "false", "2026-02-30", "2026-02-30"),
        ("2026-10-15", "maybe", "2026-10-15", "2026-10-15"),
        ("2026-10-15", "false", "2026-10-16", "2026-10-16"),
        ("2026-10-15", "true", "2026-10-16", "2026-10-15"),
        ("2026-10-15", "true", "2025-01-01", "2026-10-31"),
    ],
)
def test_invalid_run_configuration_is_rejected(args):
    with pytest.raises(ValueError):
        MODULE.validate_run_config(*args)


def test_non_canonical_dates_are_rejected():
    with pytest.raises(ValueError, match="canonical"):
        MODULE.parse_iso_date("logical_date", "2026-1-5")
