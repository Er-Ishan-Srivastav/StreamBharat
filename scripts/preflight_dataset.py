"""Fail fast when a required StreamBharat source dataset file is missing."""
from __future__ import annotations

import argparse
from pathlib import Path


REQUIRED = {
    "player_events": ("player", "playback_*.json"),
    "billing_subscr": ("billing", "subscription_events.json"),
    "billing_settle": ("billing", "gateway_settlements.csv"),
    "app_search": ("search", "search_logs.json"),
    "app_ratings": ("app", "ratings.json"),
    "ads_breaks": ("ads", "ad_breaks.csv"),
    "ads_impressions": ("ads", "ad_impressions.csv"),
}
ADDITIONAL = (
    ("users", "app/users.json"),
    ("profiles", "app/profiles.json"),
    ("content catalogue", "content/catalog_2026-10-01.json"),
    ("genre mapping", "reference/genre_mapping.csv"),
    ("ad campaigns", "ads/ad_campaigns.csv"),
)


def validate_dataset(data_root: str = "data/raw") -> list[str]:
    root = Path(data_root)
    errors = []
    if not root.is_dir():
        return [f"Dataset folder not found: {root}"]
    for alias, (folder, pattern) in REQUIRED.items():
        matches = list((root / folder).glob(pattern))
        if not matches:
            errors.append(f"{alias}: expected {root / folder / pattern}")
    for label, relative in ADDITIONAL:
        if not (root / relative).is_file():
            errors.append(f"{label}: expected {root / relative}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data/raw")
    args = parser.parse_args()
    errors = validate_dataset(args.data_root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        raise SystemExit(1)
    print(f"Dataset preflight passed: {args.data_root}")


if __name__ == "__main__":
    main()
