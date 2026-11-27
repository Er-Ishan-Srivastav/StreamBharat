"""Fail fast when a required StreamBharat source dataset file is missing."""
from __future__ import annotations

import argparse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
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
    ("October content catalogue", "content/catalog_2026-10-01.json"),
    ("licences", "content/licences.csv"),
    ("genre mapping", "reference/genre_mapping.csv"),
    ("city-to-state mapping", "reference/city_state.csv"),
    ("plans", "reference/plans.csv"),
    ("content partners", "reference/content_partners.csv"),
    ("CDN edge mapping", "reference/cdn_edges.csv"),
    ("CDN regional rate card", "reference/cdn_rates.csv"),
    ("published board metrics", "reference/published_metrics.csv"),
    ("ad campaigns", "ads/ad_campaigns.csv"),
    ("AkashCDN September log", "cdn/akashcdn_2026-09.log"),
    ("NimbusEdge September log", "cdn/nimbusedge_2026-09.tsv"),
)


MONTHLY_CATALOGS = tuple(
    f"content/catalog_2026-{month:02d}-01.json"
    for month in range(5, 11)
)
PLAYER_ARRIVAL_MONTHS = ("2026-07", "2026-08", "2026-09", "2026-10")


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
    alias_file = REPO_ROOT / "config" / "quality" / "published_metric_aliases.csv"
    if not alias_file.is_file():
        errors.append(f"published metric alias file missing: {alias_file}")
    for relative in MONTHLY_CATALOGS:
        if not (root / relative).is_file():
            errors.append(f"monthly catalogue snapshot missing: {root / relative}")
    for month in PLAYER_ARRIVAL_MONTHS:
        matches = list((root / "player").glob(f"playback_{month}-*.json"))
        if not matches:
            errors.append(
                f"historical/live player arrival files missing for {month} in {root / 'player'}"
            )
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
