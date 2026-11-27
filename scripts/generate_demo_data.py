#!/usr/bin/env python3
"""Create a tiny, deterministic OTT dataset for the local Kafka demo.

These records are synthetic and are not a substitute for the course dataset.
The generator writes only the source feeds needed by the self-contained demo.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta
from pathlib import Path


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def generate(data_root: Path, demo_date: date, users: int) -> dict[str, int]:
    day = demo_date.isoformat()
    player = []
    subscriptions = []
    settlements = []
    searches = []
    ratings = []
    breaks = []
    impressions = []

    for number in range(1, users + 1):
        user = f"DEMO-U{number:03d}"
        profile = f"DEMO-P{number:03d}"
        device = f"DEMO-D{number:03d}"
        content = "DEMO-CONTENT-001"
        base = datetime.combine(demo_date, datetime.min.time()) + timedelta(hours=10, minutes=number)
        for index, (event_type, position) in enumerate(
            [("start", 0), ("heartbeat", 300), ("complete", 600)]
        ):
            server_time = base + timedelta(minutes=index * 5)
            client_time = server_time - timedelta(seconds=30)
            event = {
                "event_id": f"{user}-PLAY-{index + 1}",
                "user_id": user,
                "profile_id": profile,
                "device_id": device,
                "content_id": content,
                "event_type": event_type,
                "position_sec": position,
                "client_ts": client_time.isoformat(timespec="seconds"),
                "server_ts": server_time.isoformat(timespec="seconds"),
                "ip_city": "Mumbai",
                "device_type": "android",
                "app_version": "7.3.0",
            }
            player.append(event)

        # A deliberately invalid record proves that DLQ validation is active.
        if number == 1:
            server_time = base + timedelta(minutes=20)
            player.append({
                "event_id": f"{user}-PLAY-INVALID",
                "user_id": user,
                "profile_id": profile,
                "device_id": device,
                "content_id": content,
                "event_type": "heartbeat",
                "position_sec": None,
                "position_ms": None,
                "client_ts": server_time.isoformat(timespec="seconds"),
                "server_ts": server_time.isoformat(timespec="seconds"),
                "ip_city": "Mumbai",
                "device_type": "ios",
                "app_version": "6.0",
            })

        subscriptions.extend([
            {
                "event_id": f"{user}-SUB-1", "user_id": user,
                "event_type": "subscribe", "plan": "premium", "price": "299.00",
                "event_ts": (base - timedelta(minutes=30)).isoformat(timespec="seconds"),
            },
            {
                "event_id": f"{user}-SUB-2", "user_id": user,
                "event_type": "payment_failed", "plan": "premium", "price": "299.00",
                "event_ts": (base - timedelta(minutes=20)).isoformat(timespec="seconds"),
            },
            {
                "event_id": f"{user}-SUB-3", "user_id": user,
                "event_type": "renew", "plan": "premium", "price": "299.00",
                "event_ts": (base - timedelta(minutes=10)).isoformat(timespec="seconds"),
            },
        ])
        settlements.extend([
            {
                "gateway": "demo_gateway", "user_id": user,
                "charge_id": f"{user}-CHARGE-1", "settlement_id": f"{user}-SETTLE-1",
                "amount_inr": "299.00",
                "settled_ts": (base - timedelta(minutes=25)).isoformat(timespec="seconds"),
                "event_type": "subscribe", "status": "success",
            },
            {
                "gateway": "demo_gateway", "user_id": user,
                "charge_id": f"{user}-CHARGE-2", "settlement_id": f"{user}-SETTLE-2",
                "amount_inr": "299.00",
                "settled_ts": (base - timedelta(minutes=5)).isoformat(timespec="seconds"),
                "event_type": "renew", "status": "success",
            },
        ])
        searches.append({
            "search_id": f"{user}-SEARCH-1", "profile_id": profile,
            "query_text": "thriller", "results_count": 4,
            "ts": base.isoformat(timespec="seconds"),
        })
        ratings.append({
            "profile_id": profile, "content_id": content,
            "rating": 4 if number != 1 else 6,
            "ts": (base + timedelta(minutes=30)).isoformat(timespec="seconds"),
        })
        break_id = f"{user}-BREAK-1"
        campaign = "DEMO-CAMPAIGN-001"
        breaks.append({
            "break_id": break_id, "impression_id": f"{user}-IMP-1",
            "user_id": user, "profile_id": profile,
            "content_id": content, "break_ts": (base + timedelta(minutes=35)).isoformat(timespec="seconds"),
            "filled": 1,
        })
        impressions.append({
            "impression_id": f"{user}-IMP-1", "user_id": user,
            "profile_id": profile, "content_id": content,
            "campaign_id": campaign,
            "ad_break_ts": (base + timedelta(minutes=35)).isoformat(timespec="seconds"),
            "duration_sec": 15, "completed": 1, "cpm_inr": "120.00",
        })

    write_jsonl(data_root / "player" / f"playback_{day}.json", player)
    write_jsonl(data_root / "billing" / "subscription_events.json", subscriptions)
    write_csv(
        data_root / "billing" / "gateway_settlements.csv", settlements,
        ["gateway", "user_id", "charge_id", "settlement_id", "amount_inr", "settled_ts", "event_type", "status"],
    )
    write_jsonl(data_root / "search" / "search_logs.json", searches)
    write_jsonl(data_root / "app" / "ratings.json", ratings)
    write_csv(
        data_root / "ads" / "ad_breaks.csv", breaks,
        ["break_id", "user_id", "profile_id", "content_id", "break_ts", "filled"],
    )
    write_csv(
        data_root / "ads" / "ad_impressions.csv", impressions,
        ["impression_id", "user_id", "profile_id", "content_id", "campaign_id", "ad_break_ts", "duration_sec", "completed", "cpm_inr"],
    )

    campaigns = [{
        "campaign_id": "DEMO-CAMPAIGN-001",
        "budget_inr": "1000.00",
        "start_date": day,
        "end_date": day,
        "daily_frequency_cap": 3,
        "target_genres": "Thriller|Drama",
    }]
    write_csv(
        data_root / "ads" / "ad_campaigns.csv",
        campaigns,
        ["campaign_id", "budget_inr", "start_date", "end_date", "daily_frequency_cap", "target_genres"],
    )

    partners = [{
        "partner_id": "DEMO-PARTNER",
        "name": "StreamBharat Demo Studios",
        "revenue_share_pct": "30.0",
        "minimum_guarantee_inr_per_quarter": "100.00",
    }]
    reference_root = data_root / "reference"
    write_csv(
        reference_root / "content_partners.csv",
        partners,
        ["partner_id", "name", "revenue_share_pct", "minimum_guarantee_inr_per_quarter"],
    )
    genre_mapping = [
        {"raw_genre": "Thriller", "standard_genre": "Thriller"},
        {"raw_genre": "Drama", "standard_genre": "Drama"},
    ]
    write_csv(
        reference_root / "genre_mapping.csv",
        genre_mapping,
        ["raw_genre", "standard_genre"],
    )

    catalogue = {
        "content_id": "DEMO-CONTENT-001",
        "title": "The Monsoon Files",
        "type": "movie",
        "languages": ["Hindi"],
        "genres": ["Thriller", "Drama"],
        "cast": [{"name": "Demo Actor", "role": "Lead"}],
        "release_date": day,
        "age_rating": "U/A 13+",
        "partner_id": "DEMO-PARTNER",
        "tier": "premium",
        "duration_sec": 600,
        "seasons": [],
    }
    catalogue_path = data_root / "content" / f"catalog_{day}.json"
    catalogue_path.parent.mkdir(parents=True, exist_ok=True)
    catalogue_path.write_text(json.dumps(catalogue, indent=2), encoding="utf-8")

    return {
        "player_events": len(player),
        "subscription_events": len(subscriptions),
        "settlements": len(settlements),
        "campaigns": len(campaigns),
        "content_partners": len(partners),
        "search_events": len(searches),
        "ratings": len(ratings),
        "ad_breaks": len(breaks),
        "ad_impressions": len(impressions),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/raw")
    parser.add_argument("--date", default="2026-10-15")
    parser.add_argument("--users", type=int, default=5)
    args = parser.parse_args()
    if args.users < 1 or args.users > 100:
        parser.error("--users must be between 1 and 100")
    counts = generate(Path(args.data_root), date.fromisoformat(args.date), args.users)
    print("SYNTHETIC DEMO DATA GENERATED (not the supplied course dataset)")
    for name, count in counts.items():
        print(f"  {name}: {count}")
    print(f"  output: {Path(args.data_root).resolve()}")


if __name__ == "__main__":
    main()
