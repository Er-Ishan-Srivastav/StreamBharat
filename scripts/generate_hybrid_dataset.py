#!/usr/bin/env python3
"""Generate a deterministic hybrid OTT dataset from a public Wikidata catalogue.

Public inputs are used only for content metadata. All account, profile, playback,
subscription, settlement, search, rating, ad and CDN records are generated test
data. The manifest records this distinction and hashes every produced source file.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import json
import random
import shutil
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

GENERATOR_VERSION = "1.0.0"
STATES = [
    ("Mumbai", "Maharashtra", "mum"),
    ("Pune", "Maharashtra", "pnq"),
    ("Delhi", "Delhi", "del"),
    ("Bengaluru", "Karnataka", "blr"),
    ("Hyderabad", "Telangana", "hyd"),
    ("Chennai", "Tamil Nadu", "maa"),
    ("Ahmedabad", "Gujarat", "amd"),
    ("Jaipur", "Rajasthan", "jai"),
    ("Kolkata", "West Bengal", "ccu"),
    ("Lucknow", "Uttar Pradesh", "lko"),
]
DEVICES = ["android", "ios", "web", "smart_tv"]
APP_VERSIONS = ["7.3.1", "7.4.0", "7.4.2", "8.0.0"]
PLANS = {"basic": 199.0, "premium": 399.0}
GENRES_FALLBACK = ["Drama", "Comedy", "Action", "Thriller", "Documentary"]
SEARCH_TERMS = ["new releases", "popular", "family", "crime", "romance", "sports", "comedy"]


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    return count


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def iso(value: datetime) -> str:
    # The simulated event clock is UTC. Keep the zone explicit in source files.
    return value.isoformat(timespec="seconds") + "Z"


def month_starts(start: date, end: date) -> list[date]:
    cursor = date(start.year, start.month, 1)
    values = []
    while cursor <= end:
        values.append(cursor)
        cursor = date(cursor.year + (cursor.month == 12), 1 if cursor.month == 12 else cursor.month + 1, 1)
    return values


def load_public_catalog(path: Path) -> tuple[dict, list[dict]]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Public catalogue not found: {path}. Run scripts/fetch_wikidata_catalog.py first."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("license") != "CC0-1.0":
        raise ValueError("Catalogue source must declare the expected CC0-1.0 licence.")
    rows = payload.get("records")
    if not isinstance(rows, list) or len(rows) < 10:
        raise ValueError("Catalogue must contain at least 10 normalized public records.")

    unique = {}
    for row in rows:
        qid = str(row.get("wikidata_qid", "")).strip()
        title = str(row.get("title", "")).strip()
        release = str(row.get("release_date", "")).strip()
        if qid and title and release:
            unique.setdefault(qid, row)
    records = list(unique.values())
    if len(records) < 10:
        raise ValueError("Catalogue contains fewer than 10 usable, unique Wikidata titles.")
    return payload, records


def build_catalog(records: list[dict], rng: random.Random) -> list[dict]:
    partners = [f"PARTNER-{number:02d}" for number in range(1, 9)]
    age_ratings = ["U", "7+", "13+", "16+", "18+"]
    catalog = []
    ordered = sorted(
        records,
        key=lambda row: (row.get("release_date", ""), row.get("wikidata_qid", "")),
        reverse=True,
    )
    for index, record in enumerate(ordered, start=1):
        runtime_minutes = record.get("runtime_minutes")
        try:
            duration = int(float(runtime_minutes) * 60) if runtime_minutes else 0
        except (TypeError, ValueError):
            duration = 0
        if not 900 <= duration <= 24000:
            duration = rng.randint(75, 150) * 60

        genres = [x for x in record.get("genres", []) if x]
        languages = [x for x in record.get("original_languages", []) if x]
        content_id = f"CNT{index:05d}"
        catalog.append(
            {
                "content_id": content_id,
                "title": record["title"],
                "type": "movie",
                "languages": languages or ["Unknown"],
                "genres": genres or ["Unclassified"],
                "cast": [],
                "release_date": record["release_date"],
                "age_rating": rng.choice(age_ratings),
                "partner_id": rng.choice(partners),
                "tier": rng.choices(["free", "premium"], weights=[35, 65], k=1)[0],
                "duration_sec": duration,
                "seasons": [],
                "wikidata_qid": record["wikidata_qid"],
                "wikipedia_title": record.get("wikipedia_title"),
                "source_url": record.get("source_url"),
                "external_source": "Wikidata",
                "external_license": "CC0-1.0",
                "synthetic_fields": ["partner_id", "tier", "age_rating"],
                "duration_fallback_synthetic": duration if not runtime_minutes else None,
                "provenance": "public_external+synthetic_derived",
            }
        )
    return catalog


def generate(args: argparse.Namespace) -> dict:
    rng = random.Random(args.seed)
    start = date.fromisoformat(args.start_date)
    end = start + timedelta(days=args.days - 1)
    public_payload, public_records = load_public_catalog(Path(args.catalog))
    catalog = build_catalog(public_records[: args.catalog_size], rng)
    if len(catalog) < 10:
        raise ValueError("The selected public catalogue slice must contain at least 10 titles.")

    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and any(target.iterdir()) and not args.overwrite:
        raise FileExistsError(
            f"Output directory is not empty: {target}. Choose another path or pass --overwrite."
        )
    if target.exists():
        shutil.rmtree(target)
    staging = target.with_name(target.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    content_by_id = {row["content_id"]: row for row in catalog}
    content_ids = list(content_by_id)
    counts: Counter = Counter()
    event_counter = 0
    player_by_arrival: dict[date, list[dict]] = defaultdict(list)
    akash_by_month: dict[str, list[str]] = defaultdict(list)
    nimbus_by_month: dict[str, list[str]] = defaultdict(list)
    state_names = sorted({state for _, state, _ in STATES})
    partner_ids = [f"PARTNER-{number:02d}" for number in range(1, 9)]

    # Each source-domain output remains in its original format and path.
    for snapshot_date in month_starts(start, end):
        snapshot_name = f"catalog_{snapshot_date:%Y-%m-%d}.json"
        write_json(staging / "content" / snapshot_name, catalog)
        counts["catalogue_snapshots"] += 1

    partner_rows = [
        {
            "partner_id": partner_id,
            "name": f"Independent Content Partner {number:02d}",
            "revenue_share_pct": f"{rng.uniform(18, 42):.2f}",
            "minimum_guarantee_inr_per_quarter": f"{rng.randrange(50000, 250001, 5000):.2f}",
        }
        for number, partner_id in enumerate(partner_ids, start=1)
    ]
    write_csv(
        staging / "reference" / "content_partners.csv",
        partner_rows,
        ["partner_id", "name", "revenue_share_pct", "minimum_guarantee_inr_per_quarter"],
    )
    edges = []
    for city, state, code in STATES:
        edges.extend(
            [
                {"edge": f"edge-{code}-01", "pop_code": f"POP_{code.upper()}-01", "region": state},
            ]
        )
    write_csv(staging / "reference" / "cdn_edges.csv", edges, ["edge", "pop_code", "region"])
    write_csv(
        staging / "reference" / "cdn_rates.csv",
        [
            {"cdn": cdn, "region": state, "rate_inr_per_gb": f"{rate:.2f}"}
            for cdn, rate in [("AkashCDN", 1.25), ("NimbusEdge", 1.10)]
            for state in state_names
        ],
        ["cdn", "region", "rate_inr_per_gb"],
    )
    all_genres = sorted({genre for row in catalog for genre in row["genres"]})
    write_csv(
        staging / "reference" / "genre_mapping.csv",
        [{"raw_genre": genre, "standard_genre": genre} for genre in all_genres],
        ["raw_genre", "standard_genre"],
    )
    write_csv(
        staging / "reference" / "city_state.csv",
        [{"city": city, "state": state} for city, state, _ in STATES],
        ["city", "state"],
    )
    write_csv(
        staging / "reference" / "plans.csv",
        [
            {"plan": plan, "price_inr_per_month": price, "max_screens": 1 if plan == "basic" else 4}
            for plan, price in PLANS.items()
        ],
        ["plan", "price_inr_per_month", "max_screens"],
    )
    write_csv(
        staging / "reference" / "published_metrics.csv",
        [],
        ["month", "metric", "value", "published_ts", "provenance"],
    )

    licence_rows = []
    for content_id in content_ids:
        for state in state_names:
            licence_rows.append(
                {
                    "content_id": content_id,
                    "region": state,
                    "licence_start": "2025-01-01",
                    "licence_end": "2027-12-31",
                }
            )
    counts["licences"] = write_csv(
        staging / "content" / "licences.csv",
        licence_rows,
        ["content_id", "region", "licence_start", "licence_end"],
    )

    user_rows = []
    profile_rows = []
    profiles_by_user: dict[str, list[dict]] = {}
    plans_by_user: dict[str, str] = {}
    state_by_user: dict[str, tuple[str, str, str]] = {}
    signup_by_user: dict[str, datetime] = {}
    for number in range(1, args.users + 1):
        user_id = f"U{number:06d}"
        location = rng.choice(STATES)
        city, state, _ = location
        signup = datetime.combine(start - timedelta(days=rng.randint(1, 180)), time(9, 0))
        plan = rng.choices(["basic", "premium", "free"], weights=[25, 45, 30], k=1)[0]
        user_rows.append(
            {
                "user_id": user_id,
                "signup_ts": iso(signup),
                "city": city,
                "state": state,
                "age_band": rng.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
                "acquisition_channel": rng.choice(["organic", "referral", "paid_search", "social", "partner"]),
                "provenance": "synthetic_generated",
            }
        )
        profile_count = 1 + int(rng.random() < 0.35)
        profile_list = []
        for profile_no in range(1, profile_count + 1):
            profile = {
                "profile_id": f"{user_id}-P{profile_no}",
                "user_id": user_id,
                "is_kids": profile_no > 1 and rng.random() < 0.55,
                "provenance": "synthetic_generated",
            }
            profile_rows.append(profile)
            profile_list.append(profile)
        profiles_by_user[user_id] = profile_list
        plans_by_user[user_id] = plan
        state_by_user[user_id] = location
        signup_by_user[user_id] = signup
    counts["users"] = write_jsonl(staging / "app" / "users.json", user_rows)
    counts["profiles"] = write_jsonl(staging / "app" / "profiles.json", profile_rows)

    campaign_rows = [
        {
            "campaign_id": f"CMP{number:04d}",
            "advertiser": f"Advertiser {number:02d}",
            "budget_inr": f"{rng.randrange(50000, 500001, 5000):.2f}",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily_frequency_cap": rng.choice([2, 3, 4, 5]),
            "target_genres": "ALL" if number % 4 == 0 else "|".join(rng.sample(all_genres, min(2, len(all_genres)))),
            "cpm_inr": f"{rng.uniform(60, 240):.2f}",
        }
        for number in range(1, 13)
    ]
    counts["campaigns"] = write_csv(
        staging / "ads" / "ad_campaigns.csv",
        campaign_rows,
        ["campaign_id", "advertiser", "budget_inr", "start_date", "end_date", "daily_frequency_cap", "target_genres", "cpm_inr"],
    )

    subscription_rows = []
    settlement_rows = []
    search_rows = []
    rating_rows = []
    break_rows = []
    impression_rows = []
    session_count = 0
    cdn_request_count = 0
    device_offsets: dict[str, int] = {}

    for user_no in range(1, args.users + 1):
        user_id = f"U{user_no:06d}"
        plan = plans_by_user[user_id]
        user_profiles = profiles_by_user[user_id]
        charge_price = PLANS.get(plan, 0.0)
        gateway = rng.choice(["razorpay", "cashfree", "payu"])
        if plan != "free":
            subscription_at = datetime.combine(start + timedelta(days=rng.randrange(args.days)), time(8, 0))
            initial_kind = "trial_start" if rng.random() < 0.12 else "subscribe"
            subscription_rows.append(
                {
                    "event_id": f"SUB-{user_id}-INIT",
                    "user_id": user_id,
                    "event_type": initial_kind,
                    "plan": plan if plan != "free" else "basic",
                    "price": "" if initial_kind == "trial_start" or plan == "free" else f"{charge_price:.2f}",
                    "event_ts": iso(subscription_at),
                    "gateway": gateway,
                    "provenance": "synthetic_generated",
                }
            )
            if initial_kind == "subscribe" and plan != "free":
                settlement_rows.append(
                    {
                        "gateway": gateway,
                        "user_id": user_id,
                        "charge_id": f"CHG-{user_id}-001",
                        "settlement_id": f"SET-{user_id}-001",
                        "amount_inr": f"{charge_price:.2f}",
                        "settled_ts": iso(subscription_at + timedelta(minutes=rng.randint(1, 20))),
                        "event_type": "subscribe",
                        "status": "success",
                    }
                )
    
            # A trial start is not a paid subscription. Until a conversion
            # event exists, do not fabricate renewals for trial-only accounts.
            cycles = max(0, (args.days - 1) // 30) if initial_kind == "subscribe" else 0
            if cycles:
                for cycle in range(1, cycles + 1):
                    renewal_at = subscription_at + timedelta(days=30 * cycle)
                    if renewal_at.date() > end:
                        continue
                    failed = rng.random() < 0.08
                    if failed:
                        subscription_rows.append(
                            {
                                "event_id": f"SUB-{user_id}-{cycle + 1:03d}-FAIL",
                                "user_id": user_id,
                                "event_type": "payment_failed",
                                "plan": plan,
                                "price": f"{charge_price:.2f}",
                                "event_ts": iso(renewal_at),
                                "gateway": gateway,
                                "provenance": "synthetic_generated",
                            }
                        )
                        if rng.random() < 0.75 and (renewal_at + timedelta(days=2)).date() <= end:
                            recovered = renewal_at + timedelta(days=2)
                            subscription_rows.append(
                                {
                                    "event_id": f"SUB-{user_id}-{cycle + 1:03d}-REC",
                                    "user_id": user_id,
                                    "event_type": "renew",
                                    "plan": plan,
                                    "price": f"{charge_price:.2f}",
                                    "event_ts": iso(recovered),
                                    "gateway": gateway,
                                    "provenance": "synthetic_generated",
                                }
                            )
                            settlement_rows.append(
                                {
                                    "gateway": gateway,
                                    "user_id": user_id,
                                    "charge_id": f"CHG-{user_id}-{cycle + 1:03d}",
                                    "settlement_id": f"SET-{user_id}-{cycle + 1:03d}",
                                    "amount_inr": f"{charge_price:.2f}",
                                    "settled_ts": iso(recovered + timedelta(minutes=5)),
                                    "event_type": "renew",
                                    "status": "success",
                                }
                            )
                    else:
                        subscription_rows.append(
                            {
                                "event_id": f"SUB-{user_id}-{cycle + 1:03d}-REN",
                                "user_id": user_id,
                                "event_type": "renew",
                                "plan": plan,
                                "price": f"{charge_price:.2f}",
                                "event_ts": iso(renewal_at),
                                "gateway": gateway,
                                "provenance": "synthetic_generated",
                            }
                        )
                        settlement_rows.append(
                            {
                                "gateway": gateway,
                                "user_id": user_id,
                                "charge_id": f"CHG-{user_id}-{cycle + 1:03d}",
                                "settlement_id": f"SET-{user_id}-{cycle + 1:03d}",
                                "amount_inr": f"{charge_price:.2f}",
                                "settled_ts": iso(renewal_at + timedelta(minutes=5)),
                                "event_type": "renew",
                                "status": "success",
                            }
                        )
    
    
        # Each user produces 1–3 searches and each profile may provide 1–3 ratings.
        for search_no in range(rng.randint(1, 3)):
            query_time = datetime.combine(
                start + timedelta(days=rng.randrange(args.days)),
                time(rng.randrange(7, 24), rng.randrange(60)),
            )
            selected_content = rng.choice(catalog)
            genre = rng.choice(selected_content["genres"])
            search_rows.append(
                {
                    "search_id": f"SRCH-{user_id}-{search_no + 1}",
                    "profile_id": rng.choice(user_profiles)["profile_id"],
                    "query_text": genre if rng.random() < 0.7 else rng.choice(SEARCH_TERMS),
                    "results_count": rng.choice([0, 0, 2, 4, 6, 10]),
                    "clicked_content_id": selected_content["content_id"] if rng.random() < 0.42 else "",
                    "ts": iso(query_time),
                    "provenance": "synthetic_generated",
                }
            )

        for profile in user_profiles:
            for rating_no in range(rng.randint(1, 3)):
                rating_time = datetime.combine(
                    start + timedelta(days=rng.randrange(args.days)),
                    time(rng.randrange(8, 24), rng.randrange(60)),
                )
                rating_rows.append(
                    {
                        "profile_id": profile["profile_id"],
                        "content_id": rng.choice(content_ids),
                        "rating": rng.randint(1, 5),
                        "ts": iso(rating_time),
                        "provenance": "synthetic_generated",
                    }
                )

    counts["subscriptions"] = write_jsonl(
        staging / "billing" / "subscription_events.json", subscription_rows
    )
    counts["settlements"] = write_csv(
        staging / "billing" / "gateway_settlements.csv",
        settlement_rows,
        ["gateway", "user_id", "charge_id", "settlement_id", "amount_inr", "settled_ts", "event_type", "status"],
    )
    counts["search"] = write_jsonl(staging / "search" / "search_logs.json", search_rows)
    counts["ratings"] = write_jsonl(staging / "app" / "ratings.json", rating_rows)

    # Generate arrival-time playback and related ad/CDN records.
    for day_index in range(args.days):
        event_day = start + timedelta(days=day_index)
        for user_no in range(1, args.users + 1):
            user_id = f"U{user_no:06d}"
            session_total = rng.choices([0, 1, 2], weights=[15, 75, 10], k=1)[0]
            for session_no in range(session_total):
                session_count += 1
                profile = rng.choice(profiles_by_user[user_id])
                content = rng.choice(catalog)
                city, state, edge_code = state_by_user[user_id]
                start_at = datetime.combine(
                    event_day,
                    time(rng.randrange(6, 24), rng.randrange(60), rng.randrange(60)),
                )
                duration = min(content["duration_sec"], rng.randint(8, 45) * 60)
                offline = rng.random() < 0.025
                download_id = f"DL-{user_id}-{day_index:03d}-{session_no:02d}" if offline else None
                device_id = f"DV-{user_id}-{profile['profile_id'].split('-')[-1]}"
                device_offsets.setdefault(device_id, rng.randint(-900, 900))
                client_offset = device_offsets[device_id]
                app_version = rng.choice(APP_VERSIONS)
                device_type = rng.choice(DEVICES)
                actual_positions = [0] + list(range(300, duration, 300))
                final_event = "complete" if duration >= content["duration_sec"] * 0.85 else "stop"
                session_events = [("start", 0)]
                session_events.extend(("heartbeat", position) for position in actual_positions[1:])
                session_events.append((final_event, duration))
                # Offline sessions are uploaded as a coherent batch. All events
                # share one arrival delay; online events get small network jitter.
                offline_upload_delay = timedelta(days=rng.randint(1, 3)) if offline else None

                for event_no, (event_type, position) in enumerate(session_events):
                    actual_time = start_at + timedelta(seconds=position)
                    delay = offline_upload_delay if offline else timedelta(seconds=rng.randint(2, 30))
                    server_time = actual_time + delay
                    event_counter += 1
                    event = {
                        "event_id": f"EVT{event_counter:012d}",
                        "user_id": user_id,
                        "profile_id": profile["profile_id"],
                        "device_id": device_id,
                        "content_id": content["content_id"],
                        "event_type": event_type,
                        "position_sec": position,
                        "client_ts": iso(actual_time + timedelta(seconds=client_offset)),
                        "server_ts": iso(server_time),
                        "ip_city": city,
                        "device_type": device_type,
                        "app_version": app_version,
                        "download_id": download_id,
                        "provenance": "synthetic_generated",
                    }
                    player_by_arrival[server_time.date()].append(event)

                    if event_type in ("start", "heartbeat"):
                        status = rng.choices([200, 206, 403, 503], weights=[94, 3, 2, 1], k=1)[0]
                        bytes_sent = rng.randint(4_000_000, 28_000_000)
                        segment = f"seg_{event_counter % 10000:05d}"
                        quality = rng.choice(["360p", "480p", "720p", "1080p"])
                        user_agent = f"StreamBharat-{device_type}/{app_version}"
                        client_hash = hashlib.sha256(user_id.encode()).hexdigest()[:8]
                        request_month = f"{server_time:%Y-%m}"
                        if rng.random() < 0.5:
                            akash_by_month[request_month].append(
                                f'{server_time:%Y-%m-%dT%H:%M:%SZ} edge-{edge_code}-01 {client_hash} '
                                f'"GET /v/{content["content_id"]}/{quality}/{segment}.ts HTTP/2" '
                                f'{status} {bytes_sent} "{user_agent}"'
                            )
                        else:
                            pop = f"POP_{edge_code.upper()}-01"
                            epoch = calendar.timegm(server_time.timetuple())
                            nimbus_by_month[request_month].append(
                                f"{epoch}\t{pop}\t{client_hash}\tvod/{content['content_id']}/{quality}/{segment}\t"
                                f"{status}\t{bytes_sent}\t{user_agent}"
                            )
                        cdn_request_count += 1

                if plans_by_user[user_id] == "free":
                    filled = int(rng.random() < 0.78)
                    impression_id = f"IMP-{user_id}-{day_index:03d}-{session_no:02d}" if filled else ""
                    break_id = f"BRK-{user_id}-{day_index:03d}-{session_no:02d}"
                    break_time = start_at + timedelta(minutes=min(15, max(1, duration // 120)))
                    break_rows.append(
                        {
                            "break_id": break_id,
                            "user_id": user_id,
                            "profile_id": profile["profile_id"],
                            "content_id": content["content_id"],
                            "break_ts": iso(break_time),
                            "filled": filled,
                            "impression_id": impression_id,
                        }
                    )
                    if filled:
                        campaign = rng.choice(campaign_rows)
                        impression_rows.append(
                            {
                                "impression_id": impression_id,
                                "user_id": user_id,
                                "profile_id": profile["profile_id"],
                                "content_id": content["content_id"],
                                "campaign_id": campaign["campaign_id"],
                                "ad_break_ts": iso(break_time),
                                "duration_sec": rng.choice([10, 15, 20, 30]),
                                "completed": int(rng.random() < 0.9),
                                "cpm_inr": campaign["cpm_inr"],
                            }
                        )

    for arrival_day, rows in sorted(player_by_arrival.items()):
        rows.sort(key=lambda row: (row["server_ts"], row["event_id"]))
        name = f"playback_{arrival_day.isoformat()}.json"
        counts["playback_events"] += write_jsonl(staging / "player" / name, rows)

    counts["ad_breaks"] = write_csv(
        staging / "ads" / "ad_breaks.csv",
        break_rows,
        ["break_id", "user_id", "profile_id", "content_id", "break_ts", "filled", "impression_id"],
    )
    counts["ad_impressions"] = write_csv(
        staging / "ads" / "ad_impressions.csv",
        impression_rows,
        ["impression_id", "user_id", "profile_id", "content_id", "campaign_id", "ad_break_ts", "duration_sec", "completed", "cpm_inr"],
    )

    for month, lines in sorted(akash_by_month.items()):
        path = staging / "cdn" / f"akashcdn_{month}.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    for month, lines in sorted(nimbus_by_month.items()):
        path = staging / "cdn" / f"nimbusedge_{month}.tsv"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    counts["cdn_requests"] = cdn_request_count

    # Calculate a run manifest with immutable source facts and local SHA-256 hashes.
    file_hashes = {}
    for path in sorted(staging.rglob("*")):
        if path.is_file():
            relative = path.relative_to(staging).as_posix()
            file_hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()

    catalogue_bytes = Path(args.catalog).read_bytes()
    manifest = {
        "dataset_name": "StreamBharat Hybrid OTT Lab",
        "dataset_type": "hybrid",
        "schema_version": "1.0",
        "generator_version": GENERATOR_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "business_period": {"start_date": start.isoformat(), "end_date": end.isoformat(), "days": args.days},
        "playback_arrival_period": {
            "start_date": min(player_by_arrival).isoformat() if player_by_arrival else None,
            "end_date": max(player_by_arrival).isoformat() if player_by_arrival else None,
        },
        "input_snapshots": {
            "wikidata_catalogue": {
                "file_name": Path(args.catalog).name,
                "sha256": hashlib.sha256(catalogue_bytes).hexdigest(),
                "source": public_payload.get("source", "Wikidata Query Service"),
                "source_endpoint": public_payload.get("source_endpoint"),
                "license": public_payload.get("license"),
                "retrieved_at_utc": public_payload.get("retrieved_at_utc"),
                "record_count": len(public_records),
            }
        },
        "configuration": {
            "seed": args.seed,
            "users": args.users,
            "catalog_size": len(catalog),
            "sessions_generated": session_count,
        },
        "provenance": {
            "public_external": {
                "source": "Wikidata Query Service",
                "source_url": public_payload.get("source_endpoint"),
                "license": "CC0-1.0",
                "retrieved_at_utc": public_payload.get("retrieved_at_utc"),
                "fields": ["title", "release_date", "genres", "original_languages", "runtime_minutes", "wikipedia_title"],
            },
            "synthetic_derived": {
                "fields": ["content_id", "partner_id", "tier", "age_rating", "duration_fallback_synthetic"],
                "description": "StreamBharat identifiers and business attributes assigned to public catalogue records.",
            },
            "synthetic_generated": {
                "domains": ["users", "profiles", "playback", "subscriptions", "settlements", "search", "ratings", "ads", "cdn", "licences", "reference"],
                "description": "All platform activity, user-like records, commercial events and operational telemetry are generated test data.",
            },
        },
        "quality_features": {
            "device_clock_skew": "A stable per-device client clock offset is applied.",
            "offline_uploads": "A small share of playback sessions arrive 1-3 days after client event time.",
            "payment_failures": "A small share of renewal attempts fail; most have a later recovery event.",
            "cdn_status_variation": "HTTP 200, 206, 403 and 503 responses are represented.",
            "ad_fill": "Some free-tier ad breaks are deliberately unfilled.",
            "known_limitations": [
                "This is not the missing coursework dataset and does not reproduce its exact row counts.",
                "The public catalogue slice is movie metadata; series and episode relationships are not invented.",
                "Cast is empty when not obtained from the public source query.",
                "Published board metrics are intentionally left empty; no real business baseline is available.",
                "Licence rows and partner assignments are synthetic scenario data, not legal licensing facts.",
            ],
        },
        "row_counts": dict(counts),
        "files_sha256": file_hashes,
    }
    write_json(staging / "manifest.json", manifest)
    staging.rename(target)
    print("HYBRID DATASET GENERATED")
    print(f"  output: {target.resolve()}")
    print(f"  period: {start.isoformat()} through {end.isoformat()} ({args.days} days)")
    print(f"  users: {args.users}; public titles: {len(catalog)}; seed: {args.seed}")
    for name, count in sorted(counts.items()):
        print(f"  {name}: {count}")
    print("  provenance: public catalogue metadata + generated OTT activity")
    print("  manifest: manifest.json")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="data/external/wikidata/catalog.json")
    parser.add_argument("--output", default="data/hybrid/raw")
    parser.add_argument("--start-date", default="2026-11-01")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--catalog-size", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if not 1 <= args.days <= 366:
        parser.error("--days must be between 1 and 366")
    if not 10 <= args.users <= 100000:
        parser.error("--users must be between 10 and 100000")
    if not 10 <= args.catalog_size <= 1000:
        parser.error("--catalog-size must be between 10 and 1000")
    try:
        generate(args)
    except (FileNotFoundError, FileExistsError, ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
