"""StreamBharat source systems and their Kafka topic/key contracts."""
import csv
import json
import os
import zlib
from datetime import datetime, timedelta

from replay_engine import Source, parse_ts

PROJECT = "streambharat"
DEFAULT_FROM = "2026-10-01"
DEFAULT_TO = "2026-10-31"
DOC = __doc__
PLAYER_V2_FROM = datetime(2026, 10, 15)


def read_player(ctx):
    """Player files are named by server-arrival date and emitted at server_ts."""
    for day in ctx.days():
        path = f"{ctx.data}/player/playback_{day.isoformat()}.json"
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle if line.strip()]
        rows.sort(key=lambda row: (row["server_ts"], row["event_id"]))
        for row in rows:
            yield parse_ts(row["server_ts"]), row["user_id"], row


def player_version(event_time, record):
    if (
        event_time < PLAYER_V2_FROM
        or record.get("device_type") != "android"
        or not str(record.get("app_version", "")).startswith("7.3")
    ):
        return record, "1"
    updated = dict(record)
    updated["position_ms"] = int(float(updated.pop("position_sec", 0) or 0) * 1000)
    updated["network"] = (
        "wifi" if zlib.crc32(record["device_id"].encode()) % 3 else "cellular"
    )
    return updated, "2"


def _json_rows(path, timestamp_field, key_field, ctx):
    month_start = f'"{timestamp_field}": "{ctx.d_from:%Y-%m}'
    month_end = f'"{timestamp_field}": "{ctx.d_to:%Y-%m}'
    rows = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if month_start not in line and month_end not in line:
                continue
            row = json.loads(line)
            event_time = parse_ts(row[timestamp_field])
            if ctx.in_range(event_time):
                rows.append((event_time, row[key_field], row))
    rows.sort(key=lambda item: item[0])
    return rows


def _csv_rows(path, timestamp_field, key_field, ctx):
    rows = []
    with open(path, encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            event_time = parse_ts(row[timestamp_field])
            if ctx.in_range(event_time):
                rows.append((event_time, row[key_field], row))
    rows.sort(key=lambda item: item[0])
    return rows


def read_subscriptions(ctx):
    yield from _json_rows(
        f"{ctx.data}/billing/subscription_events.json", "event_ts", "user_id", ctx
    )


def read_settlements(ctx):
    yield from _csv_rows(
        f"{ctx.data}/billing/gateway_settlements.csv", "settled_ts", "user_id", ctx
    )


def read_search(ctx):
    yield from _json_rows(f"{ctx.data}/search/search_logs.json", "ts", "profile_id", ctx)


def read_ratings(ctx):
    yield from _json_rows(f"{ctx.data}/app/ratings.json", "ts", "profile_id", ctx)


def read_ad_breaks(ctx):
    yield from _csv_rows(f"{ctx.data}/ads/ad_breaks.csv", "break_ts", "user_id", ctx)


def read_impressions(ctx):
    # The ad server reports the impression when the ad finishes or is skipped.
    for event_time, key, row in _csv_rows(
        f"{ctx.data}/ads/ad_impressions.csv", "ad_break_ts", "user_id", ctx
    ):
        yield event_time + timedelta(seconds=int(row["duration_sec"] or 0)), key, row


SOURCES = [
    Source(
        "player", "ott.player.events", read_player, "player-sdk",
        late_p=0.003, dup_p=0.004, dup_seconds=(1, 60),
        version=player_version, ts_field="client_ts",
        key_fields=("user_id",),
    ),
    Source(
        "subscriptions", "ott.billing.subscr", read_subscriptions, "billing",
        late_p=0.01, dup_p=0.01, amount_field="price", ts_field="event_ts",
        key_fields=("user_id",),
    ),
    Source(
        "settlements", "ott.billing.settle", read_settlements, "payment-gateway",
        late_p=0.02, late_minutes=(30, 360), dup_p=0.005,
        amount_field="amount_inr", ts_field="settled_ts", key_fields=("user_id",),
    ),
    Source(
        "search", "ott.app.search", read_search, "search-api",
        late_p=0.005, dup_p=0.003, ts_field="ts", key_fields=("profile_id",),
    ),
    Source(
        "ratings", "ott.app.ratings", read_ratings, "app-api",
        late_p=0.005, dup_p=0.003, ts_field="ts", key_fields=("profile_id",),
    ),
    Source(
        "ad_breaks", "ott.ads.breaks", read_ad_breaks, "ad-decision-server",
        late_p=0.005, dup_p=0.003, ts_field="break_ts", key_fields=("user_id",),
    ),
    Source(
        "impressions", "ott.ads.impressions", read_impressions, "ad-tracking",
        late_p=0.02, late_minutes=(2, 30), dup_p=0.008, dup_seconds=(1, 20),
        amount_field="cpm_inr", ts_field="ad_break_ts",
        key_fields=("user_id",),
    ),
]
