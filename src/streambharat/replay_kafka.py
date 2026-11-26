"""Replay the seven October 2026 source datasets into their Kafka topics.

The replay preserves the source payload (JSONL lines remain JSON; CSV rows are
serialized as JSON objects), routes records with the workbook's partition key,
and globally orders the seven feeds by their source event/arrival timestamp.
The player files are read one server-arrival day at a time to bound memory.
"""
from __future__ import annotations

import argparse
import csv
import heapq
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    from confluent_kafka import Producer
except ImportError:  # Unit tests can import the source contract without Kafka installed.
    Producer = None  # type: ignore[assignment,misc]

LOG = logging.getLogger("streambharat.replay_kafka")


@dataclass(frozen=True)
class SourceSpec:
    alias: str
    relative_path: str
    file_pattern: str
    file_format: str
    timestamp_field: str
    key_field: str
    topic: str
    source_index: int


SOURCE_SPECS = {
    "player_events": SourceSpec(
        "player_events", "player", "playback_*.json", "jsonl",
        "server_ts", "user_id", "ott.player.events", 0,
    ),
    "billing_subscr": SourceSpec(
        "billing_subscr", "billing", "subscription_events.json", "jsonl",
        "event_ts", "user_id", "ott.billing.subscr", 1,
    ),
    "billing_settle": SourceSpec(
        "billing_settle", "billing", "gateway_settlements.csv", "csv",
        "settled_ts", "user_id", "ott.billing.settle", 2,
    ),
    "app_search": SourceSpec(
        "app_search", "search", "search_logs.json", "jsonl",
        "ts", "profile_id", "ott.app.search", 3,
    ),
    "app_ratings": SourceSpec(
        "app_ratings", "app", "ratings.json", "jsonl",
        "ts", "profile_id", "ott.app.ratings", 4,
    ),
    "ads_breaks": SourceSpec(
        "ads_breaks", "ads", "ad_breaks.csv", "csv",
        "break_ts", "user_id", "ott.ads.breaks", 5,
    ),
    "ads_impressions": SourceSpec(
        "ads_impressions", "ads", "ad_impressions.csv", "csv",
        "ad_break_ts", "user_id", "ott.ads.impressions", 6,
    ),
}


def parse_event_time(value: Any) -> datetime | None:
    """Parse dataset timestamps to naive UTC datetimes for deterministic ordering."""
    if value is None or str(value).strip() == "":
        return None
    try:
        if isinstance(value, (int, float)) or str(value).strip().isdigit():
            return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(tzinfo=None)
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def record_time(row: dict[str, Any], spec: SourceSpec, fallback: datetime) -> datetime:
    event_time = parse_event_time(row.get(spec.timestamp_field))
    if event_time is None:
        return fallback
    if spec.alias == "ads_impressions":
        try:
            event_time += timedelta(seconds=float(row.get("duration_sec") or 0))
        except (TypeError, ValueError):
            pass
    return event_time


def _read_file(path: Path, file_format: str) -> Iterator[tuple[int, dict[str, Any], str]]:
    """Yield (source line, parsed row, Kafka value) without modifying raw files."""
    if file_format == "jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    LOG.warning("Malformed JSON retained for no replay: %s:%s (%s)",
                                path, line_number, exc)
                    continue
                if isinstance(row, dict):
                    yield line_number, row, line.strip()
                else:
                    LOG.warning("Skipping non-object JSON at %s:%s", path, line_number)
        return

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            line_number = reader.line_num
            yield line_number, dict(row), json.dumps(
                dict(row), ensure_ascii=False, separators=(",", ":")
            )


def _iter_file_records(
    path: Path,
    spec: SourceSpec,
    start: datetime,
    end_exclusive: datetime,
) -> list[tuple[datetime, int, int, str, str | None, str, str, int]]:
    """Sort one source file in memory; player files are bounded to one arrival day."""
    fallback = start
    records = []
    for line_number, row, raw_value in _read_file(path, spec.file_format):
        event_time = record_time(row, spec, fallback)
        if not (start <= event_time < end_exclusive):
            continue
        key_value = row.get(spec.key_field)
        key = str(key_value) if key_value not in (None, "") else None
        records.append((
            event_time, spec.source_index, line_number, spec.topic, key,
            raw_value, path.name, line_number,
        ))
    records.sort(key=lambda item: (item[0], item[1], item[2]))
    return records


def iter_source_records(
    data_root: str | Path,
    spec: SourceSpec,
    start: datetime,
    end_exclusive: datetime,
) -> Iterator[tuple[datetime, int, int, str, str | None, str, str, int]]:
    folder = Path(data_root) / spec.relative_path
    files = sorted(folder.glob(spec.file_pattern))
    if not files:
        raise FileNotFoundError(
            f"No source files found for {spec.alias}: {folder / spec.file_pattern}"
        )

    # Player history is partitioned by server-arrival day. Process one file at a
    # time; the filename order preserves the source arrival-day boundary.
    for path in files:
        records = _iter_file_records(path, spec, start, end_exclusive)
        yield from records


def iter_replay_records(
    data_root: str | Path,
    selected_topics: list[str],
    start: datetime,
    end_exclusive: datetime,
) -> Iterator[tuple[datetime, int, int, str, str | None, str, str, int]]:
    streams = [
        iter_source_records(data_root, SOURCE_SPECS[alias], start, end_exclusive)
        for alias in selected_topics
    ]
    yield from heapq.merge(*streams, key=lambda item: (item[0], item[1], item[2]))


def replay(
    data_root: str,
    selected_topics: list[str],
    start_date: date,
    end_date: date,
    bootstrap_servers: str,
    speed: float = 1.0,
    max_events: int | None = None,
) -> dict[str, int]:
    if speed < 0:
        raise ValueError("speed must be zero (instant) or a positive replay multiplier")
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    if not selected_topics:
        raise ValueError("at least one source topic must be selected")
    if Producer is None:
        raise RuntimeError(
            "Kafka producer dependency missing. Install with: pip install -e '.[kafka]'"
        )

    start = datetime.combine(start_date, datetime_time.min)
    end_exclusive = datetime.combine(end_date + timedelta(days=1), datetime_time.min)
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "streambharat-october-source-replay",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 20,
        "batch.size": 131072,
        "message.timeout.ms": 120000,
    })
    counts = {alias: 0 for alias in selected_topics}
    failures = 0
    total = 0
    first_event_time: datetime | None = None
    wall_start = time.monotonic()

    def on_delivery(err: Any, message: Any) -> None:
        nonlocal failures
        if err is not None:
            failures += 1
            LOG.error("Kafka delivery failed: %s", err)

    LOG.info(
        "Replaying %s through %s from %s at speed=%s (0 means no delay)",
        start_date, end_date, data_root, speed,
    )
    for event_time, source_index, line_number, topic, key, raw_value, source_file, source_line in iter_replay_records(
        data_root, selected_topics, start, end_exclusive
    ):
        if max_events is not None and total >= max_events:
            break
        if speed > 0:
            if first_event_time is None:
                first_event_time = event_time
                wall_start = time.monotonic()
            simulated_elapsed = (event_time - first_event_time).total_seconds() * speed / 1440.0
            wait_seconds = wall_start + simulated_elapsed - time.monotonic()
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        headers = [
            ("source_file", source_file.encode("utf-8")),
            ("source_line", str(source_line).encode("utf-8")),
            ("source_alias", selected_topics[source_index].encode("utf-8")
             if source_index < len(selected_topics) else b"unknown"),
        ]
        producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key is not None else None,
            value=raw_value.encode("utf-8"),
            headers=headers,
            on_delivery=on_delivery,
        )
        counts[next(alias for alias in selected_topics
                    if SOURCE_SPECS[alias].topic == topic)] += 1
        total += 1
        if total % 5000 == 0:
            producer.poll(0)
            remaining = producer.flush(0)
            if remaining:
                LOG.info("Queued messages remaining after %s records: %s", total, remaining)

    remaining = producer.flush(120)
    if remaining:
        failures += remaining
    LOG.info("Replay complete: total=%s failures=%s per_topic=%s", total, failures, counts)
    if failures:
        raise RuntimeError(f"{failures} Kafka messages failed or remained unconfirmed")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay the seven StreamBharat source feeds to Kafka in timestamp order"
    )
    parser.add_argument("--data-root", default="data/raw")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 10, 31))
    parser.add_argument("--topics", help="Comma-separated source aliases; default all seven")
    parser.add_argument("--bootstrap-servers",
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--speed", type=float, default=1.0,
                        help="1 = one simulated day per minute; 0 = fastest possible")
    parser.add_argument("--max-events", type=int, help="Optional cap for a small smoke replay")
    args = parser.parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    selected = (
        [value.strip() for value in args.topics.split(",") if value.strip()]
        if args.topics else list(SOURCE_SPECS)
    )
    invalid = set(selected) - set(SOURCE_SPECS)
    if invalid:
        parser.error(f"Unknown source aliases: {sorted(invalid)}")
    replay(
        args.data_root, selected, args.start_date, args.end_date,
        args.bootstrap_servers, args.speed, args.max_events,
    )


if __name__ == "__main__":
    main()
