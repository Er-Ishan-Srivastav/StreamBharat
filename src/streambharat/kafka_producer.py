"""Publish StreamBharat live simulator JSONL events to Kafka.

This bridge feeds the two live topics used by the trending and ad-pacing jobs.
A SQLite acknowledgement ledger avoids replaying acknowledged events after a
normal process restart. It does not replace downstream business-key deduplication.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterator

try:
    from confluent_kafka import Producer
except ImportError:  # Pure contract tests can run without the Kafka extra.
    Producer = None  # type: ignore[assignment,misc]

LOG = logging.getLogger("streambharat.kafka_producer")
TOPICS = {
    "plays": os.getenv("KAFKA_PLAYS_TOPIC", "ott.live.plays"),
    "ads": os.getenv("KAFKA_ADS_TOPIC", "ott.live.ads"),
}
PARTITION_KEYS = {"plays": "user_id", "ads": "user_id"}


def event_identity(event: dict[str, Any]) -> str | None:
    """Return the stable business event identifier for either live stream."""
    value = event.get("event_id") or event.get("impression_id")
    return str(value) if value not in (None, "") else None


def event_files(folder: Path) -> Iterator[Path]:
    """Yield completed simulator files only; its writer uses atomic rename."""
    if folder.exists():
        yield from sorted(
            p for p in folder.glob("*.json")
            if p.is_file() and not p.name.startswith(".tmp_")
        )


def open_state_db(path: str) -> sqlite3.Connection:
    """Open the durable producer acknowledgement ledger."""
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(state_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS published_events (
            stream TEXT NOT NULL,
            event_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_line INTEGER NOT NULL,
            published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            partition_id INTEGER,
            kafka_offset INTEGER,
            PRIMARY KEY (stream, event_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS completed_files (
            stream TEXT NOT NULL,
            source_file TEXT NOT NULL,
            completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stream, source_file)
        )
        """
    )
    conn.commit()
    return conn


def _already_published(conn: sqlite3.Connection, stream: str, event_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM published_events WHERE stream = ? AND event_id = ?",
        (stream, event_id),
    ).fetchone() is not None


def _file_completed(conn: sqlite3.Connection, stream: str, path: Path) -> bool:
    return conn.execute(
        "SELECT 1 FROM completed_files WHERE stream = ? AND source_file = ?",
        (stream, str(path.resolve())),
    ).fetchone() is not None


def _mark_file_completed(conn: sqlite3.Connection, stream: str, path: Path) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO completed_files(stream, source_file) VALUES (?, ?)",
        (stream, str(path.resolve())),
    )
    conn.commit()


def publish_file(
    producer: Any,
    topic: str,
    path: Path,
    stream: str,
    state_db: sqlite3.Connection,
) -> tuple[int, int]:
    """Publish one file; return acknowledged and failed/unconfirmed counts."""
    acknowledged = 0
    failed = 0
    pending: set[str] = set()

    def report(event_id: str, source_line: int, err: Any, message: Any) -> None:
        nonlocal acknowledged, failed
        pending.discard(event_id)
        if err is not None:
            failed += 1
            LOG.error(
                "Delivery failed for %s:%s event=%s: %s",
                path.name, source_line, event_id, err,
            )
            return
        state_db.execute(
            """
            INSERT OR IGNORE INTO published_events
                (stream, event_id, topic, source_file, source_line, partition_id, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stream, event_id, topic, str(path.resolve()), source_line,
                message.partition(), message.offset(),
            ),
        )
        acknowledged += 1
        if acknowledged % 250 == 0:
            state_db.commit()

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                LOG.warning("Skipping invalid simulator JSON %s:%s (%s)",
                            path, line_number, exc)
                continue
            if not isinstance(event, dict):
                LOG.warning("Skipping non-object event %s:%s", path, line_number)
                continue
            event_id = event_identity(event)
            if not event_id:
                LOG.warning("Skipping event without event_id/impression_id: %s:%s",
                            path, line_number)
                continue
            if event_id in pending or _already_published(state_db, stream, event_id):
                continue

            key_field = PARTITION_KEYS[stream]
            key_value = event.get(key_field)
            headers = [
                ("event_id", event_id.encode("utf-8")),
                ("source_file", path.name.encode("utf-8")),
                ("schema_version", str(event.get("schema_version", "1")).encode("utf-8")),
            ]
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=str(key_value).encode("utf-8") if key_value not in (None, "") else None,
                        value=json.dumps(
                            event, separators=(",", ":"), ensure_ascii=False
                        ).encode("utf-8"),
                        headers=headers,
                        on_delivery=lambda err, msg, eid=event_id, row=line_number:
                            report(eid, row, err, msg),
                    )
                    pending.add(event_id)
                    break
                except BufferError:
                    producer.poll(0.5)

            if line_number % 500 == 0:
                producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        LOG.error("%s messages remain queued after flush timeout for %s", remaining, path)
    state_db.commit()
    failed += len(pending)
    return acknowledged, failed


def run_publisher(
    stream: str,
    input_root: str,
    bootstrap_servers: str,
    poll_seconds: float = 1.0,
    state_db_path: str = "lake/state/kafka_producer.sqlite3",
) -> None:
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    if Producer is None:
        raise RuntimeError(
            "Kafka producer dependency missing. Install with: pip install -e '.[kafka]'"
        )

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "streambharat-simulator-bridge",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 20,
        "batch.size": 65536,
        "message.timeout.ms": 120000,
    })
    streams = ("plays", "ads") if stream == "both" else (stream,)
    state_db = open_state_db(state_db_path)
    LOG.info("Publishing live simulator files to Kafka at %s", bootstrap_servers)
    try:
        while True:
            found_work = False
            for name in streams:
                for path in event_files(Path(input_root) / name):
                    if _file_completed(state_db, name, path):
                        continue
                    found_work = True
                    sent, failures = publish_file(
                        producer, TOPICS[name], path, name, state_db
                    )
                    if failures == 0:
                        _mark_file_completed(state_db, name, path)
                        LOG.info(
                            "Acknowledged %s records from %s to %s",
                            sent, path.name, TOPICS[name],
                        )
                    else:
                        LOG.warning(
                            "%s had %s failed/unconfirmed deliveries; it will be retried",
                            path.name, failures,
                        )
            producer.poll(0.2)
            if not found_work:
                time.sleep(poll_seconds)
    except KeyboardInterrupt:
        LOG.info("Stopping publisher")
    finally:
        remaining = producer.flush(30)
        if remaining:
            LOG.warning("%s messages still queued after shutdown flush", remaining)
        state_db.commit()
        state_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bridge live simulator files into ott.live Kafka topics"
    )
    parser.add_argument("--stream", choices=["plays", "ads", "both"], default="both")
    parser.add_argument("--input-root", default="data/stream")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--state-db", default="lake/state/kafka_producer.sqlite3")
    args = parser.parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run_publisher(
        args.stream, args.input_root, args.bootstrap_servers,
        args.poll_seconds, args.state_db,
    )


if __name__ == "__main__":
    main()
