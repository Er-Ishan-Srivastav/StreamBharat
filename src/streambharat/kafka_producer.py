"""Publish StreamBharat simulator JSONL events to Kafka topics."""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterator

from confluent_kafka import Producer

LOG = logging.getLogger("streambharat.kafka_producer")
TOPICS = {
    "plays": os.getenv("KAFKA_PLAYS_TOPIC", "streambharat.plays"),
    "ads": os.getenv("KAFKA_ADS_TOPIC", "streambharat.ads"),
}


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        LOG.error("Delivery failed: %s", err)
    else:
        LOG.debug("Delivered %s[%s] offset=%s", msg.topic(), msg.partition(), msg.offset())


def event_files(folder: Path) -> Iterator[Path]:
    if folder.exists():
        yield from sorted(p for p in folder.glob("*.json") if not p.name.startswith(".tmp_"))


def publish_file(producer: Producer, topic: str, path: Path, seen: set[str]) -> int:
    published = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                LOG.warning("Skipping invalid JSON %s:%s (%s)", path, line_number, exc)
                continue
            event_id = event.get("event_id") or event.get("impression_id")
            if not event_id:
                LOG.warning("Skipping event without event_id/impression_id: %s:%s", path, line_number)
                continue
            if event_id in seen:
                continue
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=str(event_id),
                        value=json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                        on_delivery=delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(0.5)
            seen.add(event_id)
            published += 1
            if published % 500 == 0:
                producer.poll(0)
    producer.poll(0)
    return published


def run_publisher(stream: str, input_root: str, bootstrap_servers: str,
                  poll_seconds: float = 1.0) -> None:
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "streambharat-simulator-bridge",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 20,
        "message.timeout.ms": 120000,
    })
    streams = ("plays", "ads") if stream == "both" else (stream,)
    seen = {name: set() for name in streams}
    published_files = {name: set() for name in streams}
    LOG.info("Publishing simulator files to Kafka at %s", bootstrap_servers)
    try:
        while True:
            found_new = False
            for name in streams:
                for path in event_files(Path(input_root) / name):
                    identity = str(path.resolve())
                    if identity in published_files[name]:
                        continue
                    found_new = True
                    count = publish_file(producer, TOPICS[name], path, seen[name])
                    producer.flush(30)
                    published_files[name].add(identity)
                    LOG.info("Published %s records from %s to %s", count, path.name, TOPICS[name])
            producer.poll(0.2)
            if not found_new:
                time.sleep(poll_seconds)
    except KeyboardInterrupt:
        LOG.info("Stopping publisher")
    finally:
        remaining = producer.flush(30)
        if remaining:
            LOG.warning("%s messages still queued after flush timeout", remaining)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bridge simulator JSON files into Kafka")
    parser.add_argument("--stream", choices=["plays", "ads", "both"], default="both")
    parser.add_argument("--input-root", default="data/stream")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    run_publisher(args.stream, args.input_root, args.bootstrap_servers, args.poll_seconds)


if __name__ == "__main__":
    main()
