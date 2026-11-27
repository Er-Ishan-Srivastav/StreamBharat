"""Kafka output helpers for the live StreamBharat source simulator."""
import json
import os
from datetime import datetime, timezone


def now_utc():
    """Return a timezone-naive UTC timestamp for the configured Spark UTC session."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def add_args(parser):
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get(
            "KAFKA_BOOTSTRAP",
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
    )
    parser.add_argument("--dry-run", action="store_true")


def make_write(args, topics, keys, client_id):
    if args.dry_run:
        os.makedirs("_dryrun", exist_ok=True)
        files = {}

        def send(topic, key, row):
            handle = files.setdefault(
                topic, open(f"_dryrun/{topic}.jsonl", "a", encoding="utf-8")
            )
            handle.write(json.dumps({"key": key, "value": row}) + "\n")
            handle.flush()
    else:
        from confluent_kafka import Producer
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
            "compression.type": "snappy",
        })

        def send(topic, key, row):
            while True:
                try:
                    producer.produce(
                        topic,
                        key=None if key is None else str(key).encode(),
                        value=json.dumps(row, separators=(",", ":")).encode(),
                    )
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)

    def write(feed, rows, stamp=None):
        name = os.path.basename(feed.rstrip("/"))
        for row in rows:
            send(topics[name], row.get(keys[name]), row)
        if not args.dry_run:
            producer.flush(10)

    return write
