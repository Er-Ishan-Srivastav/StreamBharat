"""Create and validate the workbook-defined Kafka topic contracts."""
from __future__ import annotations

import argparse
import os

TOPICS = {
    "ott.player.events": 6,
    "ott.billing.subscr": 3,
    "ott.billing.settle": 3,
    "ott.app.search": 3,
    "ott.app.ratings": 1,
    "ott.ads.breaks": 3,
    "ott.ads.impressions": 3,
    "ott.live.plays": 6,
    "ott.live.ads": 6,
    "ott.dlq": 3,
    "ott.metrics.trending": 3,
    "ott.alerts.ad_pacing": 3,
    "ott.ref.subscription_state": 3,
}
COMPACT_TOPICS = {"ott.ref.subscription_state"}


def create_topics(bootstrap_servers: str, replication_factor: int = 1) -> dict[str, str]:
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaException
    except ImportError as exc:
        raise RuntimeError("Install the Kafka extra: pip install -e '.[kafka]'") from exc

    if replication_factor < 1:
        raise ValueError("replication_factor must be positive")
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=20)
    results: dict[str, str] = {}
    missing = []
    for name, partitions in TOPICS.items():
        existing = metadata.topics.get(name)
        if existing is None:
            missing.append(NewTopic(
                name,
                num_partitions=partitions,
                replication_factor=replication_factor,
                config=(
                    {"cleanup.policy": "compact"}
                    if name in COMPACT_TOPICS else None
                ),
            ))
            results[name] = "created"
        elif len(existing.partitions) != partitions:
            raise RuntimeError(
                f"Topic {name} has {len(existing.partitions)} partitions; "
                f"workbook contract requires {partitions}. Existing topics are not "
                "silently repartitioned because that changes key ordering."
            )
        else:
            results[name] = "exists"

    if missing:
        futures = admin.create_topics(missing, request_timeout=30)
        for name, future in futures.items():
            try:
                future.result()
                results[name] = "created"
            except KafkaException as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    # Re-read metadata after creation and verify every required topic/partition count.
    verified = admin.list_topics(timeout=20)
    for name, partitions in TOPICS.items():
        topic = verified.topics.get(name)
        if topic is None:
            raise RuntimeError(f"Kafka topic creation failed: {name}")
        if len(topic.partitions) != partitions:
            raise RuntimeError(
                f"Topic {name}: expected {partitions} partitions, got {len(topic.partitions)}"
            )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create the 13 Kafka ETL workbook topics with exact partition counts"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()
    for name, status in create_topics(args.bootstrap_servers, args.replication_factor).items():
        print(f"{name}: {status}")


if __name__ == "__main__":
    main()
