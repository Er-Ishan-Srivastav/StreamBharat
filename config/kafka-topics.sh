#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-streambharat-kafka}"

create_topic() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists --topic "$1" \
    --partitions 3 --replication-factor 1
}

create_topic streambharat.plays
create_topic streambharat.ads

docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" --list
