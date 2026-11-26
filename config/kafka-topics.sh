#!/usr/bin/env bash
set -euo pipefail

CONTAINER="\${KAFKA_CONTAINER:-streambharat-kafka}"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic streambharat.plays --partitions 3 --replication-factor 1
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic streambharat.ads --partitions 3 --replication-factor 1
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
