#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-streambharat-kafka}"
topics=(
  "ott.player.events:6"
  "ott.billing.subscr:3"
  "ott.billing.settle:3"
  "ott.app.search:3"
  "ott.app.ratings:1"
  "ott.ads.breaks:3"
  "ott.ads.impressions:3"
  "ott.live.plays:6"
  "ott.live.ads:6"
  "ott.dlq:3"
  "ott.metrics.trending:3"
  "ott.alerts.ad_pacing:3"
  "ott.ref.subscription_state:3"
)

for spec in "${topics[@]}"; do
  IFS=: read -r topic partitions <<< "$spec"
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1
done

for compact_topic in ott.ref.subscription_state ott.metrics.trending ott.alerts.ad_pacing; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:29092 \
    --entity-type topics --entity-name "$compact_topic" \
    --alter --add-config cleanup.policy=compact
done

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
