#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-streambharat-kafka}"
topics=(
  "ott.player.events:6"
  "ott.billing.subscriptions:3"
  "ott.billing.settlements:3"
  "ott.app.search:3"
  "ott.app.ratings:1"
  "ott.ads.breaks:3"
  "ott.ads.impressions:3"
  "ott.live.plays:6"
  "ott.live.ads:3"
  "ott.dlq:1"
  "ott.metrics.trending:1"
  "ott.alerts.ad_pacing:1"
  "ott.ref.subscription_state:3"
)

for spec in "${topics[@]}"; do
  IFS=: read -r topic partitions <<< "$spec"
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1
done

docker exec "$CONTAINER" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:29092 \
  --entity-type topics --entity-name ott.ref.subscription_state \
  --alter --add-config cleanup.policy=compact

for topic in ott.metrics.trending ott.alerts.ad_pacing; do
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:29092 \
    --entity-type topics --entity-name "$topic" \
    --alter --add-config cleanup.policy=delete
done


docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
