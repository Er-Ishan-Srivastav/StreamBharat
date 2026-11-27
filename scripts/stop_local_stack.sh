#!/usr/bin/env bash
set -euo pipefail

# Stop producers/streaming queries before containers, and Airflow before the
# core Kafka compose network. Volumes are deliberately preserved.
if docker compose -f docker-compose.kafka.yml ps --status running --services 2>/dev/null | grep -qx workspace; then
  echo "[1/3] Stopping optional live Q17/Q18 demo processes..."
  docker compose -f docker-compose.kafka.yml exec -T workspace \
    bash scripts/stop_live_demo.sh || {
      echo "WARN: live demo stop/serving reported an issue; inspect logs/live_demo before stopping Docker." >&2
    }
else
  echo "[1/3] Workspace is not running; skipping live demo shutdown."
fi

echo "[2/3] Stopping optional Airflow services..."
docker compose -f docker-compose.airflow.yml down --remove-orphans

echo "[3/3] Stopping core Kafka/Spark/Postgres/Metabase services..."
docker compose -f docker-compose.kafka.yml down --remove-orphans

echo
echo "StreamBharat services are stopped. Named data volumes have been preserved."
echo "Do not use 'docker compose down -v' unless you intentionally want to erase Kafka/PostgreSQL data."
