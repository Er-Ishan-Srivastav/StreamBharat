#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Review local passwords and tokens before exposing services."
fi

echo "[1/5] Building and starting Kafka, Spark workspace, Postgres and Metabase..."
docker compose -f docker-compose.kafka.yml up -d --build

echo "[2/5] Creating and verifying the 13 workbook Kafka topics..."
bash config/kafka-topics.sh

if [[ -d data/raw ]]; then
  echo "[3/5] Validating the supplied StreamBharat dataset..."
  docker compose -f docker-compose.kafka.yml exec -T workspace \
    python scripts/preflight_dataset.py --data-root data/raw
else
  echo "[3/5] Dataset not found; skipping data preflight."
  echo "      Copy the supplied dataset's data/raw directory into ./data/raw before replaying or triggering Airflow."
fi

echo "[4/5] Checking core service status..."
docker compose -f docker-compose.kafka.yml ps

echo "[5/5] Starting Airflow on the shared Docker network..."
docker compose -f docker-compose.airflow.yml up -d --build
docker compose -f docker-compose.airflow.yml ps

cat <<'EOF'

StreamBharat local stack is initialized.

Kafka UI:   http://localhost:8080
Jupyter:    http://localhost:8888
Metabase:   http://localhost:3000
Airflow:    http://localhost:8081
Postgres:   localhost:5432 / ottdw

After placing the dataset in data/raw/, trigger the historical run:
  bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31

This script initializes services and Kafka topics. It does not claim that the
full data pipeline has run; inspect Airflow task logs after triggering it.
EOF
