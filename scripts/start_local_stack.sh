#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Review and change local credentials before exposing any service."
fi

if [[ ! -d data/raw ]]; then
  echo "Missing data/raw. Copy the supplied StreamBharat dataset to data/raw without modifying the source files." >&2
  exit 2
fi

echo "[1/4] Starting Kafka, Spark workspace, Postgres and Metabase..."
docker compose -f docker-compose.kafka.yml up -d --build

echo "[2/5] Creating and verifying the 13 workbook Kafka topics..."
bash config/kafka-topics.sh

echo "[3/5] Checking the complete source/reference dataset...
docker compose -f docker-compose.kafka.yml exec -T workspace \
  python scripts/preflight_dataset.py --data-root data/raw

echo "[4/5] Waiting for local warehouse and workspace health..."
docker compose -f docker-compose.kafka.yml ps

echo "[5/5] Starting Airflow on the shared Docker network..."
docker compose -f docker-compose.airflow.yml up -d --build
docker compose -f docker-compose.airflow.yml ps

cat <<'EOF'

StreamBharat local stack is started.

Kafka UI:   http://localhost:8080
Jupyter:    http://localhost:8888
Metabase:   http://localhost:3000
Airflow:    http://localhost:8081
Postgres:   localhost:5432 / ottdw

Next: bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31

Use the Airflow UI to inspect task states and logs. This script starts the local
services and validates source-file presence; it does not claim that the full
Spark/Kafka/Postgres data run has succeeded.
EOF
