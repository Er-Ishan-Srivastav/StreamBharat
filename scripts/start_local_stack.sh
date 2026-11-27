#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Review local passwords and tokens before exposing services."
fi

echo "[1/7] Building and starting Kafka, Spark master/worker, workspace, Postgres and Metabase..."
docker compose -f docker-compose.kafka.yml up -d --build

echo "[2/7] Creating and verifying the 13 workbook Kafka topics..."
bash config/kafka-topics.sh

if [[ -d data/raw ]]; then
  echo "[3/7] Checking whether the full supplied StreamBharat dataset is present..."
  if docker compose -f docker-compose.kafka.yml exec -T workspace \
      python scripts/preflight_dataset.py --data-root data/raw; then
    echo "Full course dataset preflight passed."
  else
    echo "Dataset preflight did not pass; continuing with infrastructure startup."
    echo "A synthetic demo dataset is sufficient for scripts/run_synthetic_demo.sh."
    echo "The full Airflow historical DAG still requires the complete course dataset."
  fi
else
  echo "[3/7] Dataset not found; skipping data preflight."
  echo "      Copy the supplied dataset's data/raw directory into ./data/raw before replaying or triggering Airflow."
fi

echo "[4/7] Waiting for Spark master and worker registration..."
for attempt in $(seq 1 30); do
  if docker compose -f docker-compose.kafka.yml exec -T workspace python -c '
import json, urllib.request
status = json.load(urllib.request.urlopen("http://spark-master:8080/json/", timeout=2))
workers = [worker for worker in status.get("workers", []) if worker.get("state") == "ALIVE"]
assert workers, "No ALIVE Spark workers registered yet"
' >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" -eq 30 ]]; then
    echo "Spark worker did not register with the master. Inspect docker compose logs spark-master spark-worker." >&2
    exit 1
  fi
  sleep 2
done

docker compose -f docker-compose.kafka.yml exec -T workspace python -m streambharat.smoke

echo "[5/7] Checking core service status..."
docker compose -f docker-compose.kafka.yml ps

echo "[6/7] Airflow is optional to preserve memory on 16 GB developer laptops..."
if [[ "${START_AIRFLOW:-false}" == "true" ]]; then
  docker compose -f docker-compose.airflow.yml up -d --build
  docker compose -f docker-compose.airflow.yml ps
else
  echo "Skipping Airflow. Start it later with:"
  echo "  docker compose -f docker-compose.airflow.yml up -d --build"
fi

echo "[7/7] Creating the Metabase warehouse connection, saved questions and dashboards..."
docker compose -f docker-compose.kafka.yml exec -T workspace \
  python scripts/bootstrap_metabase.py

cat <<'EOF'

StreamBharat local stack is initialized.

Kafka UI:        http://localhost:8080
Spark master:    http://localhost:8082
Spark worker:    http://localhost:8083
Spark app UI:    http://localhost:4040
Jupyter:         http://localhost:8888
Metabase:   http://localhost:3000
Airflow (optional): http://localhost:8081
Postgres:   localhost:5432 / ottdw

After placing the dataset in data/raw/, trigger the historical run:
  bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31

This script initializes services and Kafka topics. It does not claim that the
full data pipeline has run; inspect Airflow task logs after triggering it.
EOF
