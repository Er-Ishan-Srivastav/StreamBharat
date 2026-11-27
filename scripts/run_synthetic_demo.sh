#!/usr/bin/env bash
set -euo pipefail

# Run this from the workspace container after start_local_stack.sh succeeds.
# The generated records are synthetic and deliberately small.
export PYTHONPATH="${PYTHONPATH:-/workspace/src}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$KAFKA_BOOTSTRAP_SERVERS}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
export SPARK_SESSION_TIMEZONE="${SPARK_SESSION_TIMEZONE:-UTC}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"

DEMO_DATE="${1:-2026-10-01}"
USERS="${2:-5}"

echo "[1/8] Generate small synthetic OTT source feeds..."
python scripts/generate_demo_data.py --data-root data/raw --date "$DEMO_DATE" --users "$USERS"

echo "[2/8] Replay the seven source feeds to Kafka..."
python producers/replay.py --data data/raw --from "$DEMO_DATE" --to "$DEMO_DATE" \
  --speed 0 --seed 42 --no-chaos --bootstrap "$KAFKA_BOOTSTRAP"

echo "[3/8] Ingest Kafka source topics into Bronze Delta..."
bash scripts/run_bronze_ingest.sh --starting-offsets earliest --available-now

echo "[4/8] Validate source events and write DLQ records..."
bash scripts/run_dlq_ingest.sh --starting-offsets earliest --available-now

echo "[5/8] Build Silver player events and subscription SCD2..."
python -m streambharat.silver_player \
  --bronze lake/bronze/player_events \
  --silver lake/silver/player_events \
  --offsets lake/silver/device_clock_offsets
python -m streambharat.subscription_state --bootstrap-servers "$KAFKA_BOOTSTRAP"

echo "[6/8] Build Gold engagement and viewing-session analytics..."
python -m streambharat.gold_analytics \
  --player lake/silver/player_events \
  --catalog "data/raw/content/catalog_${DEMO_DATE}.json" \
  --gold-root lake/gold
python -m streambharat.session_analytics \
  --player lake/silver/player_events \
  --catalog "data/raw/content/catalog_${DEMO_DATE}.json"

echo "[7/8] Publish available Gold/Silver Delta tables to PostgreSQL..."
python -m streambharat.serve_postgres

echo "[8/8] Refresh Metabase questions and dashboards..."
python scripts/bootstrap_metabase.py

echo
echo "SYNTHETIC DEMO PIPELINE FINISHED."
echo "Verify Delta paths under lake/bronze, lake/silver and lake/gold."
echo "Verify PostgreSQL schema ottdw and open http://localhost:3000."
echo "The original course dataset has NOT been processed by this demo."
