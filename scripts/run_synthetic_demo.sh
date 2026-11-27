#!/usr/bin/env bash
set -euo pipefail

# Run this from the workspace container after start_local_stack.sh succeeds.
# Synthetic inputs live under data/demo so the supplied course dataset is not overwritten.
export PYTHONPATH="${PYTHONPATH:-/workspace/src}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$KAFKA_BOOTSTRAP_SERVERS}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
export SPARK_SESSION_TIMEZONE="${SPARK_SESSION_TIMEZONE:-UTC}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"

DEMO_DATE="${1:-2026-10-15}"
USERS="${2:-5}"
DEMO_DATA_ROOT="${DEMO_DATA_ROOT:-data/demo/raw}"
CATALOG_PATH="$DEMO_DATA_ROOT/content/catalog_${DEMO_DATE}.json"

echo "[1/13] Generate isolated synthetic OTT source feeds..."
python scripts/generate_demo_data.py --data-root "$DEMO_DATA_ROOT" --date "$DEMO_DATE" --users "$USERS"

echo "[2/13] Replay the seven source feeds to Kafka..."
python producers/replay.py --data "$DEMO_DATA_ROOT" --from "$DEMO_DATE" --to "$DEMO_DATE" \
  --speed 0 --seed 42 --no-chaos --bootstrap "$KAFKA_BOOTSTRAP"

echo "[3/13] Ingest Kafka source topics into Bronze Delta..."
bash scripts/run_bronze_ingest.sh --starting-offsets earliest --available-now

echo "[4/13] Validate source events and write DLQ records..."
bash scripts/run_dlq_ingest.sh --starting-offsets earliest --available-now

echo "[5/13] Reconcile retained Kafka offsets against Bronze..."
python -m streambharat.bronze_reconcile

echo "[6/13] Build Silver player events and subscription SCD2..."
python -m streambharat.silver_player \
  --bronze lake/bronze/player_events \
  --silver lake/silver/player_events \
  --offsets lake/silver/device_clock_offsets
python -m streambharat.subscription_state --bootstrap-servers "$KAFKA_BOOTSTRAP"

echo "[7/13] Build Gold engagement and viewing-session analytics..."
python -m streambharat.gold_analytics \
  --player lake/silver/player_events \
  --catalog "$CATALOG_PATH" \
  --gold-root lake/gold
python -m streambharat.session_analytics \
  --player lake/silver/player_events \
  --catalog "$CATALOG_PATH"

echo "[8/13] Build ad delivery, campaign pacing and fill-rate analytics..."
python -m streambharat.ad_matching \
  --campaigns "$DEMO_DATA_ROOT/ads/ad_campaigns.csv" \
  --catalog "$CATALOG_PATH"

echo "[9/13] Reconcile subscription charges and calculate partner royalties..."
python -m streambharat.finance_royalties \
  --subscription lake/bronze/billing_subscr \
  --settlement lake/bronze/billing_settle \
  --partners "$DEMO_DATA_ROOT/reference/content_partners.csv" \
  --catalog-dir "$DEMO_DATA_ROOT/content"

echo "[10/13] Aggregate DLQ records by source and reason..."
python -m streambharat.dlq_metrics

echo "[11/13] Publish Silver/Gold Delta tables to PostgreSQL..."
python -m streambharat.serve_postgres

echo "[12/13] Verify non-empty Delta and PostgreSQL outputs..."
python scripts/verify_demo_outputs.py

echo "[13/13] Refresh Metabase questions and dashboards..."
python scripts/bootstrap_metabase.py

echo
echo "SYNTHETIC DEMO PIPELINE FINISHED."
echo "Inspect Delta paths under lake/bronze, lake/silver and lake/gold."
echo "Inspect PostgreSQL schema ottdw and open http://localhost:3000."
echo "Synthetic inputs were written to $DEMO_DATA_ROOT."
echo "The supplied course dataset under data/raw has NOT been modified or processed."
