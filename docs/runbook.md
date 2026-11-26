# StreamBharat operator runbook (local first)

## Start of day

1. Verify Docker Desktop has sufficient memory (8 GB minimum; 12–16 GB recommended with Airflow and Metabase).
2. Start infrastructure: \`docker compose -f docker-compose.kafka.yml up -d\`.
3. Create topics: \`bash config/kafka-topics.sh\`.
4. Verify broker and Postgres health: \`docker compose ps\`.
5. Verify the Python/Spark environment: \`python -m streambharat.pipeline --list\` and \`pytest -q\`.
6. Start the seven-topic replay producer supplied with the Kafka ETL kit, then start the topic-specific Bronze consumers.

## Streaming checkpoints

- One checkpoint directory per query under \`lake/_checkpoints/<query-name>\` (or the configured \`data/checkpoints/\` equivalent while migrating).
- Never share checkpoints between different Kafka topics or query plans.
- Preserve the checkpoint on normal restart.
- A poisoned checkpoint requires stopping the query, copying the checkpoint for evidence, identifying the last good offset, creating a new checkpoint directory and replaying from a controlled offset into an idempotent Delta sink. Never delete a checkpoint as the first troubleshooting step.

## Replay one day

Replay a single date with the supplied \`producers/replay.py\` command and \`--speed 0\`, then compare:
- Kafka beginning/end offsets per partition.
- Bronze count grouped by topic and partition.
- Distinct event IDs in Silver.
- Gold daily totals.

The replay engine, not raw-file edits, controls deterministic source replay. Do not invent offsets; read them from Kafka at run time.

## Stop services

Stop Spark streaming queries gracefully first so checkpoints close. Then stop producer processes. Infrastructure can be stopped with \`docker compose down\`. Do not use \`-v\` unless you intentionally want to delete local Kafka/Postgres persistent volumes.

## Alert destinations

| Event | Destination |
|---|---|
| Malformed / semantically invalid source message | \`ott.dlq\` and Bronze DLQ Delta table |
| Trending title | \`ott.metrics.trending\` |
| Overspend / frequency breach | \`ott.alerts.ad_pacing\` |
| Current subscription snapshot | \`ott.ref.subscription_state\` |
| Stream latency / lag / freshness | \`gold.stream_metrics\` and Operations dashboard |
| Quality / reconciliation blocker | Airflow quality gate; DAG fails closed |

## Recovery and correctness

- For broker failure, restore Kafka first; then restart Spark with the original checkpoint and inspect offsets.
- For task retry, retain date arguments and idempotent Delta MERGE keys.
- For producer replay, deduplicate at the business key because a new replay is a new Kafka record.
- For partial Postgres loads, clear/rebuild the staging table and rerun the transactionally guarded upsert.
- Capture before/after counts and logs for every recovery drill.

## Operational evidence required before calling the pipeline production-ready

Broker recovery, mid-replay Spark restart, 15 October schema-v2 replay, business-key deduplication, bronze offset reconciliation, DLQ classification, Postgres transaction recovery, query lag and freshness measurements must all be demonstrated and recorded. Do not claim end-to-end exactly-once without this evidence.
