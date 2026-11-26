# Kafka integration — StreamBharat

## Purpose and scope

Extend the workbook implementation with Apache Kafka as a durable event transport while preserving the existing PySpark, Spark Structured Streaming, Delta Lake and business logic.

- Q4 remains the incremental historical-file-to-Bronze process.
- Q17 remains the real-time trending calculation, now with a Kafka source option.
- Q18 remains the real-time ad pacing calculation, now with a Kafka source option.
- Existing JSON-file workflows remain available for workbook compatibility.

## Architecture

\`\`\`mermaid
flowchart LR
  SIM[ott_stream_simulator.py] --> BRIDGE[Kafka producer bridge]
  BRIDGE --> PT[(streambharat.plays)]
  BRIDGE --> AT[(streambharat.ads)]
  PT --> Q17[Spark Q17 consumer]
  AT --> Q18[Spark Q18 consumer]
  Q17 --> BUF[Delta play event buffer]
  BUF --> TREND[10-minute windows / baseline / top 20]
  TREND --> GOLD1[Gold trending Delta]
  Q18 --> LEDGER[Delta impression ledger]
  LEDGER --> PACE[Frequency caps + spend pacing]
  PACE --> GOLD2[Gold live outputs]
\`\`\`

## Topic contracts

| Topic | Kafka key | JSON value | Consumer |
|---|---|---|---|
| \`streambharat.plays\` | \`event_id\` | \`event_id, user_id, profile_id, content_id, language, event_ts\` | Q17 |
| \`streambharat.ads\` | \`impression_id\` | \`impression_id, user_id, campaign_id, ad_break_ts, duration_sec, cpm_inr\` | Q18 |

The producer passes simulator records through without altering their event schema.

## Prerequisites

- Docker Engine / Docker Desktop Compose.
- Python 3.10/3.11 and Java 17.
- Existing PySpark 3.5.3 and Delta Lake 3.2.0 dependencies.
- The \`confluent-kafka\` Python producer client.
- Spark Kafka connector \`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3\`.

The Spark Kafka connector is a JVM package, not a pip dependency. It must match Spark and Scala binary versions. Reference: [Spark 3.5.3 Structured Streaming + Kafka](https://spark.apache.org/docs/3.5.3/structured-streaming-kafka-integration.html).

Install the optional producer dependency:

\`\`\`bash
pip install "confluent-kafka>=2.3,<3"
\`\`\`

## Start broker and topics

\`\`\`bash
docker compose -f docker-compose.kafka.yml up -d
bash config/kafka-topics.sh
\`\`\`

Local host applications use \`localhost:9092\`. The one-node KRaft broker with PLAINTEXT and replication factor 1 is development-only.

## Run the live pipeline

Run each command from the repository root in separate terminals.

**Terminal 1 — event simulator**

\`\`\`bash
python ott_stream_simulator.py --stream both
\`\`\`

**Terminal 2 — Kafka producer bridge**

\`\`\`bash
python -m streambharat.kafka_producer --stream both
\`\`\`

The bridge tails completed JSON files and ignores temporary \`.tmp_\` files. Its source-file registry and event-ID set are held in memory: restarting the producer can replay prior files. Delta business keys make the downstream writes idempotent for the current Q17/Q18 use cases, but production hardening should persist a source cursor/outbox.

**Terminal 3 — Spark Q17**

\`\`\`bash
bash scripts/run_kafka_stream.sh plays
\`\`\`

**Terminal 4 — Spark Q18**

\`\`\`bash
bash scripts/run_kafka_stream.sh ads
\`\`\`

Each query has a dedicated checkpoint:
- \`data/checkpoints/kafka_q17/\`
- \`data/checkpoints/kafka_q18/\`

Do not reuse file-source checkpoints for Kafka-source queries. Default starting offsets are \`earliest\`. Preserve checkpoints on ordinary restarts.

## Processing responsibilities

### Q17 — plays
1. Read the topic with Spark Structured Streaming.
2. Parse Kafka \`value\` using the actual existing \`PLAYS_SCHEMA\`.
3. Retain Kafka key, topic, partition, offset, broker timestamp and raw JSON as lineage.
4. Reuse Q17's \`_persist_and_recompute_trends\` function, Delta event buffer, event-ID deduplication, 10-minute/1-minute sliding windows, late-event recalculation, historical baseline and Gold top-20 output.

### Q18 — ads
1. Read the topic and parse \`value\` using the existing \`AD_SCHEMA\`.
2. Reuse Q18's \`_process_ads_batch\` and Delta impression ledger keyed by \`impression_id\`.
3. Recompute frequency counts and campaign spend from unique ledger rows.
4. Publish the existing capped-user and pacing-alert outputs.

Q4 is intentionally unchanged because it handles historical playback archive ingestion using \`availableNow\`. A future direct-playback Kafka feed can have a separate Kafka-to-Bronze consumer without conflating historical backfill and live event flow.

## Delivery guarantees and recovery

The initial implementation targets at-least-once event consumption with idempotent Delta side effects. Kafka producer idempotence reduces duplicate broker writes during producer retries, but it does not provide one atomic transaction across Kafka offsets and Delta writes. The effective business idempotency keys are \`event_id\` and \`impression_id\`. Prove replay behaviour by restarting queries during controlled test writes and comparing unique ledger counts.

Spark's Kafka source exposes topic, partition, offset, key, value and broker timestamp. Keep a stable unique checkpoint per streaming query.

## Validation checklist

- [ ] Kafka broker starts and both topics are present.
- [ ] Producer delivery callbacks report no failed records.
- [ ] Kafka console consumer can read expected keys and JSON values.
- [ ] Q17 consumes Kafka events and writes trending Delta output.
- [ ] Q18 consumes Kafka events and updates its impression ledger and alerts.
- [ ] Duplicate event/impression IDs do not increase unique Delta counts.
- [ ] Stop/restart Q17 and Q18 with unchanged checkpoints and verify recovery.
- [ ] Restart the producer and verify replay cannot change final unique results.
- [ ] Inject malformed records and verify they are observable.
- [ ] Measure input rows/sec, micro-batch duration, end-to-end latency and consumer lag.
- [ ] Preserve runtime version, schema, row counts and test logs.

The feature is code-integrated but not yet runtime-validated against your local Kafka/Docker/Spark environment. Do not describe the full Kafka system as validated until these checks pass.

## Next hardening phases

1. Persist producer source-file offsets/outbox state and add dead-letter handling.
2. Add schema versioning and explicit event contract validation.
3. Add Kafka-to-Bronze for any direct upstream events, with backfill/replay controls.
4. Add monitoring for broker health, consumer lag, throughput, failed records and checkpoint health.
5. Expand to additional streaming use cases only when event contracts provide the required fields.

## References

- [Spark 3.5.3 Structured Streaming + Kafka](https://spark.apache.org/docs/3.5.3/structured-streaming-kafka-integration.html)
- [Confluent Kafka Python client](https://docs.confluent.io/kafka-clients/python/current/index.html)
- [Apache Kafka delivery semantics](https://kafka.apache.org/design/#semantics)
