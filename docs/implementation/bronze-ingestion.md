# Seven-topic Kafka Bronze ingestion

Module: `streambharat.bronze_ingest`.

The job creates one independent Structured Streaming query for each workbook source topic:

| Alias | Topic | Delta table | Checkpoint |
|---|---|---|---|
| player_events | ott.player.events | lake/bronze/player_events | lake/_checkpoints/bronze_player_events |
| billing_subscr | ott.billing.subscriptions | lake/bronze/billing_subscr | lake/_checkpoints/bronze_billing_subscr |
| billing_settle | ott.billing.settlements | lake/bronze/billing_settle | lake/_checkpoints/bronze_billing_settle |
| app_search | ott.app.search | lake/bronze/app_search | lake/_checkpoints/bronze_app_search |
| app_ratings | ott.app.ratings | lake/bronze/app_ratings | lake/_checkpoints/bronze_app_ratings |
| ads_breaks | ott.ads.breaks | lake/bronze/ads_breaks | lake/_checkpoints/bronze_ads_breaks |
| ads_impressions | ott.ads.impressions | lake/bronze/ads_impressions | lake/_checkpoints/bronze_ads_impressions |

Each Bronze record preserves the Kafka value as a UTF-8 string without parsing it. The envelope
stores source topic, partition, offset, broker timestamp, key, original headers and a JSON
representation of the headers. It then adds ingestion timestamp and date. Partitioning is by
ingestion date. The seven query names and checkpoints are independent.

## Start and replay

Start the local platform and create topics first:

```bash
docker compose -f docker-compose.kafka.yml up -d --build
bash config/kafka-topics.sh
```

Replay the supplied dataset into the seven source topics:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  python -m streambharat.replay_kafka --start-date 2026-10-01 --end-date 2026-10-31 --speed 0
```

The replay uses the source timestamp for ordering and the workbook-defined partition keys.
A value of `--speed 0` removes simulated-time delay. Use `--max-events 2000` for a small
pipeline smoke test.

## Run Bronze

For a long-running consumer, start:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_bronze_ingest.sh
```

For a completed replay or bounded catch-up:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_bronze_ingest.sh --available-now
```

The command resolves the matching Spark 3.5.3 Kafka connector. Delta is configured through
the project's Spark session factory. A Maven repository connection is required the first
time the Kafka connector is fetched.

## DLQ validation

Bronze is deliberately an audit copy and does not change or reject source values. Run the
independent validator to classify malformed JSON, missing required fields, invalid timestamps,
numeric values and selected domain constraints:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_dlq_ingest.sh --available-now
```

Invalid records are written to `lake/bronze/dlq` and published to `ott.dlq`. Each row
retains original topic, partition, offset, Kafka timestamp, key, headers, raw value and
reason code. The stable identifier is `source_topic:partition:offset`; Delta DLQ writes
use that identifier for idempotent MERGE.

The Bronze and DLQ consumers use separate checkpoints. Start both in processing mode before
a continuous replay, or run the replay first and then execute both in `--available-now`
mode. Keep checkpoints for normal recovery. A new checkpoint replays from the configured
starting offset and must be treated as an intentional backfill.

## Important reconciliation boundary

Kafka watermark offsets describe currently retained records. A simple count comparison is
valid only when the Bronze checkpoint began at the earliest retained offset and retention
has not removed source records since ingestion. The reconciliation report must retain the
low/high offsets and call out any nonzero low watermark instead of asserting that all
historical Bronze rows equal current broker retention.
