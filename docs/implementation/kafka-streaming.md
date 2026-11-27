# Kafka ETL platform — StreamBharat

## Scope and topic contracts

This extension implements the separate Kafka ETL workbook alongside the original
PySpark workbook. Original Q1–Q20 modules and their orchestrator remain intact.

| Stream | Topic | Partitions | Key |
|---|---|---:|---|
| Player events | `ott.player.events` | 6 | `user_id` |
| Subscription events | `ott.billing.subscr` | 3 | `user_id` |
| Gateway settlements | `ott.billing.settle` | 3 | `user_id` |
| Search logs | `ott.app.search` | 3 | `profile_id` |
| Ratings | `ott.app.ratings` | 1 | `profile_id` |
| Ad breaks | `ott.ads.breaks` | 3 | `user_id` |
| Ad impressions | `ott.ads.impressions` | 3 | `user_id` |
| Live plays | `ott.live.plays` | 6 | `user_id` |
| Live ad impressions | `ott.live.ads` | 6 | `user_id` |
| Dead-letter queue | `ott.dlq` | 1 | source topic/partition/offset |
| Trending output | `ott.metrics.trending` | 1 | window/language/rank |
| Pacing alerts | `ott.alerts.ad_pacing` | 1 | stable alert identifier |
| Current subscription state | `ott.ref.subscription_state` | 3 | `user_id` (compacted) |

Only `ott.ref.subscription_state` uses Kafka log compaction. Trending and ad
pacing outputs are event/update topics and are not compacted by the local topic
provisioner.

The seven source feeds are replayed from the raw course dataset by
`producers/replay.py`. It adds the workbook's late arrivals, upstream
duplicates, poisoned messages followed by corrected re-sends, and the Android
7.3 schema-v2 rollout. The optional `streambharat.replay_kafka` path provides
a clean replay with a durable local acknowledgement ledger. The live
`producers/live.py` simulator writes directly to `ott.live.plays` and
`ott.live.ads`; the original file simulator bridge remains available for the
PySpark workbook's Q17/Q18 baseline.

## Architecture

```mermaid
flowchart LR
  SRC[Seven source feeds] --> RP[Timestamp-ordered replay]
  RP --> K[(Seven source topics)]
  K --> BR[Seven independent Bronze consumers]
  BR --> BD[(Bronze Delta)]
  K --> VAL[Schema and semantic validation]
  VAL --> DLQ[(Delta DLQ + ott.dlq)]
  BD --> SIL[Silver: player / subscription / master]
  SIL --> GOLD[Gold: engagement / ads / finance / DQ / CDN]
  GOLD --> PG[(Postgres ottdw)]
  PG --> MB[Metabase views and dashboards]
  SIM[Live simulator] --> PB[Idempotent Kafka producer]
  PB --> LP[(ott.live.plays)]
  PB --> LA[(ott.live.ads)]
  LP --> TR[5-minute / 30-second trending]
  LA --> AP[Frequency cap and pacing]
  TR --> TRK[(ott.metrics.trending)]
  AP --> APK[(ott.alerts.ad_pacing)]
  AIR[Airflow nightly DAG] --> RP
  AIR --> BR
  AIR --> SIL
  AIR --> GOLD
```

## Local runtime

The local stack includes Kafka 3.8 KRaft, Spark 3.5.3 standalone master/worker,
the PySpark/Jupyter workspace, PostgreSQL 16 and Metabase; Airflow 2.10.4 is an optional orchestration service.
Kafka, Delta 3.2.1 and PostgreSQL connector JARs are preinstalled in the Spark
image. The single broker, PLAINTEXT listeners and replication factor 1 are
development settings, not production security or high availability.

From the repository root:

```bash
cp .env.example .env
bash scripts/start_local_stack.sh
```

Docker Compose creates the named shared network as part of the core stack startup, which then lets the optional Airflow compose file attach as an external network. The initializer builds the core
services, creates and validates the thirteen topic contracts, waits for a live
Spark worker, and runs Kafka/Spark/Delta/Postgres connectivity smoke checks.
It also provisions the Metabase administrator, warehouse connection, saved
questions and three dashboards. Airflow is optional to preserve memory; start it
separately with `docker compose -f docker-compose.airflow.yml up -d --build`.
Dataset preflight is attempted when `data/raw/` exists but does not block
infrastructure startup when the full course dataset is missing or incomplete.

Services:
- Kafka UI: http://localhost:8080
- Spark master UI: http://localhost:8082
- Spark worker UI: http://localhost:8083
- JupyterLab: http://localhost:8888
- Metabase: http://localhost:3000
- Airflow (optional): http://localhost:8081
- Postgres warehouse: localhost:5432, database `ottdw`

The course dataset is not stored in Git. Copy the supplied data folder to
`data/raw/` without editing the source files.

## Source replay, Bronze and DLQ

Airflow replays October source feeds with the workbook producer:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  python producers/replay.py --from 2026-10-01 --to 2026-10-31 \
  --speed 0 --seed 42
```

Chaos is enabled by default: source-specific rates create delayed records,
duplicates, malformed JSON, invalid amounts/timestamps and missing business
keys. Poisoned messages are followed by corrected re-sends. The player source
switches eligible Android 7.3 records to schema version 2 on 15 October 2026.
Use `--no-chaos` for a clean producer feed, or `--dry-run` to inspect the
serialized messages under `_dryrun/`.

For a clean replay with a durable local acknowledgement ledger, use the
alternative implementation:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  python -m streambharat.replay_kafka --start-date 2026-10-01 \
  --end-date 2026-10-31 --speed 0
```

The ledger records source topic/file/line, payload hash and acknowledged Kafka
partition/offset. Keep `lake/state/kafka_replay.sqlite3` across normal clean
replays. The Airflow chaos replay does not claim producer-side exactly-once:
retries may create new Kafka offsets, and Silver MERGE/business-key dedup is
the sink-level protection.

Seven separate Structured Streaming queries write append-only Bronze Delta
tables. Each preserves the raw value string, topic, partition, offset, broker
timestamp, Kafka key, headers, ingestion timestamp and ingestion date. Each
topic has its own checkpoint under `lake/_checkpoints`.

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_bronze_ingest.sh --available-now
docker compose -f docker-compose.kafka.yml exec workspace \
  bash scripts/run_dlq_ingest.sh --available-now
```

The source validator checks identifiers, event timestamps, selected semantic
types, player position schema-v1/schema-v2, subscription amount, ratings and ad
payload constraints. Invalid records are merged to Delta DLQ using the stable
source topic/partition/offset key, then published to `ott.dlq` with original
payload and reason code.

The retained-offset report compares Bronze records with Kafka's current
[low, high) range. It proves completeness only within the broker's retained
range; when the low watermark has advanced, earlier expired offsets cannot be
proven from the broker.

For continuous replay, start Bronze and DLQ without `--available-now` before
replaying. For a completed replay, run them with `--available-now`. Preserve
checkpoints across normal restarts. Daily DLQ counts are aggregated by source,
reason and date into `lake/gold/dlq_metrics_daily` and served through
`ottdw.v_dlq_metrics_daily`.

## Master/reference data and historical backfill

Run the master/reference load:

```bash
python -m streambharat.batch_master --data-root data/raw --output-root lake/silver
```

It loads all monthly catalog snapshots May–October into title/episode history,
genre bridges and parent/episode references; it also loads licences, users,
profiles, city/state, partners, plans, CDN rates and parses AkashCDN and
NimbusEdge September logs into a common request schema. Malformed CDN records
are kept separately.

Run the restartable July–September player backfill:

```bash
python -m streambharat.backfill_player_history \
  --data-root data/raw --start-date 2026-07-01 --end-date 2026-09-30
```

Backfill selection is by the playback file's server-arrival date. Per-device
clock medians are calculated across the selected history. Each arrival-day file
is merged by event_id and recorded in a Delta manifest so retries safely skip
completed files. Outputs include:
- `lake/gold/recon_player_daily`: event date vs arrival date reconciliation
- `lake/gold/recon_september_arrivals`: September viewing events arriving in October

The actual reconciliation counts are generated only when the supplied course
dataset is executed; none are hardcoded.

## Silver player and subscription state

The player Silver transform normalizes schema-v1 `position_sec` and schema-v2
`position_ms`, estimates per-device median online clock offset, preserves
client time for offline download playback, records server-arrival and ingestion
dates separately, and MERGEs by event_id. The backfill can pass one shared
device-offset table across all arrival-day files.

Subscription state is rebuilt from Bronze using event-time ordering. It creates
valid-from/valid-to SCD2 transitions, handles trial/active/paused/payment_failed/
cancelled states, applies a seven-day payment-failure grace period, and publishes
the current per-user state keyed by user_id to compacted
`ott.ref.subscription_state`. Expired payment failures become deterministic
synthetic involuntary cancellations; explicit cancellations remain voluntary.
Malformed and unknown subscription events are quarantined and routed to DLQ.

The subscription job currently rebuilds the learning dataset's full history,
rather than using an incremental production SCD2 state processor.

## Gold engagement and viewing sessions

`streambharat.gold_analytics` produces daily watch time, DAU, rolling 30-day MAU
and top 10 titles per language, rolling episode events to their parent series.
Watched-time credit is limited to forward-progress heartbeat intervals capped
at five minutes and excludes seek jumps.

`streambharat.session_analytics` builds profile/device/content sessions with a
30-minute inactivity boundary, heartbeat time credit, session duration,
completion rate and content drop-off. Both jobs accept a date argument and
replace only that date's Delta slice on rerun.

For near-real-time Q9, `streambharat.session_stream` consumes player Kafka
events with 30-minute session windows and a 12-minute event-time watermark.
It deduplicates event IDs, applies server time to online playback and client
time to offline downloads, credits heartbeat intervals up to five minutes,
caps credit by position/duration/session elapsed time, and writes finalized
session facts to `lake/silver/viewing_sessions_stream`. The batch Silver
pipeline remains the authoritative per-device median clock-correction path.

## Ad delivery and campaign analytics

`streambharat.ad_matching` parses Bronze ad breaks and impressions, links them
by impression_id, user and content within a forward event-time bound, and
classifies delivered, unfilled, filled-without-impression and orphan-impression
rows. It retains Kafka offset lineage, applies campaign genre targets, measures
per-user/campaign/day frequency-cap breaches, computes CPM spend and emits
hourly source break fill-rate metrics. Campaign/day Gold and ad-delivery
outputs are date-rebuildable.

Unfilled breaks do not carry a campaign_id in the supplied source contract, so
they are reported at break/hour level rather than attributed to a campaign.

For near-real-time Q10, `streambharat.ad_matching_stream` runs two
watermarked Kafka stream-stream joins: break-to-impression delivery and
impression-to-break orphan detection. It applies the impression-id/user/content
keys and a forward event-time bound (ad duration plus lateness allowance),
then writes matched/unfilled and orphan facts to separate Delta paths. These
facts are included in the Postgres serving map and dashboard views.

## Finance and partner royalties

`streambharat.finance_royalties` reconciles subscription charge events with
gateway settlements by user, amount, event type and a bounded event-time window.
Each settlement can match at most one charge; charges and unused settlements
are labelled SETTLED, UNSETTLED_CHARGE, DUPLICATE_SETTLEMENT or
ORPHAN_SETTLEMENT.

Partner royalty allocation joins watch time to the catalogue partner valid at
the viewing date, allocates settled subscription revenue using the quarter's
partner watch-time share and revenue-share percentage, then applies the
quarterly minimum guarantee. A published-metric alias map and 0.5% variance
reconciliation job are implemented; actual variances are generated only after
the supplied board file and Gold tables are run locally.

## Data quality and quarantine

Player DQ checks include unknown content, kids-profile age rating,
state-specific expired licences, position beyond duration and missing profile.
Subscription price-plan mismatches compare charged events against the plan
reference. Violations are stored in rule-specific Delta quarantine paths and
summarized into `lake/gold/dq_metrics`; FAIL-severity findings fail the Airflow
quality gate. The quality rule counts and quarantine outputs still require
full-data inspection after the first local run.

## CDN egress and cost

`streambharat.cdn_analytics` joins normalized AkashCDN/NimbusEdge requests to
the regional `cdn_rates.csv` reference. It produces daily request/byte counts,
decimal-GB egress, cost, HTTP 4xx/5xx counts, unique content and requests with
missing rate-card matches. These metrics are served to Postgres for regional
cost analysis.

## Live Kafka streams: trending and ad pacing

The workbook live source is `producers/live.py`; it writes play and ad
events directly to `ott.live.plays` and `ott.live.ads`. The original file
simulator and SQLite-backed bridge remain available for the PySpark workbook.

```bash
# Workspace terminal 1: direct Kafka source
python producers/live.py --stream both --seconds 900
# Workspace terminals 2 and 3: Structured Streaming consumers
bash scripts/run_kafka_stream.sh plays
bash scripts/run_kafka_stream.sh ads
```

For the file-based path instead:

```bash
python ott_stream_simulator.py --stream both
python -m streambharat.kafka_producer --stream both
```

Q17 uses five-minute event-time windows sliding every 30 seconds, a 12-minute
late-event allowance and ranks ten titles per language into
`ott.metrics.trending`. Q18 maintains a Delta impression ledger, recomputes
frequency caps and campaign pacing, and emits frequency/pacing alerts to
`ott.alerts.ad_pacing`. Alert IDs are tracked in Delta to suppress normal
repeat publication.

The Kafka alert sink is at-least-once across a crash between Kafka acknowledgement
and the Delta published-ID ledger commit. Do not describe this as atomic
exactly-once delivery.

## Streaming monitoring

A Structured Streaming listener captures input and processed rates, trigger
duration, maximum event time, freshness and topic lag. Lag is computed as the
broker high watermark minus Spark's processed Kafka end offset per partition,
then aggregated by query/topic; Spark Kafka source offsets are not consumer
group offsets. Metrics are written to `lake/gold/stream_metrics` and served to
Postgres. Runtime behaviour under broker outage/restart still requires a drill.

## Airflow and serving

`streambharat_nightly` orchestrates preflight, master data, topic creation,
source replay, Bronze/DLQ, offset reconciliation, player Silver, July–September
backfill, subscription SCD2, engagement, sessions, ad delivery, finance,
quality gate, CDN cost and Postgres serving. It is date-aware and has retries,
a task timeout and an SLA configuration.

Postgres loading uses a Spark JDBC staging table followed by a transaction:
for date-scoped tables, the target date slice is deleted and replaced from the
stage; history tables use primary-key upserts. DDL lives in
`sql/001_warehouse.sql`; query-ready Metabase views are in
`sql/003_dashboard_views.sql`. The local initializer calls
`scripts/bootstrap_metabase.py` to create the warehouse connection, save the
question SQL and assemble three dashboards. Their queries should be checked
against populated data after the first full run.

## Validation boundary and remaining work

The local compose stack is a single Kafka broker with a Spark 3.5.3
standalone master/worker cluster, PostgreSQL, Metabase and Jupyter; Airflow is optional.
The repository contains Spark contract tests and CI compilation,
but the full 590 MB course dataset has not yet been run end-to-end by this
implementation session. The following evidence is still required:
- successful clean local stack and all thirteen topic partition counts;
- full replay, per-partition Bronze reconciliation and complete source row counts;
- actual DQ violation review and corrected/accepted business exceptions;
- charge/settlement and published board metric variance reconciliation;
- stream stop/restart, Kafka outage, late-event and duplicate/replay drills;
- Metabase bootstrap and saved-question query execution against populated tables;
- measured throughput, latency and job-duration baselines.

AWS deployment is explicitly deferred until the local pipeline and dashboards
have been validated.
