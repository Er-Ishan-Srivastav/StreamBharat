# Airflow orchestration

The local Airflow stack runs the current full batch ETL dependency graph in
`streambharat_nightly`. It orchestrates Kafka replay, Bronze/DLQ, master data,
Silver player/backfill, subscriptions, engagement, sessions, ad delivery, finance,
data quality, CDN cost and Postgres serving.

## Start order

Copy `.env.example` to `.env` and replace the local passwords/token with
values for this machine. Place the course dataset under `data/raw/` without
modifying source files, then run:

```bash
bash scripts/start_local_stack.sh
```

The helper creates `.env` from `.env.example` only when one does not exist,
lets Docker Compose create the shared network, starts Kafka, Spark master/worker,
Postgres, Metabase and Jupyter, creates/verifies the 13 topic contracts, runs
infrastructure smoke checks and provisions the Metabase warehouse connection
and dashboards. Airflow is optional on 16 GB laptops; start it separately with
`docker compose -f docker-compose.airflow.yml up -d --build`. Dataset preflight
is attempted when `data/raw/` exists but does not block infrastructure startup
when the complete course dataset is absent or incomplete.

Open Airflow at http://localhost:8081. Local development login: `admin` /
`admin_local_dev`. Override local passwords with environment variables and do
not expose the local stack publicly.

The Kafka stack provides Kafka UI at http://localhost:8080, JupyterLab at
http://localhost:8888, Metabase at http://localhost:3000 and Postgres at
localhost:5432. The Airflow stack shares `streambharat-network`.

## DAG dependency graph

1. Preflight checks all seven event feeds, master/reference files, six monthly
   catalogue snapshots, July–October player arrival files and both CDN logs.
2. Create/validate the thirteen workbook topics and load catalogue, licences,
   users, profiles, city/state, partners, plans, genre mappings and CDN references.
3. Replay the seven source topics for the selected date range; run independent
   append-only Bronze consumers and source DLQ validation.
4. Audit retained Kafka offsets against Bronze, normalize player v1/v2 schema,
   calculate device clock offsets and MERGE Silver player events by event_id.
5. Run the restartable July–September arrival-day backfill. Completed source
   files are tracked in a Delta manifest; a retry safely re-MERGEs any file whose
   write completed before its manifest update.
6. Build subscription SCD2 and compacted current state with the seven-day
   payment-failure grace period.
7. Build daily watch time, DAU/MAU and top titles; 30-minute sessions and
   drop-off; campaign delivery and hourly ad fill; revenue reconciliation and
   partner royalties; CDN regional egress and cost.
8. Run DQ rules, write quarantine and fail the DAG when FAIL-severity rows exist.
9. Transactionally replace the requested date slice in Postgres and upsert
   all non-date-partitioned reference/history tables.

## Scheduled and manual run modes

A scheduled run replays its Airflow logical date. For a single-day rerun, use:

```json
{
  "date": "2026-10-15",
  "start_date": "2026-10-15",
  "end_date": "2026-10-15"
}
```

For the first full historical load, use the one-command trigger after both
Compose stacks are healthy:

```bash
bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31
```

Or trigger manually in the Airflow UI with:

```json
{
  "full_refresh": true,
  "start_date": "2026-10-01",
  "end_date": "2026-10-31"
}
```

This replays the selected source arrival range with controlled late, duplicate
and poisoned messages, runs the July–September player backfill, rebuilds
engagement/session/ad-delivery Gold across all available dates and loads
available Delta rows into Postgres. The Airflow producer replay is at-least-once
across task retries; downstream event/business-key MERGE and deduplication
provide sink-level correctness. The optional clean replay module uses
`lake/state/kafka_replay.sqlite3` as its durable acknowledgement ledger.
Later daily runs omit
`full_refresh` and only replace the logical-date slice for date-partitioned
outputs. The backfill manifest skips completed historical file days.

The DAG uses retries, a six-hour task timeout, and an eight-hour SLA. SLA email
delivery requires an SMTP backend; the local stack does not configure one.

## Manual backfill / diagnostic commands

Run the July–September player backfill independently:

```bash
docker compose -f docker-compose.kafka.yml exec workspace \
  python -m streambharat.backfill_player_history \
  --data-root data/raw --start-date 2026-07-01 --end-date 2026-09-30
```

The output tables include `lake/gold/recon_player_daily` and
`lake/gold/recon_september_arrivals`; the latter measures September viewing
events that reached the server in October.

For real-time Q9/Q10, start `run_session_stream.sh` and
`run_ad_matching_stream.sh` before the source replay. For Q14/Q15, start the
`run_kafka_stream.sh plays|ads` consumers and `python producers/live.py
--stream both --seconds 900` in separate workspace terminals. These continuous
streams are independent of the finite nightly batch DAG.

## Warehouse / Metabase views

On a fresh Postgres volume, `001_warehouse.sql` and
`003_dashboard_views.sql` are mounted in initialization order.

For an existing Postgres volume, initialization scripts do not replay automatically.
Apply both idempotent warehouse DDL and dashboard views when the schema changes
(including the Q17 `live_trending` table):

```bash
docker compose -f docker-compose.kafka.yml exec -T postgres \
  psql -U ott -d ottdw < sql/001_warehouse.sql
docker compose -f docker-compose.kafka.yml exec -T postgres \
  psql -U ott -d ottdw < sql/003_dashboard_views.sql
```

Then connect Metabase to database `ottdw`, schema `ottdw`. See
`config/dashboard/streambharat-metabase-questions.md` for the cards to save.

## Runtime validation boundary

A DAG visible in the UI is not a successful data run. Inspect every task log,
compare Kafka retained offset ranges to Bronze, verify Delta schemas/counts,
inspect the DQ gate and validate Postgres totals against the source workbook.
The local Compose topology uses one Kafka broker and a single-worker Spark
standalone cluster; it is a development integration environment, not a
production HA cluster. AWS deployment
is intentionally deferred.
