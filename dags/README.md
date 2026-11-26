# Airflow orchestration

The local Airflow stack runs the current full batch ETL dependency graph in
`streambharat_nightly`. It orchestrates Kafka replay, Bronze/DLQ, master data,
Silver player/backfill, subscriptions, engagement, sessions, ad delivery, finance,
data quality, CDN cost and Postgres serving.

## Start order

Place the course dataset under `data/raw/` without modifying source files, then run:

```bash
docker compose -f docker-compose.kafka.yml up -d --build
docker compose -f docker-compose.kafka.yml exec workspace python scripts/preflight_dataset.py --data-root data/raw
docker compose -f docker-compose.airflow.yml up -d --build
```

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

For a bounded source replay, set `start_date` and `end_date` to the desired
arrival dates. The historical player backfill remains a separate July–September
step and is automatically skipped file-by-file after its manifest is complete.

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

For live simulator streams, start the Kafka consumers separately:

```bash
docker compose -f docker-compose.kafka.yml exec workspace bash scripts/run_kafka_stream.sh plays
docker compose -f docker-compose.kafka.yml exec workspace bash scripts/run_kafka_stream.sh ads
```

In another workspace terminal, run `python ott_stream_simulator.py --stream both`
to generate live play and ad messages. These continuous streams are not part
of the nightly finite batch DAG.

## Warehouse / Metabase views

On a fresh Postgres volume, `001_warehouse.sql` and
`003_dashboard_views.sql` are mounted in initialization order.

For an existing Postgres volume, manually apply the dashboard views once:

```bash
docker compose -f docker-compose.kafka.yml exec -T postgres \
  psql -U ott -d ottdw < sql/003_dashboard_views.sql
```

Then connect Metabase to database `ottdw`, schema `ottdw`. See
`config/dashboard/streambharat-metabase-questions.md` for the cards to save.

## Runtime validation boundary

A DAG visible in the UI is not a successful data run. Inspect every task log,
compare Kafka retained offset ranges to Bronze, verify Delta schemas/counts,
inspect the DQ gate and validate Postgres totals against the source workbook.
The local Compose topology uses one Kafka broker and Spark local mode; it is
a development integration environment, not a production cluster. AWS deployment
is intentionally deferred.
