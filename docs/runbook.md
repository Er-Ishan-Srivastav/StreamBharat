# StreamBharat operator runbook — local-first

## Start of day

1. Keep the raw dataset in `data/raw/`; do not modify it.
2. Run `bash scripts/start_local_stack.sh`. It creates `.env` from the template
   if needed, starts the local services, checks source files and starts Airflow.
3. Verify health with `docker compose -f docker-compose.kafka.yml ps` and
   `docker compose -f docker-compose.airflow.yml ps`.
4. Run `docker compose -f docker-compose.kafka.yml exec workspace pytest -q`
   before triggering a full data run.

## Scheduled batch pipeline

Trigger `streambharat_nightly` in Airflow. The DAG:
- validates all seven source feeds, monthly catalog snapshots, supporting references and CDN logs;
- creates/validates the thirteen Kafka topics;
- loads master/reference data;
- replays the selected source arrival range to Kafka with controlled late,
  duplicate, poison and corrected messages;
- consumes seven independent Bronze Delta paths and routes invalid records to Delta + `ott.dlq`;
- reconciles retained broker offsets to Bronze;
- processes player clock offsets/schema v1/v2 and backfills July–September by server-arrival day;
- builds subscription SCD2, engagement, sessions/drop-off, ad delivery, revenue reconciliation, partner royalties, DQ and CDN cost;
- fails on FAIL-severity data-quality rows;
- atomically refreshes the requested date slice and upserts history/reference outputs to Postgres.

For the first historical load, use:
`bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31`.
This triggers `full_refresh=true`, runs the selected replay range, executes
the July–September arrival-day backfill and rebuilds date-partitioned Gold/serving
outputs. For a single-day rerun, use DAG run configuration:
`{"date":"2026-10-15","start_date":"2026-10-15","end_date":"2026-10-15"}`.

Airflow uses the workbook chaos replay producer: it deliberately emits late,
duplicate and poisoned source messages, with corrected re-sends. A task retry
or a new DAG run can create new Kafka offsets; producer idempotence only covers
the producer's internal retry protocol. Silver MERGE and business-key
deduplication are therefore mandatory. For a separate clean replay, use
`python -m streambharat.replay_kafka`; its SQLite acknowledgement ledger is
`lake/state/kafka_replay.sqlite3`. Keep it across clean replays. The historical
playback backfill is file-day restartable using
`lake/silver/player_backfill_files`. The resulting
`lake/gold/recon_september_arrivals` table quantifies September views that
arrived in October; read the measured result after the dataset run.

## Streaming checkpoints

- Each topic/query has its own checkpoint under `lake/_checkpoints`.
- Do not share checkpoints between topics or query plans.
- Preserve a checkpoint across a normal restart.
- For a poisoned checkpoint, stop the query, preserve a copy for diagnosis,
  inspect the last committed offsets and restart with a controlled new checkpoint.
- A producer replay is a new Kafka record; protect downstream outputs with
  event/business-key deduplication.

## Live streams

Run the Q17/Q18 consumers in separate workspace terminals:
```bash
bash scripts/run_kafka_stream.sh plays
bash scripts/run_kafka_stream.sh ads
```
In another terminal run the workbook Kafka producer:
`python producers/live.py --stream both --seconds 900`. It writes directly to
`ott.live.plays` and `ott.live.ads`. The original file-based simulator remains
available with `python ott_stream_simulator.py --stream both` and its Kafka
bridge. Streaming progress writes throughput,
batch duration, maximum event time, freshness and broker high-watermark lag to
`lake/gold/stream_metrics`.

## BI / Postgres

On a new Postgres volume, `001_warehouse.sql` and
`003_dashboard_views.sql` run at initialization. The startup helper then
runs `scripts/bootstrap_metabase.py` to create the local administrator,
warehouse connection, saved SQL questions and three dashboards. For an
existing volume, apply schema changes manually:
```bash
docker compose -f docker-compose.kafka.yml exec -T postgres \
  psql -U ott -d ottdw < sql/003_dashboard_views.sql
```
Open Metabase at http://localhost:3000. Login credentials are configured in
`.env` as `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD`. The three
dashboards and saved SQL questions are provisioned by the startup helper.

## Stop services

Stop Spark streaming queries gracefully first, then stop simulator/producers.
Use `docker compose down` to stop infrastructure. Do not use `docker compose down -v`
unless you intentionally want to erase local Kafka/Postgres data volumes.

## Alert destinations

| Event | Destination |
|---|---|
| Malformed/invalid source event | `ott.dlq` and `lake/bronze/dlq` |
| Live trending title | `ott.metrics.trending` |
| Ad pacing/frequency cap alert | `ott.alerts.ad_pacing` |
| Current subscription snapshot | `ott.ref.subscription_state` |
| Stream rate/lag/freshness | `lake/gold/stream_metrics` and Postgres Operations view |
| Daily DLQ counts by source/reason | `lake/gold/dlq_metrics_daily` and `ottdw.v_dlq_metrics_daily` |
| DQ FAIL row | Delta quarantine and Airflow fail gate |

## Recovery and exact-once boundary

Spark-to-Delta is checkpointed and merge-based where specified; the Kafka alert
sink is at-least-once across the crash window between Kafka acknowledgement and
the alert-ID Delta ledger commit. Do not describe the whole platform as
end-to-end exactly-once.

For broker outage, restart Kafka first and resume Spark with the original
checkpoint. For Postgres serving failures, inspect staging tables and retry the
transactional loader. Capture logs, offsets and row counts for every failure
drill before calling the pipeline production-ready.

The local stack uses one Kafka broker and a single-worker Spark standalone
cluster. Airflow batch tasks run local Spark inside the Airflow container.
AWS deployment is intentionally deferred.
