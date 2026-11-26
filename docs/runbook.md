# StreamBharat operator runbook — local-first

## Start of day

1. Keep the raw dataset in `data/raw/`; do not modify it.
2. Start the local Kafka, Postgres, Spark workspace and Metabase stack:
   `docker compose -f docker-compose.kafka.yml up -d --build`.
3. Run dataset preflight:
   `docker compose -f docker-compose.kafka.yml exec workspace python scripts/preflight_dataset.py --data-root data/raw`.
4. Start Airflow after the Kafka stack has created the shared network:
   `docker compose -f docker-compose.airflow.yml up -d --build`.
5. Verify health with `docker compose ps` in each Compose project.
6. Run `pytest -q` in the workspace before triggering a full data run.

## Scheduled batch pipeline

Trigger `streambharat_nightly` in Airflow. The DAG:
- validates all seven source feeds, monthly catalog snapshots, supporting references and CDN logs;
- creates/validates the thirteen Kafka topics;
- loads master/reference data;
- replays the logical-date source files to Kafka;
- consumes seven independent Bronze Delta paths and routes invalid records to Delta + `ott.dlq`;
- reconciles retained broker offsets to Bronze;
- processes player clock offsets/schema v1/v2 and backfills July–September by server-arrival day;
- builds subscription SCD2, engagement, sessions/drop-off, ad delivery, revenue reconciliation, partner royalties, DQ and CDN cost;
- fails on FAIL-severity data-quality rows;
- atomically refreshes the requested date slice and upserts history/reference outputs to Postgres.

For a single-day rerun, use DAG run configuration:
`{"date":"2026-10-15","start_date":"2026-10-15","end_date":"2026-10-15"}`.

The historical playback backfill is file-day restartable using
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
In another terminal run `python ott_stream_simulator.py --stream both`.
The producer bridge publishes the generated play and ad events to
`ott.live.plays` and `ott.live.ads`. Streaming progress writes throughput,
batch duration, maximum event time, freshness and broker high-watermark lag to
`lake/gold/stream_metrics`.

## BI / Postgres

On a new Postgres volume, `001_warehouse.sql` and
`003_dashboard_views.sql` run at initialization. For an existing volume:
```bash
docker compose -f docker-compose.kafka.yml exec -T postgres \
  psql -U ott -d ottdw < sql/003_dashboard_views.sql
```
Connect Metabase at http://localhost:3000 to `ottdw`; build saved questions
and dashboards from `config/dashboard/streambharat-metabase-questions.md`.

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

The local stack uses one Kafka broker and Spark local mode. AWS deployment is
intentionally deferred.
