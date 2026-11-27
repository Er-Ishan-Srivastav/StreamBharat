# StreamBharat local demonstration runbook

This runbook is for a short local demonstration when the supplied course dataset is unavailable. It uses a small deterministic synthetic dataset and exercises the central local data path.

## Important data boundary

The generated data is synthetic. It is not the supplied course dataset and must not be described as real platform activity or used to claim the original workbook's historical results. The demo runner writes under `data/demo/raw/`, keeping the supplied course dataset under `data/raw/` untouched. Delta outputs under `lake/` are demo outputs and can replace earlier local lake tables.

## 1. Initialize infrastructure

From the repository root in a WSL2 terminal, check Docker readiness, available memory, disk headroom, Compose configuration and host-port conflicts, then initialize the stack:

```bash
python3 scripts/check_local_environment.py
[ -f .env ] || cp .env.example .env
# Review local credentials in .env, then:
bash scripts/start_local_stack.sh
```

The initializer builds and starts Kafka, Spark, PostgreSQL and Metabase, provisions the workbook Kafka topics, and runs infrastructure smoke checks. Airflow is optional to keep memory usage lower on 16 GB laptops; start it separately only when needed. The full course-data preflight is intentionally skipped when `data/raw/` is absent.

To start Airflow later for DAG orchestration, run `docker compose -f docker-compose.airflow.yml up -d --build` from the repository root. It uses the shared Spark cluster.

## 2. Run the synthetic end-to-end demo

Execute the runner inside the workspace container:

```powershell
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh
```

The default date is 15 October 2026, the Android player schema-v2 rollout date, so the replay demonstrates the versioned `position_sec` → `position_ms` payload change. Optional arguments are demo date and user count:

```powershell
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh 2026-10-15 5
```

The runner performs these steps:

1. Generate isolated player, billing, search, ratings, advertising, campaign and partner files plus a catalogue snapshot.
2. Replay all seven source feeds into Kafka.
3. Ingest the seven source topics into append-only Bronze Delta tables.
4. Validate source records and route deliberately invalid player/rating records to the DLQ.
5. Reconcile Kafka retained offsets against Bronze counts for each source partition.
6. Build Silver player events with device-clock correction and subscription SCD2.
7. Build Gold engagement, active-user, top-title, viewing-session and drop-off outputs.
8. Match ad breaks and impressions and calculate campaign pacing/fill-rate metrics.
9. Reconcile subscription charges to settlements and calculate partner royalties.
10. Aggregate DLQ records by source and reason.
11. Publish available Silver/Gold Delta tables to PostgreSQL.
12. Verify required Bronze, Silver, Gold and PostgreSQL outputs are non-empty.
13. Refresh Metabase questions and dashboards.

## 3. Services and verification

| Service | Local URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark master UI | http://localhost:8082 |
| Spark worker UI | http://localhost:8083 |
| Metabase | http://localhost:3000 |
| Airflow | http://localhost:8081 |
| JupyterLab | http://localhost:8888 |

Verify Delta outputs in `lake/bronze/`, `lake/silver/` and `lake/gold/`. In PostgreSQL, inspect schema `ottdw`, especially `watch_time_daily`, `active_users_daily`, `top_titles_daily`, `subscriptions_scd2`, `viewing_sessions`, `ad_delivery`, `campaign_daily`, `revenue_reconciliation`, `royalties_quarterly` and `dlq_metrics_daily`. The Kafka-to-Bronze retained-offset report is written to `lake/gold/bronze_offset_reconciliation`.

A successful synthetic run proves the local vertical slice only. It does not validate every Q1–Q20 workbook task, full historical reconciliation, production throughput, AWS deployment or the original course dataset.

## 4. Optional live Q17/Q18 demonstration

After the synthetic batch demo has completed, start both Kafka-backed live consumers and a bounded live producer:

```bash
docker compose -f docker-compose.kafka.yml exec -T workspace \
  bash scripts/start_live_demo.sh 2026-10-15 120 both
```

The command starts Q17 trending from `ott.live.plays`, Q18 ad pacing from `ott.live.ads`, and emits synthetic live events for two minutes. Logs and PID files are stored under `logs/live_demo/`. The live producer reads the synthetic catalogue and campaign file under `data/demo/raw/`.

Inspect the output topics in Kafka UI:
- `ott.metrics.trending`
- `ott.alerts.ad_pacing`

Stop the producer and Spark consumers, wait for their final micro-batches/checkpoints, then refresh PostgreSQL and Metabase:

```bash
docker compose -f docker-compose.kafka.yml exec -T workspace \
  bash scripts/stop_live_demo.sh
```

The stop helper publishes Q17 Delta rows to PostgreSQL table `ottdw.live_trending`.
The Metabase card **Live trending titles (Q17)** reads the latest event-time window.

For a lower-resource run, choose one stream: use `plays` or `ads` instead of `both`. Keep Airflow stopped during this live demonstration on a 16 GB laptop.

## 5. Original course dataset

When the original dataset becomes available, place its `data/raw/` tree in the repository and run:

```bash
python scripts/preflight_dataset.py --data-root data/raw
bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31
```

Review the Airflow task logs and data-quality outputs before presenting full-dataset results.


## 6. Stop the local stack safely

After stopping the live demo, stop Airflow and the core services without deleting Kafka/PostgreSQL volumes:

```bash
bash scripts/stop_local_stack.sh
```

The script stops Airflow before the core Compose project, preserves named data volumes and does not run `docker compose down -v`.
