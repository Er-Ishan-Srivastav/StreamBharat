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

The initializer builds and starts Kafka, Spark, PostgreSQL and Metabase, provisions the workbook Kafka topics, waits for PostgreSQL, reapplies the warehouse DDL/views (including upgrades to an existing named volume), and runs infrastructure smoke checks. Airflow is optional to keep memory usage lower on 16 GB laptops; start it separately only when needed. The full course-data preflight is intentionally skipped when `data/raw/` is absent.

To start Airflow later for DAG orchestration, run `docker compose -f docker-compose.airflow.yml up -d --build` from the repository root. It uses the shared Spark cluster.

## 2. Run the synthetic end-to-end demo

Execute the runner inside the workspace container:

```bash
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh
```

The default date is 15 October 2026, the Android player schema-v2 rollout date, so the replay demonstrates the versioned `position_sec` → `position_ms` payload change. Optional arguments are demo date and user count:

```bash
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh 2026-10-15 5
```

The runner performs fourteen steps and keeps a timestamped log under
`logs/synthetic_demo/`. On failure it records `latest_failure.txt` with the
failed stage and log location; on success it writes `latest_success.txt`.
The verifier checks minimum row counts for all seven source feeds, executes
every committed dashboard SQL query against PostgreSQL and confirms that
Metabase contains all three dashboards and their saved questions.

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
14. Verify the Metabase warehouse connection, all three dashboards and every saved question.

## 3. Cloud-hosted acceptance workflow

Before local setup, the repository's **Actions → StreamBharat local stack
acceptance → Run workflow** workflow can build the Docker Compose services,
initialize Kafka/PostgreSQL/Metabase, execute the same synthetic pipeline and
upload diagnostic logs for seven days. Choose a synthetic user count from 1 to
20. This is a repeatable cloud-hosted integration check, not a replacement for
running the stack on the developer's machine.

## 4. Services and verification

| Service | Local URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark master UI | http://localhost:8082 |
| Spark worker UI | http://localhost:8083 |
| Metabase | http://localhost:3000 |
| Airflow | http://localhost:8081 |
| JupyterLab | http://localhost:8888 |

Verify Delta outputs in `lake/bronze/`, `lake/silver/` and `lake/gold/`. In PostgreSQL, inspect schema `ottdw`, especially `watch_time_daily`, `active_users_daily`, `top_titles_daily`, `subscriptions_scd2`, `viewing_sessions`, `ad_delivery`, `campaign_daily`, `revenue_reconciliation`, `royalties_quarterly` and `dlq_metrics_daily`. The Kafka-to-Bronze retained-offset report is written to `lake/gold/bronze_offset_reconciliation`.

A successful synthetic run proves the local vertical slice only. It does not validate every Q1–Q20 workbook task, full historical reconciliation, production throughput, AWS deployment or the original course dataset. See the [dated project status record](project-status-and-validation.md) for the acceptance actually completed on 27 November 2026.

## 5. Recorded local acceptance — 27 November 2026

The following is the observed result of the local 20-user synthetic run. The source date was 15 October 2026 and the source-generation log run ID was 20261127T091200Z. Inputs were written to data/demo/raw/; the original data/raw/ course dataset was not processed.

| Check | Observed result |
|---|---|
| Infrastructure smoke | 5/5 passed |
| Bronze source counts | Player 61; subscriptions 60; settlements 40; search 20; ratings 20; ad breaks 20; ad impressions 20 |
| DLQ | 2 records: INVALID_RATING and MISSING_POSITION |
| Kafka/Bronze offset reconciliation | 22 partitions; all reconciled; all count_delta values 0 |
| Silver | Player events 60; subscriptions SCD2 60 |
| Gold and ad outputs | watch_time_daily 20; active_users_daily 2; top_titles_daily 1; viewing_sessions 20; content_dropoff_daily 1; ad_delivery 20; campaign_daily 1; ad_fill_rate_hourly 1; revenue_reconciliation 40; royalties_quarterly 1; dlq_metrics_daily 2 |
| PostgreSQL serving | 12 required tables populated and verified |
| Warehouse SQL | 20/20 committed SQL questions executed |
| Metabase | 3 dashboards and 20 saved questions verified |
| Local tests | 77 passed |
| GitHub Actions | Python tests run #849 passed; Kafka ETL run #444 passed |

The run initially stopped during Bronze startup because the workspace image exposed python3 rather than python. After updating the shell entry points, the pipeline stages were resumed and verified individually. The first DLQ attempt also exposed a race when seven validations wrote to the same initially absent Delta DLQ table at once; the recovery ran the topic validations sequentially. The synthetic runner now processes the seven DLQ topic aliases sequentially so a fresh shared Delta DLQ table is not initialized concurrently.

**Important reproducibility note:** the corrected one-command wrapper has not yet been independently rerun from start to finish on a fresh isolated set of Kafka topics, checkpoints and volumes. The observed acceptance above is a successful stage-by-stage recovery, not a claim that the original one-shot invocation completed. Do not replay the same synthetic date into the already-populated local stack as an ordinary retry; Kafka offsets, checkpoints, Delta and PostgreSQL state persist. Use a fresh isolated environment for a clean wrapper acceptance.

The full evidence, individual row counts, issue history and remaining work are recorded in [Project history and validation record](project-status-and-validation.md).

## 6. Optional live Q17/Q18 demonstration

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

## 7. Original course dataset

When the original dataset becomes available, place its `data/raw/` tree in the repository and run:

```bash
python scripts/preflight_dataset.py --data-root data/raw
bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31
```

Review the Airflow task logs and data-quality outputs before presenting full-dataset results.


## 8. Stop the local stack safely

After stopping the live demo, stop Airflow and the core services without deleting Kafka/PostgreSQL volumes:

```bash
bash scripts/stop_local_stack.sh
```

The script stops Airflow before the core Compose project, preserves named data volumes and does not run `docker compose down -v`.
