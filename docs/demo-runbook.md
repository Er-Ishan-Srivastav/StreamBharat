# StreamBharat local demonstration runbook

This runbook is for a short local demonstration when the supplied course dataset is unavailable. It uses a small deterministic synthetic dataset and exercises the central local data path.

## Important data boundary

The generated data is synthetic. It is not the supplied course dataset and must not be described as real platform activity or used to claim the original workbook's historical results. The generator writes under `data/raw/`; do not run it over a directory containing the original dataset unless you have backed that data up.

## 1. Initialize infrastructure

From the repository root in PowerShell:

```powershell
Copy-Item .env.example .env
# Review local credentials in .env, then:
bash scripts/start_local_stack.sh
```

The initializer builds and starts Kafka, Spark, PostgreSQL, Metabase and Airflow, provisions the workbook Kafka topics, and runs infrastructure smoke checks. The full course-data preflight is intentionally skipped when `data/raw/` is absent.

## 2. Run the synthetic end-to-end demo

Execute the runner inside the workspace container:

```powershell
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh
```

Optional arguments are demo date and user count:

```powershell
docker compose -f docker-compose.kafka.yml exec -T workspace bash scripts/run_synthetic_demo.sh 2026-10-01 5
```

The runner performs these steps:

1. Generate compact player, billing, search, ratings and advertising source files plus one catalogue snapshot.
2. Replay all seven source feeds into Kafka.
3. Ingest the seven source topics into append-only Bronze Delta tables.
4. Validate source records and route deliberately invalid records to the DLQ.
5. Build Silver player events with device-clock correction and subscription SCD2.
6. Build Gold engagement, active-user, top-title, viewing-session and drop-off outputs.
7. Publish available Gold/Silver Delta tables to PostgreSQL.
8. Refresh the Metabase warehouse connection, questions and dashboards.

## 3. Services and verification

| Service | Local URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark master UI | http://localhost:8082 |
| Spark worker UI | http://localhost:8083 |
| Metabase | http://localhost:3000 |
| Airflow | http://localhost:8081 |
| JupyterLab | http://localhost:8888 |

Verify Delta outputs in `lake/bronze/`, `lake/silver/` and `lake/gold/`. In PostgreSQL, inspect schema `ottdw`, especially `watch_time_daily`, `active_users_daily`, `top_titles_daily`, `subscriptions_scd2`, `viewing_sessions` and `content_dropoff_daily`.

A successful synthetic run proves the local vertical slice only. It does not validate every Q1–Q20 workbook task, full historical reconciliation, production throughput, AWS deployment or the original course dataset.

## 4. Original course dataset

When the original dataset becomes available, place its `data/raw/` tree in the repository and run:

```bash
python scripts/preflight_dataset.py --data-root data/raw
bash scripts/trigger_local_full_run.sh 2026-10-01 2026-10-31
```

Review the Airflow task logs and data-quality outputs before presenting full-dataset results.
