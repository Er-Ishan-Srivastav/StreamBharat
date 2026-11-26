# Airflow orchestration

The Airflow stack now contains a runnable local DAG for the implemented Kafka-to-Gold slice:
`streambharat_nightly`.

## Start order

The Kafka/Postgres/Metabase compose stack creates the shared Docker network. Start it
first, create the source dataset on the host at `data/raw/`, then start Airflow:

```bash
docker compose -f docker-compose.kafka.yml up -d --build
docker compose -f docker-compose.airflow.yml up -d --build
```

Open the Airflow UI at http://localhost:8081. The local development login is
`admin` / `admin_local_dev`; override it with `AIRFLOW_ADMIN_PASSWORD` in your
environment. Change the default before sharing the stack or exposing the UI beyond localhost.

## DAG execution order

1. Validate the required seven source feeds, catalogue, genre mapping and campaign file.
2. Create or validate the thirteen workbook Kafka topics.
3. Replay source files by selected event/arrival date into the seven Kafka source topics.
4. Ingest each source topic to its independent append-only Bronze Delta table.
5. Validate source records and write malformed/invalid events to Delta DLQ and `ott.dlq`.
6. Reconcile Bronze offsets within the broker's current retained range.
7. Build Silver player events, correct online device clocks, preserve offline client time and MERGE event IDs.
8. Build Gold daily watch time, DAU/MAU and top titles.
9. Transactionally upsert available Gold outputs into Postgres.

The scheduled run uses the Airflow logical date (`ds`) for source replay. For a full
historical backfill, trigger the DAG manually with this JSON configuration:

```json
{
  "start_date": "2026-10-01",
  "end_date": "2026-10-31"
}
```

The DAG requires the complete course dataset under `data/raw/`. The Compose file mounts
the repository, dataset and lake paths into the Airflow containers and shares the
`streambharat-network` with Kafka and Postgres. Airflow runs Spark in local mode inside
the scheduler worker process; no separate Spark master/worker cluster is configured.

## Important scope boundary

This DAG operationalises the implemented source/player/subscription/engagement slice.
Sessionisation, the ad-break/impression stream join, full financial reconciliation and
royalties, streaming metrics/lag monitoring, saved Metabase dashboard provisioning and
the AWS deployment remain outstanding. A DAG appearing in
the UI does not establish successful data processing; inspect each task log and compare
the resulting Delta and Postgres row counts.
