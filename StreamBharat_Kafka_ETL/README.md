# StreamBharat Kafka ETL — canonical local-first project

This folder contains the fully assembled project archive and the implementation
guide for the supplied Kafka ETL workbook.

**Download the complete, self-contained project:** [StreamBharat_Kafka_ETL_complete.tar.gz](StreamBharat_Kafka_ETL_complete.tar.gz)

The archive contains the complete source tree, Spark jobs, Kafka producer/replay,
the Airflow DAG, SQL warehouse schema, notebook, configuration, Spark contract
tests and runbooks. It excludes generated Python caches, local credentials,
raw data and Delta output.

## First initialization

Download the archive from this folder, then extract it in your repository
working directory. The archive recreates the StreamBharat_Kafka_ETL folder.

```bash
tar -xzf StreamBharat_Kafka_ETL_complete.tar.gz
cd StreamBharat_Kafka_ETL
cp .env.example .env
# Set PROJECT_DIR to this directory's absolute host path in .env.
make init
```

make init builds the local Spark/Airflow images, starts Kafka, Kafka UI, Spark
master/worker, Jupyter, Postgres, Airflow and Metabase, and runs the connectivity
smoke test. It does not require the course dataset for the initial smoke check.

For the historical data run, place the supplied dataset's data/raw folder
inside the project and follow the README and docs/runbook.md.

The project targets local Docker initialization first. AWS deployment is
intentionally deferred.
