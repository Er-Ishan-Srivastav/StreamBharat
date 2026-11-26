# Airflow DAGs

\`streambharat_nightly.py\` is the initial DAG scaffold for the ETL workbook sequence.

Start Airflow after the DAG's referenced job modules and dependencies have been built:

\`\`\`bash
docker compose -f docker-compose.airflow.yml up -d
\`\`\`

Open the UI at http://localhost:8081.

Important: the DAG deliberately represents the target ordering, but its task commands still require workbook-specific CLI interfaces. It must not be scheduled until these interfaces and the quality report contract are implemented and tested in the Airflow container.
