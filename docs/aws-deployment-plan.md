# AWS deployment plan — deferred until local acceptance

The local platform should become reproducible and observable before any AWS resources are provisioned. Keep the application layer independent of infrastructure by using environment-driven endpoints and configurable paths.

## Proposed service mapping

| Local component | AWS target | Design constraint |
|---|---|---|
| Kafka KRaft | Amazon MSK | Replication, TLS/SASL, topic ACLs, private subnets |
| Delta Lake | Amazon S3 | Delta-compatible Spark runtime, catalog/metastore, lifecycle policies |
| Spark local/Docker | EMR Serverless or EMR on EC2 | Match Spark/Scala/Delta versions and Kafka connector |
| Airflow | MWAA | Private networking, workers and IAM roles |
| PostgreSQL ottdw | RDS PostgreSQL or Aurora PostgreSQL | Private subnets, backups, TLS, connection pooling |
| Metabase | Private ECS/Fargate deployment or alternate BI | Authentication, HTTPS, security groups |
| Secrets | AWS Secrets Manager + IAM roles | No static credentials inside repository or images |
| Logs/metrics | CloudWatch | Retention, alarms, structured logs |

## Deployment gates

1. Local Docker Compose clean install from an empty volume.
2. Seven-topic Bronze offset reconciliation passes.
3. Silver deduplication and schema-v2 replay passes.
4. DLQ reason-code counts are explainable.
5. Gold finance and watch-time reconciliations pass.
6. Stream recovery and duplicate replay drills pass.
7. Postgres staging/upsert tests prove no partial table refresh.
8. Metabase dashboard uses warehouse views, not Spark driver files.
9. Throughput, latency, lag and costs are measured on representative load.
10. Security review approves secrets, topic ACLs, encryption and network exposure.

Only then size MSK brokers, Spark capacity, S3 storage and retention, MWAA environment, RDS instance and dashboard hosting. Do not estimate production sizing based solely on the synthetic training dataset.
