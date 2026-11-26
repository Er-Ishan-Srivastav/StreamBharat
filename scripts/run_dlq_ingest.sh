#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}"

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "$KAFKA_PACKAGE" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-8}" \
  src/streambharat/dlq_ingest.py "$@"
