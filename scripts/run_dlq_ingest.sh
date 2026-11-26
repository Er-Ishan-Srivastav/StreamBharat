#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}"
export SPARK_EXTRA_PACKAGES="${KAFKA_PACKAGE}"

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS:-8}" \
  src/streambharat/dlq_ingest.py "$@"
