#!/usr/bin/env bash
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}"
export SPARK_EXTRA_PACKAGES="${KAFKA_PACKAGE},${SPARK_EXTRA_PACKAGES:-}"

export PYTHONPATH="${PYTHONPATH:-src}"
python -m streambharat.bronze_ingest "$@"
