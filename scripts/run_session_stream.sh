#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-localhost}" \
  src/streambharat/session_stream.py "$@"
