#!/usr/bin/env bash
set -euo pipefail
python -m streambharat.q4_incremental_ingestion "$@"
