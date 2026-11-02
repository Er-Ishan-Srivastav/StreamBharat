"""End-to-end batch orchestration for Q4 -> Q20.

Q17 is intentionally run as a separate long-running stream alongside the
simulator; see README.md for the two-terminal launch sequence.
"""

from __future__ import annotations

import argparse

from .q4_incremental_ingestion import run_q4
from .q20_churn import run_q20


def run_batch_pipeline(
    playback_input: str = "data/raw/player",
    bronze_path: str = "data/delta/bronze/playback",
    q4_checkpoint: str = "data/checkpoints/q4_playback",
    subscription_path: str = "data/raw/billing/subscription_events.json",
    users_path: str = "data/raw/app/users.json",
    model_path: str = "artifacts/models/q20_churn_logreg",
    churn_output: str = "data/delta/gold/churn_scores",
    metrics_path: str = "artifacts/reports/q20_metrics.json",
    as_of: str = "2026-10-31",
) -> None:
    run_q4(playback_input, bronze_path, q4_checkpoint, available_now=True)
    run_q20(bronze_path, subscription_path, users_path, model_path,
            churn_output, metrics_path, as_of=as_of)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the StreamBharat Q4 -> Q20 batch pipeline")
    parser.add_argument("--as-of", default="2026-10-31")
    parser.add_argument("--playback-input", default="data/raw/player")
    parser.add_argument("--bronze", default="data/delta/bronze/playback")
    args = parser.parse_args()
    run_batch_pipeline(playback_input=args.playback_input, bronze_path=args.bronze, as_of=args.as_of)


if __name__ == "__main__":
    main()
