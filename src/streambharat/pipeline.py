"""End-to-end batch orchestrator for all batch questions in StreamBharat.

Q17 and Q18 are intentionally launched as separate long-running file streams.
Use the workbook-specific simulator and the commands in README.md for those two
live demonstrations. The batch path runs Q1-Q16, Q19 and Q20 in dependency order.
"""
from __future__ import annotations

import argparse
import importlib
from collections.abc import Callable


BATCH_ORDER = [
    "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9",
    "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16", "Q19", "Q20",
]
STREAMING_QUESTIONS = {"Q17", "Q18"}


def _invoke(module_name: str, function_name: str, *args, **kwargs) -> None:
    """Import a stage only when it is executed (so dry-run works without Spark)."""
    module = importlib.import_module(f".{{module_name}}", package=__package__)
    getattr(module, function_name)(*args, **kwargs)


def build_steps(as_of: str) -> dict[str, tuple[str, Callable[[], None]]]:
    """Return stage descriptions and lazy actions without importing Spark modules."""
    return {
        "Q1": ("Flatten catalogue and publish content dimensions/bridges",
               lambda: _invoke("q1_catalog", "run_q1")),
        "Q2": ("Clean playback and correct device clocks",
               lambda: _invoke("q2_playback_clean", "run_q2")),
        "Q3": ("Parse both CDN providers and publish usage aggregates",
               lambda: _invoke("q3_cdn", "run_q3")),
        "Q4": ("Incrementally ingest playback to Bronze Delta",
               lambda: _invoke("q4_incremental_ingestion", "run_q4", "data/raw/player",
                               "data/delta/bronze/playback", "data/checkpoints/q4_playback",
                               available_now=True)),
        "Q5": ("Build viewing sessions and daily watch facts",
               lambda: _invoke("q5_sessions", "run_q5")),
        "Q6": ("Build subscription lifecycle and gateway reconciliation",
               lambda: _invoke("q6_subscriptions", "run_q6")),
        "Q7": ("Build search normalisation, conversion and content-gap outputs",
               lambda: _invoke("q7_search", "run_q7")),
        "Q8": ("Build content SCD2, licence audit and August time-travel snapshot",
               lambda: _invoke("q8_content_history", "run_q8")),
        "Q9": ("Build DAU/WAU/MAU and published-metric comparison",
               lambda: _invoke("q9_active_users", "run_q9")),
        "Q10": ("Build regional/national Top 10 rankings",
                lambda: _invoke("q10_top10", "run_q10")),
        "Q11": ("Build subscriber cohorts, retention and MRR bridge",
                lambda: _invoke("q11_cohorts_mrr", "run_q11")),
        "Q12": ("Detect concurrent-screen and multi-city account sharing",
                lambda: _invoke("q12_account_sharing", "run_q12")),
        "Q13": ("Build ad delivery, frequency credits and campaign pacing",
                lambda: _invoke("q13_ads", "run_q13")),
        "Q14": ("Benchmark skew/AQE/salting and calculate CDN title cost",
                lambda: _invoke("q14_skew_optimization", "run_q14")),
        "Q15": ("Allocate partner royalties, guarantees and title statements",
                lambda: _invoke("q15_royalties", "run_q15")),
        "Q16": ("Train ALS and evaluate against language popularity baseline",
                lambda: _invoke("q16_recommendations", "run_q16", as_of=as_of)),
        "Q19": ("Run kids-safety, quality and reconciliation gates",
                lambda: _invoke("q19_quality", "run_q19")),
        "Q20": ("Train churn model and score active paid subscribers",
                lambda: _invoke("q20_churn", "run_q20",
                    playback_path="data/delta/silver/playback_clean",
                    subscription_path="data/raw/billing/subscription_events.json",
                    users_path="data/raw/app/users.json",
                    model_path="artifacts/models/q20_churn_logreg",
                    output_path="data/delta/gold/churn_scores",
                    metrics_path="artifacts/reports/q20_metrics.json",
                    as_of=as_of, top_n=50000)),
    }


def run_batch_pipeline(as_of: str = "2026-10-31", only: list[str] | None = None,
                       skip: list[str] | None = None, dry_run: bool = False) -> list[str]:
    steps = build_steps(as_of)
    selected = [q.upper() for q in only] if only else BATCH_ORDER.copy()
    skipped = {q.upper() for q in (skip or [])}
    invalid = (set(selected) | skipped) - set(BATCH_ORDER) - STREAMING_QUESTIONS
    if invalid:
        raise ValueError(f"Unknown question IDs: {sorted(invalid)}")
    streaming_selected = set(selected) & STREAMING_QUESTIONS
    if streaming_selected:
        raise ValueError(
            "Q17/Q18 are long-running jobs and cannot be part of the finite batch pipeline. "
            "Launch them separately; see README.md."
        )
    order = [q for q in BATCH_ORDER if q in selected and q not in skipped]
    completed: list[str] = []
    for question in order:
        description, action = steps[question]
        print(f"\n[{question}] {description}", flush=True)
        if dry_run:
            completed.append(question)
            continue
        action()
        completed.append(question)
        print(f"[{question}] completed", flush=True)
    return completed


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the StreamBharat batch pipeline")
    parser.add_argument("--as-of", default="2026-10-31")
    parser.add_argument("--only", default=None,
                        help="Comma-separated batch question IDs, e.g. Q1,Q2,Q4,Q5")
    parser.add_argument("--skip", default=None,
                        help="Comma-separated batch question IDs to omit, e.g. Q14,Q16")
    parser.add_argument("--dry-run", action="store_true", help="Print planned steps without running Spark")
    parser.add_argument("--list", action="store_true", help="List batch and streaming questions")
    args = parser.parse_args()
    if args.list:
        print("Batch: " + ", ".join(BATCH_ORDER))
        print("Streaming (separate terminal): Q17, Q18")
        return
    only = [x.strip() for x in args.only.split(",") if x.strip()] if args.only else None
    skip = [x.strip() for x in args.skip.split(",") if x.strip()] if args.skip else None
    completed = run_batch_pipeline(args.as_of, only, skip, args.dry_run)
    print("\nPipeline plan completed: " + ", ".join(completed), flush=True)


if __name__ == "__main__":
    main()
