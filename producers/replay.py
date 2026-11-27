"""Replay the seven source systems with the workbook's controlled imperfections.

Examples:
  python producers/replay.py --from 2026-10-01 --to 2026-10-31 --speed 0
  python producers/replay.py --sources player,subscriptions --speed 0 --no-chaos
  python producers/replay.py --dry-run --speed 0
"""
import sources
from replay_engine import main

if __name__ == "__main__":
    main(sources)
