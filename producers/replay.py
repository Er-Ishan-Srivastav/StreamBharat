"""Run the seven-source Kafka replay from the StreamBharat project package.

Examples:
  python producers/replay.py --start-date 2026-10-01 --end-date 2026-10-31 --speed 0
  python producers/replay.py --topics player_events,app_search --speed 10
"""
from streambharat.replay_kafka import main

if __name__ == "__main__":
    main()
