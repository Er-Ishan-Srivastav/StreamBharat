"""Generate realistic live play and ad events for the Q14/Q15 Kafka streams.

Run from the repository root:
  python producers/live.py --stream both --seconds 900
  python producers/live.py --stream plays
  python producers/live.py --stream ads
"""
import argparse
import csv
import json
import random
import time
from datetime import timedelta

from live_sink import add_args, make_write, now_ist

parser = argparse.ArgumentParser()
parser.add_argument("--stream", choices=["plays", "ads", "both"], default="both")
parser.add_argument("--seconds", type=int, default=0, help="0 runs until Ctrl+C")
parser.add_argument("--seed", type=int, default=9)
parser.add_argument("--catalog", default="data/raw/content/catalog_2026-10-01.json")
parser.add_argument("--campaigns", default="data/raw/ads/ad_campaigns.csv")
add_args(parser)
args = parser.parse_args()
rng = random.Random(args.seed)

with open(args.catalog, encoding="utf-8") as handle:
    catalog_data = json.load(handle)
# Course snapshots are arrays; the compact synthetic demo may be a single object.
catalog = catalog_data if isinstance(catalog_data, list) else [catalog_data]
titles = [
    (item["content_id"], item["languages"][0], rng.paretovariate(1.3))
    for item in catalog
    if isinstance(item, dict)
    and item.get("type") in ("movie", "series")
    and item.get("languages")
]
if not titles:
    raise RuntimeError("No movie/series titles with languages in the October catalogue")
weights = [weight for _, _, weight in titles]

with open(args.campaigns, encoding="utf-8", newline="") as handle:
    campaigns = list(csv.DictReader(handle))
if not campaigns:
    raise RuntimeError(f"{args.campaigns} contains no campaigns")

users = [f"U{i:06d}" for i in range(1, 2501)]
surge_pool = rng.sample(titles, min(6, len(titles)))
overspender = rng.choice(campaigns)["campaign_id"]
sticky_users = rng.sample(users, 15)

TOPICS = {"plays": "ott.live.plays", "ads": "ott.live.ads"}
KEYS = {"plays": "user_id", "ads": "user_id"}
write = make_write(args, TOPICS, KEYS, "streambharat-live")

event_no = impression_no = 0
started = time.time()
print(f"Sending live events to {args.bootstrap if not args.dry_run else '_dryrun/'}; Ctrl+C to stop")
try:
    while not args.seconds or time.time() - started < args.seconds:
        now = now_ist()
        elapsed = time.time() - started
        surge = surge_pool[int(elapsed // 180) % len(surge_pool)]
        surge_strength = min(1.0, (elapsed % 180) / 60)
        plays, ads = [], []

        if args.stream in ("plays", "both"):
            for _ in range(rng.randint(60, 100)):
                event_no += 1
                title = (
                    surge if rng.random() < 0.18 * surge_strength
                    else rng.choices(titles, weights=weights)[0]
                )
                user = rng.choice(users)
                event_time = now - timedelta(seconds=rng.randint(0, 4))
                if rng.random() < 0.03:
                    event_time -= timedelta(minutes=rng.randint(2, 12))
                plays.append({
                    "event_id": f"LP{event_no:09d}",
                    "user_id": user,
                    "profile_id": f"{user}-P1",
                    "content_id": title[0],
                    "language": title[1],
                    "event_ts": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
                })

        if args.stream in ("ads", "both"):
            for _ in range(rng.randint(40, 70)):
                impression_no += 1
                user = rng.choice(users)
                campaign = rng.choice(campaigns)
                if rng.random() < 0.25:
                    campaign = next(
                        item for item in campaigns
                        if item["campaign_id"] == overspender
                    )
                if rng.random() < 0.08:
                    user = rng.choice(sticky_users)
                    campaign = campaigns[sum(map(ord, user)) % len(campaigns)]
                ads.append({
                    "impression_id": f"LI{impression_no:09d}",
                    "user_id": user,
                    "campaign_id": campaign["campaign_id"],
                    "ad_break_ts": (
                        now - timedelta(seconds=rng.randint(0, 4))
                    ).strftime("%Y-%m-%dT%H:%M:%S"),
                    "duration_sec": rng.choice([10, 15, 20, 30]),
                    "cpm_inr": int(float(campaign["cpm_inr"])),
                })

        if args.stream in ("plays", "both"):
            write("plays", plays)
        if args.stream in ("ads", "both"):
            write("ads", ads)
        time.sleep(2)
except KeyboardInterrupt:
    pass
print("Live simulator stopped.")
