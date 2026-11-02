"""StreamBharat live stream simulator for Q17 (trending now) and Q18 (ad pacing and frequency capping).

Run from the dataset folder in a second terminal while your Spark stream runs:
    python ott_stream_simulator.py --stream plays     # Q17
    python ott_stream_simulator.py --stream ads       # Q18
    python ott_stream_simulator.py --stream both --seconds 900

Every 2 seconds it writes one JSON-lines file to:
    data/stream/plays/  event_id, user_id, profile_id, content_id, language, event_ts       (one row per playback start)
    data/stream/ads/    impression_id, user_id, campaign_id, ad_break_ts, duration_sec, cpm_inr

Titles come from content/catalog_2026-10-01.json and campaigns from ads/ad_campaigns.csv.
The streams contain realistic surprises for you to detect.
"""
import argparse, csv, json, os, random, time
from collections import Counter
from datetime import datetime, timedelta

ap = argparse.ArgumentParser()
ap.add_argument("--stream", choices=["plays", "ads", "both"], default="both")
ap.add_argument("--seconds", type=int, default=0)
ap.add_argument("--seed", type=int, default=9)
args = ap.parse_args()
rnd = random.Random(args.seed)

cat = json.load(open("data/raw/content/catalog_2026-10-01.json", encoding="utf-8"))
titles = []
for t in cat:
    if t["type"] in ("movie", "series"):
        titles.append((t["content_id"], t["languages"][0], rnd.paretovariate(1.3)))
weights = [w for _, _, w in titles]
camps = list(csv.DictReader(open("data/raw/ads/ad_campaigns.csv", encoding="utf-8")))
users = [f"U{i:06d}" for i in range(1, 2501)]
for d in ("data/stream/plays", "data/stream/ads"):
    os.makedirs(d, exist_ok=True)

surge_pool = rnd.sample(titles, 6)          # titles that suddenly start trending, one after another
overspender = rnd.choice(camps)["campaign_id"]
sticky_users = rnd.sample(users, 15)          # these users keep getting the same ad


def write(folder, rows, stamp):
    if not rows:
        return
    tmp = f"{folder}/.tmp_{stamp}.json"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in rows) + "\n")
    os.replace(tmp, f"{folder}/{os.path.basename(folder)}_{stamp}.json")


ev = imp = 0
start = time.time()
print("writing to data/stream/ ... Ctrl+C to stop")
try:
    while not args.seconds or time.time() - start < args.seconds:
        now = datetime.now()
        stamp = now.strftime("%Y%m%d_%H%M%S")
        el = time.time() - start
        surge = surge_pool[int(el // 180) % len(surge_pool)]          # a new title surges every 3 minutes
        surge_strength = min(1.0, (el % 180) / 60)                    # ramps up over the first minute
        plays, ads = [], []
        if args.stream in ("plays", "both"):
            for _ in range(rnd.randint(60, 100)):
                ev += 1
                t = surge if rnd.random() < 0.18 * surge_strength else rnd.choices(titles, weights=weights)[0]
                u = rnd.choice(users)
                ts = now - timedelta(seconds=rnd.randint(0, 4))
                if rnd.random() < 0.03:
                    ts -= timedelta(minutes=rnd.randint(2, 12))         # late events
                plays.append({"event_id": f"LP{ev:09d}", "user_id": u, "profile_id": f"{u}-P1", "content_id": t[0], "language": t[1],
                              "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%S")})
        if args.stream in ("ads", "both"):
            for _ in range(rnd.randint(40, 70)):
                imp += 1
                u = rnd.choice(users)
                c = rnd.choice(camps)
                if rnd.random() < 0.25:
                    c = next(x for x in camps if x["campaign_id"] == overspender)   # one campaign spends far too fast
                if rnd.random() < 0.08:
                    u = rnd.choice(sticky_users)
                    c = camps[sum(map(ord, u)) % len(camps)]               # always the same campaign for this user
                ads.append({"impression_id": f"LI{imp:09d}", "user_id": u, "campaign_id": c["campaign_id"],
                            "ad_break_ts": (now - timedelta(seconds=rnd.randint(0, 4))).strftime("%Y-%m-%dT%H:%M:%S"),
                            "duration_sec": rnd.choice([10, 15, 20, 30]), "cpm_inr": int(c["cpm_inr"])})
        write("data/stream/plays", plays, stamp)
        write("data/stream/ads", ads, stamp)
        time.sleep(2)
except KeyboardInterrupt:
    pass
print("stopped")
