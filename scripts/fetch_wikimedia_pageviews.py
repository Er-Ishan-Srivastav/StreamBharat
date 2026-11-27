#!/usr/bin/env python3
"""Fetch daily English Wikipedia pageviews for Wikidata-linked catalogue titles.

Pageviews are a public-interest proxy, not OTT viewing or customer telemetry.
Requests are made sequentially and the output is a dated CC0 snapshot.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
LICENSE = "CC0-1.0"


def fetch_pageviews(
    title: str, start: date, end: date, user_agent: str, timeout: int
) -> list[dict]:
    article = quote(title.replace(" ", "_"), safe="")
    url = (
        f"{BASE_URL}/en.wikipedia.org/all-access/user/{article}/daily/"
        f"{start:%Y%m%d}00/{end:%Y%m%d}00"
    )
    request = Request(url, headers={"Accept": "application/json", "User-Agent": user_agent})
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload.get("items", [])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="data/external/wikidata/catalog.json")
    parser.add_argument("--output", default="data/external/wikimedia/pageviews.jsonl")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--articles", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--user-agent",
        default="StreamBharat-Hybrid-Data-Lab/1.0 (https://github.com/Er-Ishan-Srivastav/StreamBharat)",
        help="Use a descriptive User-Agent; add a project contact address when publishing.",
    )
    args = parser.parse_args()

    if not 1 <= args.days <= 365:
        parser.error("--days must be between 1 and 365")
    if not 1 <= args.articles <= 50:
        parser.error("--articles must be between 1 and 50")

    catalog_path = Path(args.catalog)
    if not catalog_path.is_file():
        print(
            f"ERROR: {catalog_path} not found. Run fetch_wikidata_catalog.py first.",
            file=sys.stderr,
        )
        return 2

    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    titles = [
        row for row in catalog.get("records", [])
        if row.get("wikipedia_title")
    ][: args.articles]
    if not titles:
        print("ERROR: no catalogue records have an English Wikipedia article.", file=sys.stderr)
        return 3

    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    failed_articles = []
    with output.open("w", encoding="utf-8") as handle:
        for index, title in enumerate(titles, start=1):
            article = title["wikipedia_title"]
            items = None
            error_message = None
            for attempt in range(3):
                try:
                    items = fetch_pageviews(article, start, end, args.user_agent, args.timeout)
                    time.sleep(1)
                    break
                except HTTPError as exc:
                    error_message = f"HTTP {exc.code}"
                    if exc.code not in (429, 500, 502, 503, 504):
                        break
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                    error_message = str(exc)
                    retry_after = None
                else:
                    retry_after = None
                if attempt < 2:
                    try:
                        delay = max(1, min(int(retry_after), 60)) if retry_after else 2 ** (attempt + 1)
                    except (TypeError, ValueError):
                        delay = 2 ** (attempt + 1)
                    time.sleep(delay)

            if items is None:
                failed_articles.append({"title": article, "error": error_message})
                print(f"  [{index}/{len(titles)}] unavailable: {article} ({error_message})")
                continue

            for item in items:
                timestamp = str(item.get("timestamp", ""))
                if len(timestamp) < 8:
                    continue
                row = {
                    "wikidata_qid": title["wikidata_qid"],
                    "wikipedia_title": article,
                    "date": f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}",
                    "pageviews": int(item.get("views", 0)),
                    "access": item.get("access", "all-access"),
                    "agent": item.get("agent", "user"),
                    "source": "Wikimedia Analytics API",
                    "license": LICENSE,
                    "retrieved_at_utc": retrieved_at,
                    "provenance": "public_external",
                    "measure_semantics": "Wikipedia article pageviews; not OTT plays or watch time",
                }
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                total_rows += 1
            print(f"  [{index}/{len(titles)}] {article}: {len(items)} daily rows")

    summary = {
        "source": "Wikimedia Analytics API",
        "license": LICENSE,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "requested_articles": len(titles),
        "pageview_rows": total_rows,
        "failed_articles": failed_articles,
        "output": str(output),
    }
    summary_path = output.with_suffix(".manifest.json")
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print("PUBLIC INTEREST SNAPSHOT WRITTEN")
    print(f"  rows: {total_rows}")
    print(f"  failed_articles: {len(failed_articles)}")
    print(f"  output: {output.resolve()}")
    if total_rows == 0:
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
