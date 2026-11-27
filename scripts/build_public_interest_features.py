#!/usr/bin/env python3
"""Join Wikimedia pageviews to generated StreamBharat content IDs by Wikidata QID.

The output is an external-interest feature table. It is not an OTT view count,
recommendation label, or observed StreamBharat customer interaction.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


def build_features(
    public_catalog_path: str | Path,
    pageviews_path: str | Path,
    hybrid_catalog_path: str | Path,
) -> list[dict]:
    public_catalog = json.loads(Path(public_catalog_path).read_text(encoding="utf-8"))
    public_qids = {
        str(row["wikidata_qid"]): row
        for row in public_catalog.get("records", [])
        if row.get("wikidata_qid")
    }
    hybrid_catalog = json.loads(Path(hybrid_catalog_path).read_text(encoding="utf-8"))
    content_by_qid = {
        str(row["wikidata_qid"]): str(row["content_id"])
        for row in hybrid_catalog
        if row.get("wikidata_qid") and row.get("content_id")
    }

    daily_counts: dict[str, dict[date, int]] = defaultdict(lambda: defaultdict(int))
    with Path(pageviews_path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                qid = str(row["wikidata_qid"])
                day = date.fromisoformat(str(row["date"]))
                views = int(row["pageviews"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"{pageviews_path}:{line_number}: invalid pageview row: {exc}") from exc
            if views < 0:
                raise ValueError(f"{pageviews_path}:{line_number}: pageviews cannot be negative")
            daily_counts[qid][day] += views

    output = []
    for qid, day_values in sorted(daily_counts.items()):
        content_id = content_by_qid.get(qid)
        if not content_id:
            continue
        title = public_qids.get(qid, {}).get("title", "")
        first_day, last_day = min(day_values), max(day_values)
        cursor = first_day
        while cursor <= last_day:
            daily_views = day_values.get(cursor, 0)
            rolling_views = sum(
                day_values.get(cursor - timedelta(days=offset), 0)
                for offset in range(7)
            )
            output.append(
                {
                    "content_id": content_id,
                    "wikidata_qid": qid,
                    "title": title,
                    "metric_date": cursor.isoformat(),
                    "article_pageviews": daily_views,
                    "rolling_7d_article_pageviews": rolling_views,
                    "metric_source": "Wikimedia Analytics API",
                    "provenance": "public_external",
                    "measure_semantics": "Wikipedia article pageviews; not OTT plays or watch time",
                }
            )
            cursor += timedelta(days=1)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-catalog", default="data/external/wikidata/catalog.json")
    parser.add_argument("--pageviews", default="data/external/wikimedia/pageviews.jsonl")
    parser.add_argument(
        "--hybrid-catalog",
        default="data/hybrid/raw/content/catalog_2026-11-01.json",
        help="One generated monthly catalogue snapshot containing Wikidata QIDs.",
    )
    parser.add_argument(
        "--output",
        default="data/external/wikimedia/content_interest_features.csv",
    )
    args = parser.parse_args()

    for source in (args.public_catalog, args.pageviews, args.hybrid_catalog):
        if not Path(source).is_file():
            print(f"ERROR: required input not found: {source}", file=sys.stderr)
            return 2

    try:
        rows = build_features(args.public_catalog, args.pageviews, args.hybrid_catalog)
    except (ValueError, json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: cannot build public-interest features: {exc}", file=sys.stderr)
        return 3
    if not rows:
        print(
            "ERROR: no pageview records could be linked to the selected hybrid catalogue. "
            "Check the catalogue snapshots and pageview QIDs.",
            file=sys.stderr,
        )
        return 4

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "content_id", "wikidata_qid", "title", "metric_date",
        "article_pageviews", "rolling_7d_article_pageviews",
        "metric_source", "provenance", "measure_semantics",
    ]
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    manifest = {
        "dataset": "streambharat_public_content_interest_features",
        "schema_version": "1.0",
        "license": "CC0-1.0",
        "source_semantics": "Wikipedia article pageviews; not OTT plays or watch time",
        "inputs": {
            "public_catalog_sha256": hashlib.sha256(Path(args.public_catalog).read_bytes()).hexdigest(),
            "pageviews_sha256": hashlib.sha256(Path(args.pageviews).read_bytes()).hexdigest(),
            "hybrid_catalog_sha256": hashlib.sha256(Path(args.hybrid_catalog).read_bytes()).hexdigest(),
        },
        "output": output.name,
        "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "row_count": len(rows),
        "content_count": len({row["content_id"] for row in rows}),
    }
    manifest_path = output.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print("PUBLIC-INTEREST FEATURES BUILT")
    print(f"  linked rows: {len(rows)}")
    print(f"  distinct content IDs: {len({row['content_id'] for row in rows})}")
    print(f"  output: {output.resolve()}")
    print(f"  manifest: {manifest_path.resolve()}")
    print("  semantics: external article interest, not OTT consumption.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
