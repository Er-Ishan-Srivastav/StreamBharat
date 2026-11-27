#!/usr/bin/env python3
"""Fetch a current, CC0-licensed film catalogue slice from Wikidata SPARQL.

This downloads descriptive metadata only. It does not create OTT behaviour,
ratings, playback history, customer records, licences, payments, or ad delivery.
Those are generated separately by generate_hybrid_dataset.py.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ENDPOINT = "https://query.wikidata.org/sparql"
SOURCE_PAGE = "https://www.wikidata.org/wiki/Wikidata:Licensing"
LICENSE = "CC0-1.0"

QUERY_TEMPLATE = """
SELECT ?item ?itemLabel ?releaseDate ?runtime ?articleTitle
       (GROUP_CONCAT(DISTINCT STR(?genreLabel); separator="|") AS ?genres)
       (GROUP_CONCAT(DISTINCT STR(?languageLabel); separator="|") AS ?languages)
WHERE {
  ?item wdt:P31 wd:Q11424;
        wdt:P577 ?releaseDate.
  FILTER(YEAR(?releaseDate) >= %YEAR%)
  OPTIONAL { ?item wdt:P2047 ?runtime. }
  OPTIONAL { ?item wdt:P136 ?genre. }
  OPTIONAL { ?item wdt:P364 ?language. }
  OPTIONAL {
    ?article schema:about ?item;
             schema:isPartOf <https://en.wikipedia.org/>;
             schema:name ?articleTitle.
  }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en".
  }
}
GROUP BY ?item ?itemLabel ?releaseDate ?runtime ?articleTitle
ORDER BY DESC(?releaseDate)
LIMIT %LIMIT%
"""


def request_catalog(query: str, user_agent: str, timeout: int) -> dict:
    url = ENDPOINT + "?" + urlencode({"query": query, "format": "json"})
    request = Request(
        url,
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": user_agent,
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _value(binding: dict, key: str, default: str = "") -> str:
    return str(binding.get(key, {}).get("value", default))


def normalize_rows(payload: dict) -> list[dict]:
    grouped: dict[str, dict] = {}
    for binding in payload.get("results", {}).get("bindings", []):
        qid = _value(binding, "item").rsplit("/", 1)[-1]
        title = _value(binding, "itemLabel").strip()
        if not qid or not title or title.lower() == "unknown":
            continue
        release_value = _value(binding, "releaseDate")
        release_date = release_value[:10] if release_value else ""
        if not release_date:
            continue

        row = grouped.setdefault(
            qid,
            {
                "wikidata_qid": qid,
                "title": title,
                "release_date": release_date,
                "runtime_minutes": None,
                "genres": [],
                "original_languages": [],
                "wikipedia_title": None,
                "source_url": "https://www.wikidata.org/wiki/" + qid,
            },
        )
        runtime = _value(binding, "runtime")
        if runtime and row["runtime_minutes"] is None:
            try:
                row["runtime_minutes"] = round(float(runtime), 2)
            except ValueError:
                pass
        genres = _value(binding, "genres")
        languages = _value(binding, "languages")
        row["genres"] = sorted(set(row["genres"]) | {x for x in genres.split("|") if x})
        row["original_languages"] = sorted(
            set(row["original_languages"]) | {x for x in languages.split("|") if x}
        )
        article = _value(binding, "articleTitle")
        if article and not row["wikipedia_title"]:
            row["wikipedia_title"] = article

    rows = list(grouped.values())
    rows.sort(key=lambda item: (item["release_date"], item["wikidata_qid"]), reverse=True)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/external/wikidata/catalog.json")
    parser.add_argument("--min-year", type=int, default=2018)
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument(
        "--user-agent",
        default="StreamBharat-Hybrid-Data-Lab/1.0 (https://github.com/Er-Ishan-Srivastav/StreamBharat)",
        help="Use a descriptive User-Agent; add a project contact address when publishing.",
    )
    args = parser.parse_args()

    if not 1900 <= args.min_year <= datetime.now(timezone.utc).year:
        parser.error("--min-year must be a valid year no later than the current year")
    if not 10 <= args.limit <= 1000:
        parser.error("--limit must be between 10 and 1000")

    query = QUERY_TEMPLATE.replace("%YEAR%", str(args.min_year)).replace(
        "%LIMIT%", str(args.limit)
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    try:
        payload = request_catalog(query, args.user_agent, args.timeout)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"ERROR: Wikidata request failed: {exc}", file=sys.stderr)
        return 2

    titles = normalize_rows(payload)
    if len(titles) < 10:
        print(
            f"ERROR: expected at least 10 usable catalogue records; received {len(titles)}. "
            "Inspect the Wikidata response or retry later.",
            file=sys.stderr,
        )
        return 3

    manifest = {
        "dataset": "wikidata_film_catalogue",
        "schema_version": "1.0",
        "source": "Wikidata Query Service",
        "source_endpoint": ENDPOINT,
        "source_policy": SOURCE_PAGE,
        "license": LICENSE,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "query_min_release_year": args.min_year,
        "requested_limit": args.limit,
        "record_count": len(titles),
        "records": titles,
    }
    output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print("PUBLIC CATALOGUE SNAPSHOT WRITTEN")
    print(f"  records: {len(titles)}")
    print(f"  licence: {LICENSE}")
    print(f"  retrieved_at_utc: {manifest['retrieved_at_utc']}")
    print(f"  output: {output.resolve()}")
    print("  note: descriptive metadata only; no OTT activity is present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
