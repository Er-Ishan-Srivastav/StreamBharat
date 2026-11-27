#!/usr/bin/env python3
"""Verify Metabase can authenticate and that all committed cards are on dashboards."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bootstrap_metabase import (  # noqa: E402
    BASE_URL,
    COLLECTION_NAME,
    MetabaseAPI,
    _list_payload,
    parse_dashboard_spec,
)


def verify_metabase() -> dict[str, int]:
    api = MetabaseAPI(BASE_URL)
    api.wait_until_ready()
    api.authenticate()

    databases = _list_payload(api.request("GET", "/api/database"))
    database = next(
        (item for item in databases if item.get("name") == "StreamBharat Warehouse"),
        None,
    )
    if database is None:
        raise RuntimeError("Metabase warehouse database is missing")
    database_id = database["id"]

    collections = _list_payload(api.request("GET", "/api/collection"))
    collection = next(
        (item for item in collections if item.get("name") == COLLECTION_NAME),
        None,
    )
    if collection is None:
        raise RuntimeError(f"Metabase collection is missing: {COLLECTION_NAME}")

    cards = _list_payload(api.request("GET", "/api/card"))
    cards_by_name = {item.get("name"): item for item in cards}
    dashboards = {
        item.get("name"): item
        for item in _list_payload(api.request("GET", "/api/dashboard"))
    }

    specs = parse_dashboard_spec(
        Path("config/dashboard/streambharat-metabase-questions.md")
    )
    checked_dashboards = 0
    checked_cards = 0
    failures = []

    for spec in specs:
        dashboard_name = spec["name"]
        dashboard = dashboards.get(dashboard_name)
        if dashboard is None:
            failures.append(f"Missing dashboard: {dashboard_name}")
            continue

        details = api.request("GET", f"/api/dashboard/{dashboard['id']}")
        attached_ids = {
            item.get("card_id")
            for item in details.get("dashcards", [])
            if item.get("card_id") is not None
        }
        checked_dashboards += 1

        for question in spec["questions"]:
            card_name = f"{dashboard_name} — {question['name']}"
            card = cards_by_name.get(card_name)
            if card is None:
                failures.append(f"Missing saved question: {card_name}")
                continue
            card_id = card.get("id")
            if card_id not in attached_ids:
                failures.append(f"Question is not attached to its dashboard: {card_name}")
                continue
            dataset_query = card.get("dataset_query") or {}
            card_database_id = dataset_query.get("database")
            if card_database_id is not None and card_database_id != database_id:
                failures.append(f"Question targets the wrong database: {card_name}")
                continue
            checked_cards += 1

    if failures:
        for failure in failures:
            print(f"FAIL  {failure}", file=sys.stderr)
        raise RuntimeError(
            f"Metabase verification found {len(failures)} issue(s); "
            f"verified {checked_dashboards} dashboards and {checked_cards} cards"
        )

    result = {
        "dashboards": checked_dashboards,
        "cards": checked_cards,
        "expected_cards": sum(len(item["questions"]) for item in specs),
    }
    if checked_cards != result["expected_cards"]:
        raise RuntimeError(
            f"Expected {result['expected_cards']} dashboard cards, verified {checked_cards}"
        )
    print(
        f"Metabase verification passed: {result['dashboards']} dashboards, "
        f"{result['cards']} saved questions, warehouse database id={database_id}."
    )
    return result


if __name__ == "__main__":
    try:
        verify_metabase()
    except Exception as exc:
        print(f"Metabase verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
