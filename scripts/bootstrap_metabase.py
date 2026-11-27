#!/usr/bin/env python3
"""Idempotently provision Metabase, its warehouse connection, questions and dashboards."""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


BASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@streambharat.local")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "StreamBharat2026!")
WAREHOUSE_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ott_local_dev")
WAREHOUSE_HOST = os.getenv("POSTGRES_HOST", "postgres")
QUESTIONS_FILE = Path("config/dashboard/streambharat-metabase-questions.md")
COLLECTION_NAME = "StreamBharat Operations & Business Intelligence"


class MetabaseAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session_id = None

    def request(self, method: str, path: str, payload=None, authenticated=True):
        headers = {"Accept": "application/json"}
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload).encode("utf-8")
        if authenticated and self.session_id:
            headers["X-Metabase-Session"] = self.session_id
        request = urllib.request.Request(
            self.base_url + path, data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Metabase API {method} {path} returned HTTP {exc.code}: {detail[:1200]}"
            ) from exc

    def wait_until_ready(self, timeout_seconds=240):
        deadline = time.monotonic() + timeout_seconds
        last_error = None
        while time.monotonic() < deadline:
            try:
                result = self.request("GET", "/api/health", authenticated=False)
                if result.get("status") == "ok":
                    return
            except Exception as exc:
                last_error = exc
            time.sleep(3)
        raise RuntimeError(f"Metabase did not become healthy: {last_error}")

    def authenticate(self):
        properties = self.request(
            "GET", "/api/session/properties", authenticated=False
        )
        if properties.get("setup-token"):
            self.request(
                "POST",
                "/api/setup",
                {
                    "token": properties["setup-token"],
                    "user": {
                        "first_name": "StreamBharat",
                        "last_name": "Admin",
                        "email": ADMIN_EMAIL,
                        "password": ADMIN_PASSWORD,
                    },
                    "prefs": {
                        "site_name": "StreamBharat",
                        "site_locale": "en",
                        "allow_tracking": False,
                    },
                },
                authenticated=False,
            )
            result = self.request(
                "POST",
                "/api/session",
                {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
                authenticated=False,
            )
            self.session_id = result["id"]
            print("Created and authenticated the initial Metabase administrator.")
        else:
            result = self.request(
                "POST",
                "/api/session",
                {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
                authenticated=False,
            )
            self.session_id = result["id"]
            print("Authenticated with the existing Metabase administrator.")


def _list_payload(result):
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("data", result.get("items", []))
    return []


def parse_dashboard_spec(path: Path):
    dashboards = []
    dashboard = None
    question = None
    sql_lines = []
    in_sql = False

    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## Dashboard "):
            title = line.split("—", 1)[-1].strip()
            dashboard = {"name": title, "questions": []}
            dashboards.append(dashboard)
            question = None
        elif dashboard and line.startswith("**") and line.endswith("**"):
            question = line.strip("*").strip()
        elif dashboard and line.strip() == "```sql":
            in_sql = True
            sql_lines = []
        elif in_sql and line.strip() == "```":
            sql = "\n".join(sql_lines).strip()
            if question and sql:
                dashboard["questions"].append({"name": question, "sql": sql})
            in_sql = False
            question = None
        elif in_sql:
            sql_lines.append(line)

    return dashboards


def main():
    api = MetabaseAPI(BASE_URL)
    api.wait_until_ready()
    api.authenticate()

    databases = _list_payload(api.request("GET", "/api/database"))
    database = next(
        (item for item in databases if item.get("name") == "StreamBharat Warehouse"),
        None,
    )
    if database is None:
        database = api.request(
            "POST",
            "/api/database",
            {
                "name": "StreamBharat Warehouse",
                "engine": "postgres",
                "details": {
                    "host": WAREHOUSE_HOST,
                    "port": int(os.getenv("POSTGRES_PORT", "5432")),
                    "dbname": os.getenv("POSTGRES_DB", "ottdw"),
                    "user": os.getenv("POSTGRES_USER", "ott"),
                    "password": WAREHOUSE_PASSWORD,
                    "ssl": False,
                },
            },
        )
        print("Connected Metabase to the StreamBharat PostgreSQL warehouse.")
    database_id = database["id"]

    collections = _list_payload(api.request("GET", "/api/collection"))
    collection = next(
        (item for item in collections if item.get("name") == COLLECTION_NAME),
        None,
    )
    if collection is None:
        collection = api.request(
            "POST",
            "/api/collection",
            {
                "name": COLLECTION_NAME,
                "description": "Executive performance, streaming operations, finance and data quality.",
            },
        )
    collection_id = collection["id"]

    cards = _list_payload(api.request("GET", "/api/card"))
    cards_by_name = {item.get("name"): item for item in cards}
    dashboards = parse_dashboard_spec(QUESTIONS_FILE)
    dashboard_index = {
        item.get("name"): item
        for item in _list_payload(api.request("GET", "/api/dashboard"))
    }

    for dashboard_spec in dashboards:
        dashboard_name = dashboard_spec["name"]
        dashboard = dashboard_index.get(dashboard_name)
        if dashboard is None:
            dashboard = api.request(
                "POST",
                "/api/dashboard",
                {
                    "name": dashboard_name,
                    "description": f"StreamBharat {dashboard_name} dashboard.",
                    "collection_id": collection_id,
                    "parameters": [],
                },
            )
            print(f"Created dashboard: {dashboard_name}")
        dashboard_id = dashboard["id"]
        details = api.request("GET", f"/api/dashboard/{dashboard_id}")
        dashcards = list(details.get("dashcards", []))
        present_card_ids = {item.get("card_id") for item in dashcards}
        new_dashcards = []

        for index, question in enumerate(dashboard_spec["questions"]):
            card_name = f"{dashboard_name} — {question['name']}"
            card = cards_by_name.get(card_name)
            if card is None:
                card = api.request(
                    "POST",
                    "/api/card",
                    {
                        "name": card_name,
                        "description": f"Auto-provisioned from {QUESTIONS_FILE.as_posix()}",
                        "display": "table",
                        "visualization_settings": {},
                        "dataset_query": {
                            "database": database_id,
                            "type": "native",
                            "native": {
                                "query": question["sql"],
                                "template-tags": {},
                            },
                        },
                        "collection_id": collection_id,
                        "type": "question",
                    },
                )
                cards_by_name[card_name] = card
                print(f"Created question: {card_name}")
            card_id = card["id"]
            if card_id not in present_card_ids:
                row = (len(dashcards) + len(new_dashcards)) // 2 * 5
                col = ((len(dashcards) + len(new_dashcards)) % 2) * 12
                new_dashcards.append({
                    "id": -1 - len(new_dashcards),
                    "card_id": card_id,
                    "row": row,
                    "col": col,
                    "size_x": 12,
                    "size_y": 5,
                    "parameter_mappings": [],
                    "visualization_settings": {},
                    "series": [],
                })

        if new_dashcards:
            payload = {
                "name": dashboard_name,
                "description": f"StreamBharat {dashboard_name} dashboard.",
                "collection_id": collection_id,
                "parameters": details.get("parameters", []),
                "dashcards": dashcards + new_dashcards,
            }
            api.request("PUT", f"/api/dashboard/{dashboard_id}", payload)
        print(
            f"Dashboard ready: {dashboard_name} "
            f"http://localhost:3000/dashboard/{dashboard_id}"
        )

    print("Metabase warehouse connection, collection, saved questions and dashboards are ready.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Metabase bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)
