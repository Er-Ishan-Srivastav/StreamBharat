#!/usr/bin/env python3
"""Validate the hybrid source dataset before replay or Spark processing.

This preflight validates the generated hybrid contract. It is intentionally
separate from scripts/preflight_dataset.py, which checks the original course data.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path

EVENT_TYPES = {"start", "heartbeat", "pause", "seek", "stop", "complete"}
AKASH_RE = re.compile(
    r'^(\S+)\s+(\S+)\s+(\S+)\s+"GET /v/([^/]+)/([^/]+)/([^/.]+)\.ts HTTP/[^"]+"\s+(\d{3})\s+(\d+)\s+"([^"]+)"$'
)
NIMBUS_RE = re.compile(
    r'^(\d+)\t([^\t]+)\t([^\t]+)\tvod/([^/]+)/([^/]+)/([^\t]+)\t(\d{3})\t(\d+)\t(.+)$'
)


class Validation:
    def __init__(self, root: Path):
        self.root = root
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.counts: Counter = Counter()
        self.checks: list[dict] = []

    def fail(self, message: str) -> None:
        self.errors.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append({"name": name, "passed": bool(ok), "detail": detail})
        if not ok:
            self.fail(f"{name}: {detail or 'validation failed'}")

    def read_jsonl(self, relative: str, required: bool = True) -> list[dict]:
        path = self.root / relative
        if not path.is_file():
            if required:
                self.fail(f"Missing required file: {relative}")
            return []
        rows = []
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.fail(f"{relative}:{line_number}: invalid JSON ({exc.msg})")
                    continue
                if not isinstance(row, dict):
                    self.fail(f"{relative}:{line_number}: JSONL row must be an object")
                    continue
                rows.append(row)
        self.counts[relative] = len(rows)
        return rows

    def read_csv(self, relative: str, required_columns: set[str]) -> list[dict]:
        path = self.root / relative
        if not path.is_file():
            self.fail(f"Missing required file: {relative}")
            return []
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fields = set(reader.fieldnames or [])
            self.check(
                f"{relative} columns",
                required_columns.issubset(fields),
                f"missing columns: {sorted(required_columns - fields)}",
            )
            rows = list(reader)
        self.counts[relative] = len(rows)
        return rows

    def validate_manifest(self) -> dict:
        path = self.root / "manifest.json"
        if not path.is_file():
            self.fail("Missing manifest.json")
            return {}
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            self.fail(f"Invalid manifest.json: {exc}")
            return {}
        self.check("dataset_type", manifest.get("dataset_type") == "hybrid", str(manifest.get("dataset_type")))
        self.check("schema_version", bool(manifest.get("schema_version")), "schema_version is missing")
        self.check("generator_version", bool(manifest.get("generator_version")), "generator_version is missing")
        self.check(
            "provenance classes",
            set(manifest.get("provenance", {})) == {
                "public_external", "synthetic_derived", "synthetic_generated"
            },
            str(sorted(manifest.get("provenance", {}))),
        )
        input_snapshots = manifest.get("input_snapshots", {})
        catalogue_input = input_snapshots.get("wikidata_catalogue", {})
        self.check(
            "Wikidata input snapshot metadata",
            bool(catalogue_input.get("sha256"))
            and catalogue_input.get("license") == "CC0-1.0"
            and bool(catalogue_input.get("source_endpoint")),
            "input snapshot checksum, CC0 licence or source endpoint is missing",
        )
        hashes = manifest.get("files_sha256", {})
        self.check("manifest file hashes", isinstance(hashes, dict) and bool(hashes), "no source-file checksums")
        actual_files = {
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*")
            if path.is_file() and path.name != "manifest.json"
        }
        expected_files = set(hashes) if isinstance(hashes, dict) else set()
        self.check(
            "manifest inventory",
            actual_files == expected_files,
            f"untracked={sorted(actual_files - expected_files)}; missing={sorted(expected_files - actual_files)}",
        )
        for relative, expected in hashes.items():
            candidate = self.root / relative
            if not candidate.is_file():
                self.fail(f"Manifest file is missing: {relative}")
                continue
            actual = hashlib.sha256(candidate.read_bytes()).hexdigest()
            if actual != expected:
                self.fail(f"Checksum mismatch: {relative}")
        return manifest

    def validate(self) -> dict:
        manifest = self.validate_manifest()

        # Catalogue snapshots: each must be valid nested JSON and have unique IDs.
        catalog_paths = sorted((self.root / "content").glob("catalog_*.json"))
        self.check("catalogue snapshots", bool(catalog_paths), "no content/catalog_*.json files")
        catalog_by_id: dict[str, dict] = {}
        for path in catalog_paths:
            try:
                rows = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                self.fail(f"{path.relative_to(self.root)}: invalid JSON ({exc})")
                continue
            if not isinstance(rows, list) or not rows:
                self.fail(f"{path.relative_to(self.root)}: expected a non-empty JSON array")
                continue
            ids = []
            for row in rows:
                if not isinstance(row, dict):
                    self.fail(f"{path.relative_to(self.root)}: catalogue item must be an object")
                    continue
                content_id = row.get("content_id")
                if not content_id:
                    self.fail(f"{path.relative_to(self.root)}: missing content_id")
                    continue
                ids.append(content_id)
                catalog_by_id[str(content_id)] = row
                if row.get("provenance") != "public_external+synthetic_derived":
                    self.fail(f"{path.relative_to(self.root)}: unexpected catalogue provenance for {content_id}")
                if not row.get("wikidata_qid"):
                    self.fail(f"{path.relative_to(self.root)}: missing Wikidata QID for {content_id}")
                if not row.get("title") or not row.get("release_date"):
                    self.fail(f"{path.relative_to(self.root)}: missing title or release date for {content_id}")
            if len(ids) != len(set(ids)):
                self.fail(f"{path.relative_to(self.root)}: duplicate content_id values")
            self.counts[str(path.relative_to(self.root))] = len(ids)
        self.check("catalogue IDs", bool(catalog_by_id), "catalogue ID index is empty")

        users = self.read_jsonl("app/users.json")
        profiles = self.read_jsonl("app/profiles.json")
        ratings = self.read_jsonl("app/ratings.json")
        subscriptions = self.read_jsonl("billing/subscription_events.json")
        searches = self.read_jsonl("search/search_logs.json")

        user_ids = [str(row.get("user_id", "")) for row in users]
        profile_ids = [str(row.get("profile_id", "")) for row in profiles]
        self.check("user primary keys", all(user_ids) and len(user_ids) == len(set(user_ids)), "missing or duplicate user_id")
        self.check("profile primary keys", all(profile_ids) and len(profile_ids) == len(set(profile_ids)), "missing or duplicate profile_id")
        user_set, profile_set, content_set = set(user_ids), set(profile_ids), set(catalog_by_id)
        profile_to_user = {
            str(row.get("profile_id")): str(row.get("user_id"))
            for row in profiles
        }

        for row in profiles:
            if row.get("user_id") not in user_set:
                self.fail(f"Profile references unknown user_id: {row.get('profile_id')} -> {row.get('user_id')}")
        valid_subscription_types = {
            "trial_start", "subscribe", "renew", "upgrade", "downgrade",
            "pause", "resume", "cancel", "payment_failed",
        }
        subscription_event_ids = [row.get("event_id") for row in subscriptions]
        self.check(
            "subscription event primary keys",
            all(subscription_event_ids)
            and len(subscription_event_ids) == len(set(subscription_event_ids)),
            "missing or duplicate subscription event_id",
        )
        for row in subscriptions:
            if row.get("user_id") not in user_set:
                self.fail(f"Subscription references unknown user_id: {row.get('user_id')}")
            if row.get("event_type") not in valid_subscription_types:
                self.fail(f"Invalid subscription event_type: {row.get('event_type')}")
            if not row.get("event_ts"):
                self.fail(f"Subscription event missing event_ts: {row.get('user_id')}")
        search_ids = [row.get("search_id") for row in searches]
        self.check(
            "search primary keys",
            all(search_ids) and len(search_ids) == len(set(search_ids)),
            "missing or duplicate search_id",
        )
        for row in searches:
            if row.get("profile_id") not in profile_set:
                self.fail(f"Search references unknown profile_id: {row.get('profile_id')}")
            try:
                result_count = int(row.get("results_count", -1))
            except (TypeError, ValueError):
                result_count = -1
            if result_count < 0:
                self.fail(f"Search has invalid results_count: {row.get('search_id')}")
            clicked = row.get("clicked_content_id")
            if clicked and clicked not in content_set:
                self.fail(f"Search click references unknown content_id: {clicked}")
            if clicked and result_count == 0:
                self.fail(f"Search click exists despite zero results: {row.get('search_id')}")
        rating_keys = [
            (row.get("profile_id"), row.get("content_id"))
            for row in ratings
        ]
        self.check(
            "rating natural keys",
            len(rating_keys) == len(set(rating_keys)),
            "duplicate profile_id/content_id rating pairs",
        )
        for row in ratings:
            if row.get("profile_id") not in profile_set:
                self.fail(f"Rating references unknown profile_id: {row.get('profile_id')}")
            if row.get("content_id") not in content_set:
                self.fail(f"Rating references unknown content_id: {row.get('content_id')}")
            if row.get("rating") not in range(1, 6):
                self.fail(f"Rating outside 1..5: {row.get('rating')}")

        settlement_rows = self.read_csv(
            "billing/gateway_settlements.csv",
            {"gateway", "user_id", "charge_id", "settlement_id", "amount_inr", "settled_ts", "event_type", "status"},
        )
        settlement_ids = [row.get("settlement_id") for row in settlement_rows]
        self.check(
            "settlement primary keys",
            all(settlement_ids) and len(settlement_ids) == len(set(settlement_ids)),
            "missing or duplicate settlement_id",
        )
        for row in settlement_rows:
            if row.get("user_id") not in user_set:
                self.fail(f"Settlement references unknown user_id: {row.get('user_id')}")
            try:
                amount = float(row.get("amount_inr", ""))
            except (TypeError, ValueError):
                amount = -1
            if amount < 0:
                self.fail(f"Settlement has invalid amount_inr: {row.get('settlement_id')}")

        playback_files = sorted((self.root / "player").glob("playback_*.json"))
        self.check("playback arrival files", bool(playback_files), "no player/playback_*.json files")
        playback_count = 0
        event_ids = set()
        arrival_dates = []
        for path in playback_files:
            expected_name = re.fullmatch(r"playback_(\d{4}-\d{2}-\d{2})\.json", path.name)
            if not expected_name:
                self.fail(f"Unexpected playback file name: {path.name}")
                continue
            arrival_date = date.fromisoformat(expected_name.group(1))
            arrival_dates.append(arrival_date)
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    playback_count += 1
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        self.fail(f"{path.name}:{line_number}: invalid JSON")
                        continue
                    if not isinstance(row, dict):
                        self.fail(f"{path.name}:{line_number}: playback record must be an object")
                        continue
                    event_id = row.get("event_id")
                    if not event_id or event_id in event_ids:
                        self.fail(f"{path.name}:{line_number}: missing or duplicate event_id {event_id}")
                    event_ids.add(event_id)
                    if row.get("user_id") not in user_set:
                        self.fail(f"{path.name}:{line_number}: unknown user_id {row.get('user_id')}")
                    if row.get("profile_id") not in profile_set:
                        self.fail(f"{path.name}:{line_number}: unknown profile_id {row.get('profile_id')}")
                    elif profile_to_user.get(str(row.get("profile_id"))) != str(row.get("user_id")):
                        self.fail(f"{path.name}:{line_number}: profile does not belong to user")
                    if row.get("content_id") not in content_set:
                        self.fail(f"{path.name}:{line_number}: unknown content_id {row.get('content_id')}")
                    if row.get("event_type") not in EVENT_TYPES:
                        self.fail(f"{path.name}:{line_number}: invalid event_type {row.get('event_type')}")
                    if row.get("provenance") != "synthetic_generated":
                        self.fail(f"{path.name}:{line_number}: playback provenance is not synthetic_generated")
                    try:
                        server_date = datetime.fromisoformat(row["server_ts"]).date()
                        datetime.fromisoformat(row["client_ts"])
                    except (KeyError, TypeError, ValueError):
                        self.fail(f"{path.name}:{line_number}: invalid client_ts/server_ts")
                        continue
                    if server_date != arrival_date:
                        self.fail(f"{path.name}:{line_number}: server_ts date {server_date} != file date {arrival_date}")
                    if not isinstance(row.get("position_sec"), int) or row["position_sec"] < 0:
                        self.fail(f"{path.name}:{line_number}: position_sec must be a non-negative integer")
        self.counts["player/playback_events"] = playback_count

        ad_campaigns = self.read_csv(
            "ads/ad_campaigns.csv",
            {"campaign_id", "advertiser", "budget_inr", "start_date", "end_date", "daily_frequency_cap", "target_genres", "cpm_inr"},
        )
        ad_breaks = self.read_csv(
            "ads/ad_breaks.csv",
            {"break_id", "user_id", "profile_id", "content_id", "break_ts", "filled", "impression_id"},
        )
        ad_impressions = self.read_csv(
            "ads/ad_impressions.csv",
            {"impression_id", "user_id", "profile_id", "content_id", "campaign_id", "ad_break_ts", "duration_sec", "completed", "cpm_inr"},
        )
        campaign_ids = {row.get("campaign_id") for row in ad_campaigns}
        break_ids = [row.get("break_id") for row in ad_breaks]
        impression_ids = [row.get("impression_id") for row in ad_impressions]
        self.check("campaign primary keys", all(campaign_ids) and len(campaign_ids) == len(ad_campaigns), "missing or duplicate campaign_id")
        self.check("ad break primary keys", all(break_ids) and len(break_ids) == len(set(break_ids)), "missing or duplicate break_id")
        self.check("ad impression primary keys", all(impression_ids) and len(impression_ids) == len(set(impression_ids)), "missing or duplicate impression_id")
        impression_map = {row["impression_id"]: row for row in ad_impressions if row.get("impression_id")}
        break_impression_ids = set()
        for row in ad_breaks:
            if row.get("user_id") not in user_set or row.get("profile_id") not in profile_set or row.get("content_id") not in content_set:
                self.fail(f"Ad break has an unknown user/profile/content reference: {row.get('break_id')}")
            elif profile_to_user.get(str(row.get("profile_id"))) != str(row.get("user_id")):
                self.fail(f"Ad break profile does not belong to user: {row.get('break_id')}")
            filled = str(row.get("filled")) == "1"
            impression_id = row.get("impression_id", "")
            if filled != bool(impression_id):
                self.fail(f"Ad break fill/impression mismatch: {row.get('break_id')}")
            if impression_id:
                break_impression_ids.add(impression_id)
                if impression_id not in impression_map:
                    self.fail(f"Ad break references missing impression: {impression_id}")
        for row in ad_impressions:
            if row.get("user_id") not in user_set or row.get("profile_id") not in profile_set or row.get("content_id") not in content_set:
                self.fail(f"Ad impression has an unknown user/profile/content reference: {row.get('impression_id')}")
            elif profile_to_user.get(str(row.get("profile_id"))) != str(row.get("user_id")):
                self.fail(f"Ad impression profile does not belong to user: {row.get('impression_id')}")
            if row.get("campaign_id") not in campaign_ids:
                self.fail(f"Ad impression references unknown campaign: {row.get('campaign_id')}")
        if set(impression_map) != break_impression_ids:
            self.fail("Ad impression set does not match filled break impression IDs")

        licences = self.read_csv(
            "content/licences.csv", {"content_id", "region", "licence_start", "licence_end"}
        )
        for row in licences:
            if row.get("content_id") not in content_set:
                self.fail(f"Licence references unknown content_id: {row.get('content_id')}")

        for relative, columns in [
            ("reference/content_partners.csv", {"partner_id", "name", "revenue_share_pct", "minimum_guarantee_inr_per_quarter"}),
            ("reference/cdn_rates.csv", {"cdn", "region", "rate_inr_per_gb"}),
            ("reference/cdn_edges.csv", {"edge", "pop_code", "region"}),
            ("reference/genre_mapping.csv", {"raw_genre", "standard_genre"}),
            ("reference/city_state.csv", {"city", "state"}),
            ("reference/plans.csv", {"plan", "price_inr_per_month", "max_screens"}),
            ("reference/published_metrics.csv", {"month", "metric", "value", "published_ts"}),
        ]:
            self.read_csv(relative, columns)

        # Validate both provider-specific log encodings; row counts are recorded by provider.
        cdn_files = sorted((self.root / "cdn").glob("*"))
        self.check("CDN source files", bool(cdn_files), "no CDN log files")
        cdn_count = 0
        for path in cdn_files:
            if not path.is_file():
                continue
            if path.name.startswith("akashcdn_") and path.suffix == ".log":
                pattern = AKASH_RE
                provider = "akash"
            elif path.name.startswith("nimbusedge_") and path.suffix == ".tsv":
                pattern = NIMBUS_RE
                provider = "nimbus"
            else:
                self.fail(f"Unexpected CDN source file: {path.name}")
                continue
            line_count = 0
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    line_count += 1
                    if not pattern.match(line.rstrip("\n")):
                        self.fail(f"{path.name}:{line_number}: invalid {provider} CDN record")
            cdn_count += line_count
            self.counts[str(path.relative_to(self.root))] = line_count
        self.counts["cdn/requests"] = cdn_count

        actual_rows = dict(self.counts)
        expected_rows = manifest.get("row_counts", {})
        # Only compare row_count keys explicitly defined by the generator.
        checks = {
            "users": actual_rows.get("app/users.json", 0),
            "profiles": actual_rows.get("app/profiles.json", 0),
            "subscriptions": actual_rows.get("billing/subscription_events.json", 0),
            "settlements": actual_rows.get("billing/gateway_settlements.csv", 0),
            "search": actual_rows.get("search/search_logs.json", 0),
            "ratings": actual_rows.get("app/ratings.json", 0),
            "ad_breaks": actual_rows.get("ads/ad_breaks.csv", 0),
            "ad_impressions": actual_rows.get("ads/ad_impressions.csv", 0),
            "licences": actual_rows.get("content/licences.csv", 0),
            "playback_events": actual_rows.get("player/playback_events", 0),
            "cdn_requests": actual_rows.get("cdn/requests", 0),
            "campaigns": actual_rows.get("ads/ad_campaigns.csv", 0),
        }
        for name, actual in checks.items():
            if name in expected_rows and actual != expected_rows[name]:
                self.fail(f"Manifest row count mismatch for {name}: manifest={expected_rows[name]}, actual={actual}")

        self.check("source files have provenance manifest", bool(manifest.get("provenance")), "missing provenance metadata")
        if playback_files and arrival_dates:
            self.check(
                "arrival period",
                manifest.get("playback_arrival_period", {}).get("start_date") == min(arrival_dates).isoformat()
                and manifest.get("playback_arrival_period", {}).get("end_date") == max(arrival_dates).isoformat(),
                f"manifest={manifest.get('playback_arrival_period')}, files={min(arrival_dates)}..{max(arrival_dates)}",
            )
        if expected_rows.get("published_metrics", 0) == 0:
            self.warn("published_metrics.csv is intentionally header-only; no authentic baseline exists for reconciliation.")

        return {
            "dataset_type": manifest.get("dataset_type", "unknown"),
            "root": str(self.root),
            "status": "failed" if self.errors else "passed",
            "row_counts_observed": dict(sorted(self.counts.items())),
            "checks": self.checks,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def validate_dataset(root: str | Path) -> dict:
    return Validation(Path(root)).validate()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/hybrid/raw")
    parser.add_argument("--report", default="logs/hybrid_preflight/latest.json")
    args = parser.parse_args()

    root = Path(args.data_root)
    report = validate_dataset(root)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(f"HYBRID DATA PREFLIGHT: {report['status'].upper()}")
    print(f"  root: {root}")
    for name, count in report["row_counts_observed"].items():
        print(f"  {name}: {count}")
    for warning in report["warnings"]:
        print(f"  WARNING: {warning}")
    for error in report["errors"]:
        print(f"  ERROR: {error}", file=sys.stderr)
    print(f"  report: {report_path}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
