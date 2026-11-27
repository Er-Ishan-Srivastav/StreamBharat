#!/usr/bin/env python3
"""Validate the date and refresh settings supplied to the Airflow DAG."""
from __future__ import annotations

import os
from datetime import date
import re


def parse_iso_date(name: str, value: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", value):
        raise ValueError(f"{name} must use canonical YYYY-MM-DD format; got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid calendar date; got {value!r}") from exc


def validate_run_config(
    logical_date: str,
    full_refresh: str,
    replay_start: str,
    replay_end: str,
) -> dict[str, object]:
    logical = parse_iso_date("logical_date", logical_date)
    start = parse_iso_date("replay_start", replay_start)
    end = parse_iso_date("replay_end", replay_end)

    normalized_refresh = str(full_refresh).strip().lower()
    if normalized_refresh not in {"true", "false"}:
        raise ValueError("full_refresh must be either 'true' or 'false'")
    is_full_refresh = normalized_refresh == "true"

    if end < start:
        raise ValueError(
            f"replay_end ({end.isoformat()}) must be on or after replay_start ({start.isoformat()})"
        )

    if not is_full_refresh and (start != logical or end != logical):
        raise ValueError(
            "A daily run must replay exactly its logical date. "
            "Set full_refresh=true for a multi-day replay range."
        )

    if is_full_refresh and (end - start).days > 366:
        raise ValueError("A full-refresh replay range cannot exceed 367 calendar days")

    return {
        "logical_date": logical.isoformat(),
        "full_refresh": is_full_refresh,
        "replay_start": start.isoformat(),
        "replay_end": end.isoformat(),
    }


def main() -> None:
    config = validate_run_config(
        os.environ.get("STREAMBHARAT_LOGICAL_DATE", ""),
        os.environ.get("STREAMBHARAT_FULL_REFRESH", "false"),
        os.environ.get("STREAMBHARAT_REPLAY_START", ""),
        os.environ.get("STREAMBHARAT_REPLAY_END", ""),
    )
    print(
        "Run configuration validated: "
        f"logical_date={config['logical_date']}, "
        f"full_refresh={str(config['full_refresh']).lower()}, "
        f"replay={config['replay_start']}..{config['replay_end']}"
    )


if __name__ == "__main__":
    main()
