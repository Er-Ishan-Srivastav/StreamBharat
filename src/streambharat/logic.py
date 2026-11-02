"""Deterministic business rules shared by Spark jobs and unit tests."""
from __future__ import annotations
from typing import Any, Iterable

def has_download_id(value: Any) -> bool:
    return value is not None and str(value).strip() != ""

def choose_latest_event(rows: Iterable[dict[str, Any]], key: str = "event_id") -> list[dict[str, Any]]:
    latest: dict[Any, dict[str, Any]] = {}
    for row in rows:
        event_key = row.get(key)
        if event_key is None:
            continue
        previous = latest.get(event_key)
        now_key = (str(row.get("server_ts") or ""), str(row.get("event_type") or ""))
        old_key = ((str(previous.get("server_ts") or ""), str(previous.get("event_type") or ""))
                   if previous else None)
        if previous is None or now_key > old_key:
            latest[event_key] = row
    return list(latest.values())

def trending_growth(current_viewers: int | float, baseline_viewers: int | float) -> float:
    return float(current_viewers) / max(float(baseline_viewers), 1.0)

def is_trending(current_viewers: int, baseline_viewers: float, minimum_viewers: int = 200,
                minimum_growth: float = 1.0) -> bool:
    return current_viewers >= minimum_viewers and trending_growth(current_viewers, baseline_viewers) >= minimum_growth

def watch_decline(current_minutes: float, previous_minutes: float) -> float:
    return 0.0 if previous_minutes <= 0 else (previous_minutes - current_minutes) / previous_minutes

def churn_terminal_label(terminal_type: str | None, renewed_within_grace: bool = False) -> int | None:
    """Successful renewal inside the grace window takes precedence over intermediate failures/cancels."""
    if renewed_within_grace or terminal_type == "renewed":
        return 0
    if terminal_type in {"voluntary_cancel", "involuntary_failure", "no_renewal_within_grace"}:
        return 1
    return None
