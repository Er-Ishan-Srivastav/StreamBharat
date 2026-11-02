"""Small deterministic business rules shared by the Spark jobs and unit tests."""

from __future__ import annotations

from typing import Any, Iterable


def has_download_id(value: Any) -> bool:
    """True when a playback event belongs to a downloaded/offline session."""
    return value is not None and str(value).strip() != ""


def choose_latest_event(rows: Iterable[dict[str, Any]], key: str = "event_id") -> list[dict[str, Any]]:
    """Deduplicate a batch by key, retaining the row with the latest server_ts.

    A stable lexical tie-break makes this deterministic when timestamps match.
    Spark ingestion uses an equivalent Window expression at scale.
    """
    latest: dict[Any, dict[str, Any]] = {}
    for row in rows:
        event_key = row.get(key)
        if event_key is None:
            continue
        previous = latest.get(event_key)
        if previous is None or (
            str(row.get("server_ts") or ""), str(row.get("event_type") or "")
        ) > (
            str(previous.get("server_ts") or ""), str(previous.get("event_type") or "")
        ):
            latest[event_key] = row
    return list(latest.values())


def trending_growth(current_viewers: int | float, baseline_viewers: int | float) -> float:
    """Return current/baseline lift; a zero baseline is protected with 1."""
    return float(current_viewers) / max(float(baseline_viewers), 1.0)


def is_trending(current_viewers: int, baseline_viewers: float, minimum_viewers: int = 200,
                minimum_growth: float = 1.0) -> bool:
    """Apply an absolute audience threshold and a relative baseline threshold."""
    return current_viewers >= minimum_viewers and trending_growth(current_viewers, baseline_viewers) >= minimum_growth


def watch_decline(current_minutes: float, previous_minutes: float) -> float:
    """Relative decline from the previous 28-day period; negative means growth."""
    if previous_minutes <= 0:
        return 0.0
    return (previous_minutes - current_minutes) / previous_minutes


def churn_terminal_label(terminal_type: str | None) -> int | None:
    """Map the first observed terminal subscription event to the binary label.

    A voluntary cancel and an unrecovered payment failure are churn (1).
    A subsequent successful renewal is retained (0). Missing outcomes are censored.
    """
    if terminal_type in {"voluntary_cancel", "involuntary_failure"}:
        return 1
    if terminal_type == "renewed":
        return 0
    return None
