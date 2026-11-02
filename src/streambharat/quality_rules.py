"""Pure Python quality/business rules suitable for deterministic unit testing."""
from __future__ import annotations
from decimal import Decimal, ROUND_DOWN


def cap_watch_minutes(watch_minutes: float, duration_minutes: float, multiplier: float = 1.5) -> float:
    if watch_minutes < 0 or duration_minutes < 0 or multiplier <= 0:
        raise ValueError("Watch time, duration and cap multiplier must be non-negative")
    return min(float(watch_minutes),float(duration_minutes)*multiplier)


def calculate_completion(watch_minutes: float, duration_minutes: float) -> float | None:
    if duration_minutes <= 0: return None
    return min(max(float(watch_minutes),0.0)/float(duration_minutes),1.0)


def is_payment_recovered(failure_ts, renewal_ts, grace_days: int = 7) -> bool:
    if failure_ts is None or renewal_ts is None: return False
    return failure_ts < renewal_ts <= failure_ts + __import__("datetime").timedelta(days=grace_days)


def exceeds_daily_watch_limit(minutes: float, max_minutes: float = 1440.0) -> bool:
    return float(minutes) > float(max_minutes)


def metric_drift_exceeds(published: float, recalculated: float, tolerance: float = 0.005) -> bool:
    if tolerance < 0: raise ValueError("Tolerance cannot be negative")
    if published == 0: return recalculated != 0
    return abs(recalculated-published)/abs(published) > tolerance


def allocate_exact(total_inr: Decimal, weights: dict[str, Decimal]) -> dict[str, Decimal]:
    """Largest-remainder currency allocation; output always sums to rounded total."""
    cents=int((total_inr*100).quantize(Decimal("1")))
    positive={k:max(Decimal(0),v) for k,v in weights.items()}; denom=sum(positive.values())
    if denom==0: return {k:Decimal("0.00") for k in weights}
    raw={k:Decimal(cents)*v/denom for k,v in positive.items()}
    base={k:int(x.to_integral_value(rounding=ROUND_DOWN)) for k,x in raw.items()}
    remain=cents-sum(base.values())
    for k in sorted(raw,key=lambda z:(raw[z]-base[z],z),reverse=True)[:remain]: base[k]+=1
    return {k:(Decimal(v)/100).quantize(Decimal("0.01")) for k,v in base.items()}


def royalty_with_guarantee(earned: Decimal, guarantee: Decimal) -> tuple[Decimal, Decimal]:
    topup=max(Decimal("0.00"),guarantee-earned)
    return earned+topup,topup
