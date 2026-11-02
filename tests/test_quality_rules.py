from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from streambharat.quality_rules import (
    allocate_exact,
    calculate_completion,
    cap_watch_minutes,
    exceeds_daily_watch_limit,
    is_payment_recovered,
    metric_drift_exceeds,
    royalty_with_guarantee,
)


def test_watch_time_cap_and_completion_bounds():
    assert cap_watch_minutes(80, 40, 1.5) == 60.0
    assert cap_watch_minutes(30, 40, 1.5) == 30.0
    assert calculate_completion(60, 40) == 1.0
    assert calculate_completion(20, 40) == 0.5
    assert calculate_completion(10, 0) is None


def test_watch_time_rejects_negative_inputs():
    with pytest.raises(ValueError):
        cap_watch_minutes(-1, 10)


def test_payment_failure_recovery_includes_seventh_day_boundary():
    failure = datetime(2026, 10, 1, 12, 0)
    assert is_payment_recovered(failure, failure + timedelta(days=7))
    assert not is_payment_recovered(failure, failure + timedelta(days=7, seconds=1))
    assert not is_payment_recovered(failure, failure)


def test_daily_watch_limit_is_strictly_greater_than_24_hours():
    assert not exceeds_daily_watch_limit(1440)
    assert exceeds_daily_watch_limit(1440.01)


def test_published_metric_freeze_threshold_is_point_five_percent():
    assert not metric_drift_exceeds(1000, 1005, 0.005)
    assert metric_drift_exceeds(1000, 1005.01, 0.005)
    assert metric_drift_exceeds(0, 0.01, 0.005)


def test_largest_remainder_allocation_reconciles_to_exact_paise():
    allocation = allocate_exact(Decimal("10.00"), {
        "partner-a": Decimal("1"), "partner-b": Decimal("1"), "partner-c": Decimal("1")
    })
    assert sum(allocation.values()) == Decimal("10.00")
    assert sorted(allocation.values()) == [Decimal("3.33"), Decimal("3.33"), Decimal("3.34")]


def test_zero_royalty_weights_return_zero_for_each_partner():
    allocation = allocate_exact(Decimal("25.00"), {"a": Decimal("0"), "b": Decimal("0")})
    assert allocation == {"a": Decimal("0.00"), "b": Decimal("0.00")}


def test_quarterly_guarantee_top_up_only_fills_shortfall():
    payable, top_up = royalty_with_guarantee(Decimal("120.00"), Decimal("150.00"))
    assert payable == Decimal("150.00")
    assert top_up == Decimal("30.00")
    assert royalty_with_guarantee(Decimal("180.00"), Decimal("150.00")) == (
        Decimal("180.00"), Decimal("0.00")
    )
