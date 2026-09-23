"""
Tests for Stage 3: the augment-pipeline cross-check.

Built against direct, hand-constructed SaturationForecast/AugmentPlan
inputs rather than a live forecaster run -- the cross-check's logic
(compare two dates, pick a status) is a separate concern from how a
forecast gets produced, and Stage 2's own timing behavior is already
covered by test_forecaster.py.
"""

import csv
import os
import tempfile

from src.common.models import AugmentPlan, SaturationForecast
from src.planning.augment_pipeline import InvalidAugmentPlanError, load_augment_plans
from src.planning.cross_check import cross_check

BASE_KWARGS = dict(
    circuit_id="DCI-TEST",
    generated_at="2026-01-01T00:00:00+00:00",
    threshold_pct=90.0,
    current_utilization_pct=70.0,
    trend_pct_per_day=1.0,
    confidence=0.9,
    sample_count=100,
)


def _forecast(predicted_saturation_date, days_to_saturation=None) -> SaturationForecast:
    return SaturationForecast(
        predicted_saturation_date=predicted_saturation_date,
        days_to_saturation=days_to_saturation,
        **BASE_KWARGS,
    )


def test_not_on_track_short_circuits_regardless_of_plan():
    forecast = _forecast(None)
    plan = AugmentPlan(circuit_id="DCI-TEST", planned_upgrade_date="2026-01-05T00:00:00+00:00", new_capacity_bps=1)

    result = cross_check(forecast, plan)

    assert result.status == "not_on_track"
    assert result.days_of_slack is None


def test_missing_plan_when_saturating_but_no_plan_exists():
    forecast = _forecast("2026-02-01T00:00:00+00:00", days_to_saturation=31.0)

    result = cross_check(forecast, plan=None)

    assert result.status == "missing_plan"
    assert result.planned_upgrade_date is None


def test_at_risk_when_saturation_predicted_before_planned_upgrade():
    forecast = _forecast("2026-02-01T00:00:00+00:00", days_to_saturation=31.0)
    plan = AugmentPlan(circuit_id="DCI-TEST", planned_upgrade_date="2026-03-01T00:00:00+00:00", new_capacity_bps=1)

    result = cross_check(forecast, plan)

    assert result.status == "at_risk"
    assert result.days_of_slack > 0


def test_on_track_when_upgrade_lands_before_predicted_saturation():
    forecast = _forecast("2026-03-01T00:00:00+00:00", days_to_saturation=59.0)
    plan = AugmentPlan(circuit_id="DCI-TEST", planned_upgrade_date="2026-02-01T00:00:00+00:00", new_capacity_bps=1)

    result = cross_check(forecast, plan)

    assert result.status == "on_track"
    assert result.days_of_slack < 0


def test_load_augment_plans_reads_csv_rows():
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "plans.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["circuit_id", "planned_upgrade_date", "new_capacity_bps"])
            writer.writerow(["DCI-A", "2026-06-01T00:00:00+00:00", "200000000000"])

        plans = load_augment_plans(csv_path)

        assert plans["DCI-A"].new_capacity_bps == 200_000_000_000
        assert "DCI-B" not in plans


def test_load_augment_plans_missing_file_returns_empty():
    assert load_augment_plans("/nonexistent/path.csv") == {}


def test_load_augment_plans_rejects_duplicate_circuit_id():
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "plans.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["circuit_id", "planned_upgrade_date", "new_capacity_bps"])
            writer.writerow(["DCI-A", "2026-06-01T00:00:00+00:00", "1"])
            writer.writerow(["DCI-A", "2026-07-01T00:00:00+00:00", "2"])

        try:
            load_augment_plans(csv_path)
            assert False, "expected InvalidAugmentPlanError"
        except InvalidAugmentPlanError:
            pass


def test_load_augment_plans_rejects_non_integer_capacity():
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "plans.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["circuit_id", "planned_upgrade_date", "new_capacity_bps"])
            writer.writerow(["DCI-A", "2026-06-01T00:00:00+00:00", "not-a-number"])

        try:
            load_augment_plans(csv_path)
            assert False, "expected InvalidAugmentPlanError"
        except InvalidAugmentPlanError:
            pass
