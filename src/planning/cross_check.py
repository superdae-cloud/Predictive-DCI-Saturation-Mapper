"""
Stage 3: cross-check each circuit's Stage 2 forecast against its planned
capacity augment. This is the scheduling-conflict check the whole project
exists to automate (see README's "Why this exists") -- most shops do this
by hand, in a spreadsheet, after someone already noticed a link running
hot.

Usage:
    python -m src.planning.cross_check --db-path data/telemetry.sqlite \
        --augment-plan-path data/augment_pipeline.csv
"""

from __future__ import annotations

import argparse
from datetime import datetime

from src.common.models import AugmentPlan, CrossCheckResult, SaturationForecast
from src.ingestion.storage import TimeSeriesStore
from src.modeling.forecaster import DEFAULT_THRESHOLD_PCT, build_forecast
from src.planning.augment_pipeline import load_augment_plans


def cross_check(forecast: SaturationForecast, plan: AugmentPlan | None) -> CrossCheckResult:
    """Compare one circuit's forecast against its (possibly absent) plan.

    See CrossCheckResult's docstring for what each status means. The
    ordering of checks matters: "not on track to saturate" is decided
    first and short-circuits everything else, because a missing or
    late-landing augment plan for a circuit that was never going to
    saturate isn't a problem worth flagging.
    """
    if forecast.predicted_saturation_date is None:
        return CrossCheckResult(
            circuit_id=forecast.circuit_id,
            status="not_on_track",
            predicted_saturation_date=None,
            planned_upgrade_date=plan.planned_upgrade_date if plan else None,
            days_of_slack=None,
        )

    if plan is None:
        return CrossCheckResult(
            circuit_id=forecast.circuit_id,
            status="missing_plan",
            predicted_saturation_date=forecast.predicted_saturation_date,
            planned_upgrade_date=None,
            days_of_slack=None,
        )

    predicted = datetime.fromisoformat(forecast.predicted_saturation_date)
    planned = datetime.fromisoformat(plan.planned_upgrade_date)
    # positive => planned upgrade lands AFTER predicted saturation => too late, at risk
    # negative => planned upgrade lands BEFORE predicted saturation => safe, with that much margin
    days_of_slack = (planned - predicted).total_seconds() / 86400.0
    status = "at_risk" if days_of_slack > 0 else "on_track"

    return CrossCheckResult(
        circuit_id=forecast.circuit_id,
        status=status,
        predicted_saturation_date=forecast.predicted_saturation_date,
        planned_upgrade_date=plan.planned_upgrade_date,
        days_of_slack=days_of_slack,
    )


def _format_result(r: CrossCheckResult) -> str:
    if r.status == "not_on_track":
        return f"{r.circuit_id:<20} OK       not on track to saturate"
    if r.status == "missing_plan":
        return (
            f"{r.circuit_id:<20} MISSING  predicted saturation "
            f"{r.predicted_saturation_date} but NO augment plan exists"
        )
    if r.status == "at_risk":
        return (
            f"{r.circuit_id:<20} AT RISK  predicted saturation {r.predicted_saturation_date} "
            f"is {abs(r.days_of_slack):.1f} days BEFORE planned upgrade {r.planned_upgrade_date}"
        )
    return (
        f"{r.circuit_id:<20} OK       planned upgrade {r.planned_upgrade_date} "
        f"lands {abs(r.days_of_slack):.1f} days before predicted saturation "
        f"{r.predicted_saturation_date}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-path", default="data/telemetry.sqlite")
    parser.add_argument("--augment-plan-path", default="data/augment_pipeline.csv")
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    args = parser.parse_args()

    store = TimeSeriesStore(args.db_path)
    circuit_ids = store.circuit_ids()
    if not circuit_ids:
        print(f"No telemetry found in {args.db_path} -- run the generator/consumer first.")
        store.close()
        return

    plans = load_augment_plans(args.augment_plan_path)

    results = []
    for circuit_id in sorted(circuit_ids):
        history = store.history_for_circuit(circuit_id)
        forecast = build_forecast(circuit_id, history, threshold_pct=args.threshold_pct)
        results.append(cross_check(forecast, plans.get(circuit_id)))
    store.close()

    for r in results:
        print(_format_result(r))

    flagged = [r for r in results if r.status in ("at_risk", "missing_plan")]
    print()
    if flagged:
        print(f"{len(flagged)} of {len(results)} circuit(s) need attention: "
              f"{', '.join(r.circuit_id for r in flagged)}")
    else:
        print(f"All {len(results)} circuit(s) are on track.")


if __name__ == "__main__":
    main()
