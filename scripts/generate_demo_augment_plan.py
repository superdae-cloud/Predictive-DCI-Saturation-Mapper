"""
(Re)generates data/augment_pipeline.csv for the demo fleet, calibrated
against whatever is *currently* in storage -- not hardcoded dates.

Why this needs to exist at all: a static CSV of "planned_upgrade_date"
values would go stale the moment more time passes or the demo is run for
a different duration, since SaturationForecast.predicted_saturation_date
is always computed relative to "now" and the trend accumulated so far.
Deriving each planned date from the circuit's *actual current forecast*
(offset earlier or later) keeps the demo's four Stage 3 outcomes correct
regardless of when or how long you've run the generator:

  DCI-ASH-DAL-W1 (linear ramp)    -> plan set LATE  -> demonstrates "at_risk"
  DCI-NYC-CHI-W1 (seasonal growth) -> plan set EARLY -> demonstrates "on_track"
  DCI-SJC-PDX-W2 (step change)     -> no plan at all -> demonstrates "missing_plan"
  DCI-LON-FRA-W1 (healthy plateau)  -> no plan needed -> demonstrates "not_on_track"

Usage (run as a module, like the other CLIs in this repo, so `src` resolves):
    python -m scripts.generate_demo_augment_plan --db-path data/telemetry.sqlite
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta

from src.ingestion.storage import TimeSeriesStore
from src.modeling.forecaster import build_forecast

LATE_OFFSET_DAYS = 14     # how far past predicted saturation to set the "at risk" plan
EARLY_FRACTION = 0.4       # how much of the remaining runway to shave off for the "on track" plan


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-path", default="data/telemetry.sqlite")
    parser.add_argument("--output-path", default="data/augment_pipeline.csv")
    args = parser.parse_args()

    store = TimeSeriesStore(args.db_path)

    def forecast_for(circuit_id: str):
        history = store.history_for_circuit(circuit_id)
        return build_forecast(circuit_id, history)

    ash_dal = forecast_for("DCI-ASH-DAL-W1")
    nyc_chi = forecast_for("DCI-NYC-CHI-W1")
    store.close()

    rows = []

    if ash_dal.predicted_saturation_date:
        predicted = datetime.fromisoformat(ash_dal.predicted_saturation_date)
        late_plan = predicted + timedelta(days=LATE_OFFSET_DAYS)
        rows.append(("DCI-ASH-DAL-W1", late_plan.isoformat(), 200_000_000_000))
    else:
        print("Warning: DCI-ASH-DAL-W1 has no forecast yet -- run the generator/consumer first.")

    if nyc_chi.predicted_saturation_date:
        predicted = datetime.fromisoformat(nyc_chi.predicted_saturation_date)
        remaining_days = max(nyc_chi.days_to_saturation, 1.0)
        early_plan = predicted - timedelta(days=remaining_days * EARLY_FRACTION)
        rows.append(("DCI-NYC-CHI-W1", early_plan.isoformat(), 40_000_000_000))
    else:
        print("Warning: DCI-NYC-CHI-W1 has no forecast yet -- run the generator/consumer first.")

    # DCI-SJC-PDX-W2 and DCI-LON-FRA-W1 deliberately get no row -- see module docstring.

    with open(args.output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["circuit_id", "planned_upgrade_date", "new_capacity_bps"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} row(s) to {args.output_path}")


if __name__ == "__main__":
    main()
