"""
Loads the capacity-augment / leased-circuit pipeline -- the "planned
upgrades" table Stage 3 cross-checks forecasts against.

A CSV, not a database: most shops already track this in a spreadsheet (see
README's "Why this exists"), and a network planner should be able to open
this file directly without any tooling. Keeping it a flat file with one
row per circuit is deliberate -- same "boring on purpose" philosophy as
TimeSeriesStore's choice of SQLite.
"""

from __future__ import annotations

import csv
from pathlib import Path

from src.common.models import AugmentPlan

REQUIRED_FIELDS = {"circuit_id", "planned_upgrade_date", "new_capacity_bps"}


class InvalidAugmentPlanError(ValueError):
    pass


def load_augment_plans(csv_path: str) -> dict[str, AugmentPlan]:
    """Returns {circuit_id: AugmentPlan}, one entry per CSV row.

    A circuit with no row here has no planned upgrade at all -- that's a
    valid, meaningful state (see CrossCheckResult's "missing_plan" status),
    not an error.
    """
    path = Path(csv_path)
    if not path.exists():
        return {}

    plans: dict[str, AugmentPlan] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        missing_header = REQUIRED_FIELDS - set(reader.fieldnames or [])
        if missing_header:
            raise InvalidAugmentPlanError(f"missing columns: {sorted(missing_header)}")

        for row in reader:
            circuit_id = row["circuit_id"].strip()
            if not circuit_id:
                continue
            if circuit_id in plans:
                raise InvalidAugmentPlanError(f"duplicate circuit_id in {csv_path}: {circuit_id}")

            try:
                new_capacity_bps = int(row["new_capacity_bps"])
            except ValueError as e:
                raise InvalidAugmentPlanError(
                    f"{circuit_id}: new_capacity_bps must be an integer, got {row['new_capacity_bps']!r}"
                ) from e
            if new_capacity_bps <= 0:
                raise InvalidAugmentPlanError(f"{circuit_id}: new_capacity_bps must be positive")

            plans[circuit_id] = AugmentPlan(
                circuit_id=circuit_id,
                planned_upgrade_date=row["planned_upgrade_date"].strip(),
                new_capacity_bps=new_capacity_bps,
            )

    return plans
