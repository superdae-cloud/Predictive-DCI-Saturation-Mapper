"""
Trend-fitting math for Stage 2 (forecasting). Pure functions, no I/O -- same
convention as circuit_profiles.py, so this is independently testable against
known inputs without touching storage or Kafka.

Why weighted least squares, not plain OLS:

A plain full-history OLS fit is the "naive model" circuit_profiles.py's four
archetypes are deliberately built to break (see docs/architecture.md and the
README's archetype table):

  - step_change: a plain fit blends the flat pre-jump baseline with the
    post-jump plateau into one gentle upward slope, understating how close
    the circuit already is to its new (possibly saturated) level.
  - seasonal_growth: a plain fit is unbiased in theory but its confidence
    interval balloons with daily/weekly noise, making single-fit output
    jumpy run to run.

Exponentially recency-weighted least squares fixes the first problem
directly (old data barely counts) and helps the second as long as the
half-life spans at least one full seasonal cycle. The half-life is chosen
as a fraction of the observed span rather than a fixed duration because
this pipeline's timestamps are NOT reliably "wall-clock hours" -- the
synthetic generator compresses simulated weeks into minutes of real time,
so a fixed-hours window would mean something different in a live demo than
in a production deployment. Scaling to the observed span keeps the method's
behavior consistent either way.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass(frozen=True)
class TrendFit:
    slope_pct_per_hour: float
    intercept_pct: float
    r_squared: float

    def value_at(self, hour: float) -> float:
        return self.intercept_pct + self.slope_pct_per_hour * hour


def fit_weighted_linear_trend(
    hours: Sequence[float],
    pct: Sequence[float],
    weights: Sequence[float],
) -> TrendFit:
    """Fit pct ~ intercept + slope * hours via weighted least squares.

    Returns slope/intercept plus the weighted R^2 (fraction of weighted
    variance explained), used downstream as a confidence score.
    """
    if len(hours) != len(pct) or len(hours) != len(weights):
        raise ValueError("hours, pct, and weights must be the same length")
    if len(hours) < 2:
        raise ValueError("need at least 2 points to fit a trend")

    h = np.asarray(hours, dtype=float)
    y = np.asarray(pct, dtype=float)
    w = np.asarray(weights, dtype=float)

    slope, intercept = np.polyfit(h, y, deg=1, w=w)

    fitted = intercept + slope * h
    weighted_mean = np.average(y, weights=w)
    ss_res = np.sum(w * (y - fitted) ** 2)
    ss_tot = np.sum(w * (y - weighted_mean) ** 2)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 1.0

    return TrendFit(
        slope_pct_per_hour=float(slope),
        intercept_pct=float(intercept),
        r_squared=max(0.0, min(1.0, float(r_squared))),
    )


def recency_weights(hours: Sequence[float], half_life_hours: float) -> list[float]:
    """Exponential decay weights: 0.5 at half_life_hours in the past, etc."""
    h = np.asarray(hours, dtype=float)
    latest = h.max()
    return list(np.power(0.5, (latest - h) / half_life_hours))


def hours_to_cross_threshold(fit: TrendFit, current_hour: float, threshold_pct: float) -> float | None:
    """Hours from current_hour until the fitted line reaches threshold_pct.

    Returns None if the trend is flat/negative (never crosses going
    forward), or 0.0 if the fitted line is already at/above threshold.
    """
    current_fitted = fit.value_at(current_hour)
    if current_fitted >= threshold_pct:
        return 0.0

    # Require a slope large enough to matter within a plausible planning
    # horizon; near-zero slopes shouldn't project a saturation date decades
    # out just because they're technically positive.
    if fit.slope_pct_per_hour <= MIN_MEANINGFUL_SLOPE_PCT_PER_HOUR:
        return None

    hour_at_threshold = (threshold_pct - fit.intercept_pct) / fit.slope_pct_per_hour
    return max(0.0, hour_at_threshold - current_hour)


# Slopes below this are treated as "flat" -- below ~1 percentage point of
# growth per simulated month, not worth projecting forward.
MIN_MEANINGFUL_SLOPE_PCT_PER_HOUR = 1.0 / (30 * 24)
