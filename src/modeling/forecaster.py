"""
Stage 2 forecasting driver + CLI.

Reads a circuit's stored history (TimeSeriesStore.history_for_circuit),
fits a recency-weighted trend (trend_fit.py), and emits a
SaturationForecast -- the shape Stage 3's augment-pipeline cross-check will
consume (see docs/architecture.md).

Usage:
    python -m src.modeling.forecaster --db-path data/telemetry.sqlite
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from src.common.models import SaturationForecast
from src.ingestion.storage import TimeSeriesStore
from src.modeling.trend_fit import (
    fit_weighted_linear_trend,
    hours_to_cross_threshold,
    recency_weights,
)

DEFAULT_THRESHOLD_PCT = 90.0
DEFAULT_HALF_LIFE_FRACTION = 0.25
MIN_SAMPLES_FOR_FORECAST = 5


def _parse_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _cumulative_hours(sample_interval_s: list[int]) -> list[float]:
    """Time axis built from declared window durations, not observed gaps.

    Why not just diff the recorded timestamps: sample_interval_s is, by
    definition (see src/common/models.py), how much real time each sample's
    window covers. Observed wall-clock arrival gaps can diverge from that a
    lot -- most sharply for this project's own synthetic generator, which
    deliberately compresses simulated weeks into real minutes for demos, but
    also for any real pipeline replaying backlog or catching up after a
    consumer outage. Anchoring to the declared interval instead means the
    fitted trend (and the date it projects) reflects what the telemetry
    itself claims about elapsed time, independent of how fast it happened to
    arrive. The tradeoff: this assumes contiguous samples with no dropped
    windows; gap-aware reconciliation would need sequence numbers this
    schema doesn't carry yet.
    """
    hours = [0.0]
    for interval_s in sample_interval_s[1:]:
        hours.append(hours[-1] + interval_s / 3600.0)
    return hours


def build_forecast(
    circuit_id: str,
    history: list[tuple[str, int, float]],
    threshold_pct: float = DEFAULT_THRESHOLD_PCT,
    half_life_fraction: float = DEFAULT_HALF_LIFE_FRACTION,
) -> SaturationForecast:
    """Build a SaturationForecast from (timestamp, sample_interval_s, utilization_pct) history.

    `history` must be ordered ascending by timestamp (TimeSeriesStore
    guarantees this). Takes raw history rather than a store so the trend
    logic is testable against synthetic sequences without touching SQLite.
    """
    now = datetime.now(timezone.utc).isoformat()

    if len(history) < MIN_SAMPLES_FOR_FORECAST:
        return SaturationForecast(
            circuit_id=circuit_id,
            generated_at=now,
            threshold_pct=threshold_pct,
            current_utilization_pct=history[-1][2] if history else 0.0,
            trend_pct_per_day=0.0,
            predicted_saturation_date=None,
            days_to_saturation=None,
            confidence=0.0,
            sample_count=len(history),
        )

    timestamps = [_parse_timestamp(ts) for ts, _, _ in history]
    sample_interval_s = [interval for _, interval, _ in history]
    pct = [p for _, _, p in history]
    hours = _cumulative_hours(sample_interval_s)

    span_hours = hours[-1] - hours[0]
    half_life = max(span_hours * half_life_fraction, 1e-6)
    weights = recency_weights(hours, half_life)

    fit = fit_weighted_linear_trend(hours, pct, weights)
    current_hour = hours[-1]
    hours_ahead = hours_to_cross_threshold(fit, current_hour, threshold_pct)

    predicted_date = None
    days_to_saturation = None
    if hours_ahead is not None:
        predicted_date = (timestamps[-1] + timedelta(hours=hours_ahead)).isoformat()
        days_to_saturation = hours_ahead / 24.0

    return SaturationForecast(
        circuit_id=circuit_id,
        generated_at=now,
        threshold_pct=threshold_pct,
        current_utilization_pct=pct[-1],
        trend_pct_per_day=fit.slope_pct_per_hour * 24.0,
        predicted_saturation_date=predicted_date,
        days_to_saturation=days_to_saturation,
        confidence=fit.r_squared,
        sample_count=len(history),
    )


def _format_forecast(f: SaturationForecast) -> str:
    if f.sample_count < MIN_SAMPLES_FOR_FORECAST:
        return f"{f.circuit_id:<20} not enough data yet ({f.sample_count} samples)"
    if f.predicted_saturation_date is None:
        return (
            f"{f.circuit_id:<20} {f.current_utilization_pct:6.2f}% now, "
            f"trend {f.trend_pct_per_day:+6.3f} pp/day -> not on track to saturate "
            f"(confidence {f.confidence:.2f})"
        )
    return (
        f"{f.circuit_id:<20} {f.current_utilization_pct:6.2f}% now, "
        f"trend {f.trend_pct_per_day:+6.3f} pp/day -> predicted saturation "
        f"{f.predicted_saturation_date} ({f.days_to_saturation:.1f} days out, "
        f"confidence {f.confidence:.2f})"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-path", default="data/telemetry.sqlite")
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--half-life-fraction", type=float, default=DEFAULT_HALF_LIFE_FRACTION)
    args = parser.parse_args()

    store = TimeSeriesStore(args.db_path)
    circuit_ids = store.circuit_ids()
    if not circuit_ids:
        print(f"No telemetry found in {args.db_path} -- run the generator/consumer first.")
        store.close()
        return

    for circuit_id in sorted(circuit_ids):
        history = store.history_for_circuit(circuit_id)
        forecast = build_forecast(
            circuit_id, history,
            threshold_pct=args.threshold_pct,
            half_life_fraction=args.half_life_fraction,
        )
        print(_format_forecast(forecast))

    store.close()


if __name__ == "__main__":
    main()
