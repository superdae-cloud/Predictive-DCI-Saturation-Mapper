"""
Tests for Stage 2 forecasting, built against the same four archetype
fixtures ingestion testing uses (circuit_profiles.DEMO_CIRCUITS) so the
forecaster is checked against the exact failure modes it's meant to avoid
(see docs/architecture.md's archetype table).

History is built with synthetic timestamps at known hourly offsets, not by
running the live generator (which stamps real wall-clock "now()" -- too
close together to carry meaningful time deltas for trend fitting). This
mirrors how test_pipeline.py exercises circuit_profiles' pure
utilization_pct(t_hours, rng) function directly.
"""

from datetime import datetime, timedelta, timezone
import random

from src.generator.circuit_profiles import DEMO_CIRCUITS
from src.modeling.forecaster import build_forecast, DEFAULT_THRESHOLD_PCT
from src.modeling.trend_fit import (
    fit_weighted_linear_trend,
    hours_to_cross_threshold,
    recency_weights,
)

BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _profile(trend_type: str):
    return next(p for p in DEMO_CIRCUITS if p.trend_type == trend_type)


def _history(profile, hours: list[float], rng: random.Random) -> list[tuple[str, int, float]]:
    """Builds (timestamp, sample_interval_s, pct) history.

    Timestamps are real wall-clock stamps (as storage would have them), but
    what actually drives the forecaster's time axis is sample_interval_s --
    set here to match the hour spacing used to generate `pct`, exactly like
    a real fixed-cadence collector would report.
    """
    interval_s = int(round((hours[1] - hours[0]) * 3600)) if len(hours) > 1 else 60
    return [
        ((BASE_TIME + timedelta(hours=t)).isoformat(), interval_s, profile.utilization_pct(t, rng))
        for t in hours
    ]


def test_weighted_fit_recovers_a_known_line():
    hours = list(range(0, 100, 5))
    pct = [10.0 + 0.5 * h for h in hours]  # noiseless: pct = 10 + 0.5*hour
    weights = recency_weights(hours, half_life_hours=25.0)

    fit = fit_weighted_linear_trend(hours, pct, weights)

    assert abs(fit.slope_pct_per_hour - 0.5) < 1e-6
    assert abs(fit.intercept_pct - 10.0) < 1e-6
    assert fit.r_squared > 0.999


def test_linear_ramp_predicts_a_future_saturation_date():
    profile = _profile("linear_ramp")
    rng = random.Random(1)
    hours = list(range(0, 168 * 3, 4))  # 3 simulated weeks, every 4h
    history = _history(profile, hours, rng)

    forecast = build_forecast(profile.circuit_id, history)

    # start_pct=42, growth_pct_per_week=6.5 -> crosses 90% around week 7.4
    # (~52 sim days from t=0); with 3 weeks of history that's comfortably
    # in the future relative to the last sample.
    assert forecast.predicted_saturation_date is not None
    assert forecast.trend_pct_per_day > 0
    last_sample_time = BASE_TIME + timedelta(hours=hours[-1])
    assert datetime.fromisoformat(forecast.predicted_saturation_date) > last_sample_time
    assert forecast.confidence > 0.8  # noise_std_pct=1.5 on a clean linear signal


def test_step_change_forecast_reacts_faster_than_a_naive_full_history_fit():
    profile = _profile("step_change")  # jumps 35% -> 88% at hour 72
    rng = random.Random(2)
    # Only ~9h of data past the step -- the window where a naive fit still
    # has most of its history at the old, low baseline. Far past the step
    # (e.g. a full week later), even a naive fit converges on the same
    # answer, so this window is where the two approaches actually diverge.
    hours = list(range(0, 90, 2))
    history = _history(profile, hours, rng)

    forecast = build_forecast(profile.circuit_id, history)

    # It really is already near/at the plateau -- this isn't noise.
    assert forecast.current_utilization_pct > 80.0

    # A plain, unweighted OLS over the same data blends the low pre-jump
    # baseline into a gentle slope and projects saturation days away, even
    # though the circuit is already sitting at the new plateau right now.
    naive_fit = fit_weighted_linear_trend(hours, [p for _, _, p in history], [1.0] * len(hours))
    naive_hours_ahead = hours_to_cross_threshold(naive_fit, hours[-1], DEFAULT_THRESHOLD_PCT)
    assert naive_hours_ahead is not None and naive_hours_ahead > 24.0

    # The recency-weighted forecast should not repeat that under-reaction:
    # it's already at/near threshold, so it should flag it as imminent.
    assert forecast.days_to_saturation is not None
    assert forecast.days_to_saturation < 1.0


def test_seasonal_growth_forecast_is_not_derailed_by_daily_weekly_cycles():
    profile = _profile("seasonal_growth")
    rng = random.Random(3)
    hours = list(range(0, 168 * 4, 2))  # 4 simulated weeks, every 2h
    history = _history(profile, hours, rng)

    forecast = build_forecast(profile.circuit_id, history)

    # start_pct=55, growth_pct_per_week=4.0 -> crosses 90% around week 8.75.
    # Loose bounds: the point is that a positive, plausible trend is
    # recovered despite the daily/weekly sinusoid layered on top, not exact
    # agreement with the noiseless math.
    assert forecast.trend_pct_per_day > 0
    assert forecast.predicted_saturation_date is not None
    assert 0 < forecast.days_to_saturation < 168  # sanity: within ~24 sim weeks


def test_healthy_plateau_is_never_forecast_to_saturate():
    profile = _profile("healthy_plateau")
    rng = random.Random(4)
    hours = list(range(0, 168 * 8, 6))  # 8 simulated weeks
    history = _history(profile, hours, rng)

    forecast = build_forecast(profile.circuit_id, history, threshold_pct=DEFAULT_THRESHOLD_PCT)

    assert forecast.predicted_saturation_date is None
    assert forecast.days_to_saturation is None


def test_insufficient_history_returns_a_no_confidence_placeholder():
    forecast = build_forecast("DCI-NEW-CIRCUIT", [(BASE_TIME.isoformat(), 60, 10.0)])

    assert forecast.predicted_saturation_date is None
    assert forecast.confidence == 0.0
    assert forecast.sample_count == 1


def test_already_past_threshold_predicts_immediate_saturation():
    hours = list(range(0, 20))
    history = [((BASE_TIME + timedelta(hours=h)).isoformat(), 3600, 95.0) for h in hours]

    forecast = build_forecast("DCI-HOT", history, threshold_pct=90.0)

    assert forecast.days_to_saturation == 0.0
    assert forecast.predicted_saturation_date is not None
