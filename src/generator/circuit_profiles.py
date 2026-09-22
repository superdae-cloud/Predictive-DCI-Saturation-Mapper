"""
Synthetic circuit definitions.

Each profile describes how a circuit's utilization evolves over *simulated*
time (in hours since the generator started). The generator advances
simulated time faster than wall-clock time so you can watch weeks of trend
unfold in a few minutes during development/demos.

Four trend archetypes are modeled, deliberately chosen to give the
downstream forecasting stage (see src/modeling/) a mix of easy and hard
cases:

  linear_ramp      - steady organic growth. The easy case: a straight line
                     fit should predict this well.
  step_change      - flat, then a sudden jump (e.g. a new tenant/service
                     turns up on the link). Tests whether the model reacts
                     fast enough after a regime change instead of averaging
                     it away.
  seasonal_growth  - a slow upward trend with strong daily + weekly cycles
                     layered on top (typical of user-facing traffic). Tests
                     whether the model can separate signal (the trend) from
                     noise (the cycle) instead of alarming on every daily
                     peak.
  healthy_plateau  - stays flat with just noise. A "control" circuit that
                     should NEVER get flagged, so demos also show the model
                     correctly staying quiet.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import math
import random

TrendType = Literal["linear_ramp", "step_change", "seasonal_growth", "healthy_plateau"]


@dataclass(frozen=True)
class CircuitProfile:
    circuit_id: str
    site_a: str
    site_b: str
    link_type: str
    capacity_bps: int
    trend_type: TrendType
    start_pct: float          # utilization % at t=0
    noise_std_pct: float      # stddev of per-sample noise, in percentage points

    # trend-specific parameters (unused ones are ignored per trend_type)
    growth_pct_per_week: float = 0.0
    step_at_hours: float = 0.0
    step_to_pct: float = 0.0
    seasonal_amplitude_pct: float = 0.0

    def utilization_pct(self, t_hours: float, rng: random.Random) -> float:
        """Return a noisy utilization percentage at simulated hour t_hours."""
        base = self._trend_pct(t_hours)
        noisy = base + rng.gauss(0, self.noise_std_pct)
        return max(0.0, min(100.0, noisy))

    def _trend_pct(self, t_hours: float) -> float:
        weeks = t_hours / 168.0

        if self.trend_type == "linear_ramp":
            return self.start_pct + self.growth_pct_per_week * weeks

        if self.trend_type == "step_change":
            return self.step_to_pct if t_hours >= self.step_at_hours else self.start_pct

        if self.trend_type == "seasonal_growth":
            trend = self.start_pct + self.growth_pct_per_week * weeks
            daily = self.seasonal_amplitude_pct * math.sin(2 * math.pi * t_hours / 24.0)
            weekly = (self.seasonal_amplitude_pct / 2) * math.sin(2 * math.pi * t_hours / 168.0)
            return trend + daily + weekly

        if self.trend_type == "healthy_plateau":
            return self.start_pct

        raise ValueError(f"unknown trend_type: {self.trend_type}")


# A small demo fleet. Capacities are realistic-ish DCI sizes (10G/100G waves).
DEMO_CIRCUITS: list[CircuitProfile] = [
    CircuitProfile(
        circuit_id="DCI-ASH-DAL-W1",
        site_a="ASH", site_b="DAL", link_type="leased-wave",
        capacity_bps=100_000_000_000,
        trend_type="linear_ramp",
        start_pct=42.0, noise_std_pct=1.5,
        growth_pct_per_week=6.5,   # will cross 100% well within weeks -> the headline case
    ),
    CircuitProfile(
        circuit_id="DCI-SJC-PDX-W2",
        site_a="SJC", site_b="PDX", link_type="owned-dark-fiber",
        capacity_bps=100_000_000_000,
        trend_type="step_change",
        start_pct=35.0, noise_std_pct=1.0,
        step_at_hours=72, step_to_pct=88.0,   # a tenant onboards on day 3
    ),
    CircuitProfile(
        circuit_id="DCI-NYC-CHI-W1",
        site_a="NYC", site_b="CHI", link_type="leased-wave",
        capacity_bps=10_000_000_000,
        trend_type="seasonal_growth",
        start_pct=55.0, noise_std_pct=1.2,
        growth_pct_per_week=4.0, seasonal_amplitude_pct=8.0,
    ),
    CircuitProfile(
        circuit_id="DCI-LON-FRA-W1",
        site_a="LON", site_b="FRA", link_type="leased-wave",
        capacity_bps=100_000_000_000,
        trend_type="healthy_plateau",
        start_pct=28.0, noise_std_pct=1.0,
    ),
]
