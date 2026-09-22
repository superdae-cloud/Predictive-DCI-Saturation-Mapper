"""
Shared data model for a single telemetry sample.

In a real deployment, records like this are what you'd get AFTER decoding
raw sFlow/NetFlow/IPFIX packets from a collector (e.g. via `pmacct`,
`goflow2`, or a vendor telemetry stream) and mapping counters onto a known
circuit. We model that decoded shape directly so the rest of the pipeline
(Kafka, parsing, storage, forecasting) doesn't care whether the bytes
underneath came from a real router or our synthetic generator.

Field naming loosely follows common NetFlow/IPFIX fields:
  - in_octets / out_octets  -> bytes counters over the sample window
  - link_capacity_bps       -> provisioned circuit capacity (from inventory,
                               not something you get off the wire)
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json


@dataclass(frozen=True)
class TelemetrySample:
    circuit_id: str            # stable identifier, e.g. "DCI-EQX-ASH-DAL-01"
    site_a: str                 # origin site / PoP
    site_b: str                 # destination site / PoP
    link_type: str               # "leased", "owned-dark-fiber", "wave", etc.
    timestamp: str                # ISO-8601 UTC, when the sample window ended
    sample_interval_s: int        # length of the aggregation window
    in_octets: int                 # bytes received in the window
    out_octets: int                 # bytes sent in the window
    link_capacity_bps: int           # provisioned capacity of the circuit

    @property
    def utilization_pct(self) -> float:
        """Peak-direction utilization for this sample window, as a percent."""
        bits_in = self.in_octets * 8
        bits_out = self.out_octets * 8
        peak_bps = max(bits_in, bits_out) / self.sample_interval_s
        return round((peak_bps / self.link_capacity_bps) * 100, 3)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "TelemetrySample":
        return TelemetrySample(**json.loads(raw))

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class SaturationForecast:
    """Output of the Stage 2 modeling step for one circuit.

    This is the shape Stage 3's augment-pipeline cross-check will consume
    (see docs/architecture.md): for each forecast, compare
    predicted_saturation_date against that circuit's planned upgrade date.
    """

    circuit_id: str
    generated_at: str                       # ISO-8601 UTC, when this forecast was computed
    threshold_pct: float                     # saturation threshold this forecast targets
    current_utilization_pct: float            # most recent observed sample, not fitted
    trend_pct_per_day: float                   # fitted slope, in percentage points/day
    predicted_saturation_date: str | None       # ISO-8601 UTC, or None if not on track
    days_to_saturation: float | None              # None if not on track, else >= 0
    confidence: float                          # weighted R^2 of the trend fit, 0..1
    sample_count: int                          # samples the fit was based on

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> "SaturationForecast":
        return SaturationForecast(**json.loads(raw))
