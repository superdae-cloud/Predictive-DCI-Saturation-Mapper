"""
Normalization / validation layer.

This is the seam where, in a production build, you'd plug in an actual
sFlow/NetFlow/IPFIX decoder (e.g. wrapping goflow2 or pmacct output) that
maps raw collector packets onto TelemetrySample. Because we're consuming
already-structured JSON from the synthetic generator (or, eventually, a
real collector emitting the same schema), "parsing" here means validating
the schema and rejecting malformed samples rather than binary decoding --
but the pipeline stages after this one never need to know the difference.
"""

from __future__ import annotations

from src.common.models import TelemetrySample


class InvalidTelemetryError(ValueError):
    pass


REQUIRED_FIELDS = {
    "circuit_id", "site_a", "site_b", "link_type", "timestamp",
    "sample_interval_s", "in_octets", "out_octets", "link_capacity_bps",
}


def parse_raw(raw: dict) -> TelemetrySample:
    missing = REQUIRED_FIELDS - raw.keys()
    if missing:
        raise InvalidTelemetryError(f"missing fields: {sorted(missing)}")

    if raw["sample_interval_s"] <= 0:
        raise InvalidTelemetryError("sample_interval_s must be positive")
    if raw["link_capacity_bps"] <= 0:
        raise InvalidTelemetryError("link_capacity_bps must be positive")
    if raw["in_octets"] < 0 or raw["out_octets"] < 0:
        raise InvalidTelemetryError("octet counters cannot be negative")

    return TelemetrySample(
        circuit_id=str(raw["circuit_id"]),
        site_a=str(raw["site_a"]),
        site_b=str(raw["site_b"]),
        link_type=str(raw["link_type"]),
        timestamp=str(raw["timestamp"]),
        sample_interval_s=int(raw["sample_interval_s"]),
        in_octets=int(raw["in_octets"]),
        out_octets=int(raw["out_octets"]),
        link_capacity_bps=int(raw["link_capacity_bps"]),
    )
