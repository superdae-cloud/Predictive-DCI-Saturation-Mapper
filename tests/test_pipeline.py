"""
Smoke tests covering the ingestion path end-to-end without needing a live
Kafka broker: generator -> parser -> storage. Kafka itself (producer.py /
consumer.py's Kafka branch) is exercised manually against docker-compose,
since spinning up a broker isn't practical inside a unit test.
"""

import random
import tempfile
import os

from src.common.models import TelemetrySample
from src.generator.circuit_profiles import DEMO_CIRCUITS
from src.generator.telemetry_generator import utilization_to_sample
from src.ingestion.parser import parse_raw, InvalidTelemetryError
from src.ingestion.storage import TimeSeriesStore
from dataclasses import asdict


def test_utilization_pct_matches_expected_ratio():
    s = TelemetrySample(
        circuit_id="X", site_a="A", site_b="B", link_type="leased",
        timestamp=TelemetrySample.now_iso(), sample_interval_s=60,
        in_octets=375_000_000, out_octets=300_000_000,
        link_capacity_bps=10_000_000_000,
    )
    # peak direction = in_octets: (375_000_000*8)/60 = 50,000,000 bps = 0.5% of 10G
    assert abs(s.utilization_pct - 0.5) < 1e-6


def test_parser_rejects_missing_and_invalid_fields():
    good = asdict(TelemetrySample(
        circuit_id="X", site_a="A", site_b="B", link_type="leased",
        timestamp=TelemetrySample.now_iso(), sample_interval_s=60,
        in_octets=1, out_octets=1, link_capacity_bps=1000,
    ))
    parse_raw(good)  # should not raise

    missing = dict(good)
    del missing["circuit_id"]
    try:
        parse_raw(missing)
        assert False, "expected InvalidTelemetryError"
    except InvalidTelemetryError:
        pass

    negative = dict(good)
    negative["in_octets"] = -5
    try:
        parse_raw(negative)
        assert False, "expected InvalidTelemetryError"
    except InvalidTelemetryError:
        pass


def test_linear_ramp_circuit_trends_upward_and_crosses_saturation():
    profile = next(p for p in DEMO_CIRCUITS if p.trend_type == "linear_ramp")
    rng = random.Random(1)

    pct_week0 = profile.utilization_pct(0, rng)
    pct_week8 = profile.utilization_pct(9 * 168, rng)  # 9 simulated weeks out
    assert pct_week8 > pct_week0
    assert pct_week8 >= 95.0  # growth_pct_per_week * 9 weeks pushes it near/over capacity


def test_step_change_circuit_jumps_at_threshold():
    profile = next(p for p in DEMO_CIRCUITS if p.trend_type == "step_change")
    rng = random.Random(2)
    before = profile.utilization_pct(profile.step_at_hours - 1, rng)
    after = profile.utilization_pct(profile.step_at_hours + 1, rng)
    assert after - before > 20  # a real jump, not noise


def test_healthy_plateau_never_flagged_as_watch():
    profile = next(p for p in DEMO_CIRCUITS if p.trend_type == "healthy_plateau")
    rng = random.Random(3)
    for t in range(0, 168 * 8, 24):  # sample daily across 8 simulated weeks
        assert profile.utilization_pct(t, rng) < 85.0


def test_generator_to_storage_round_trip():
    rng = random.Random(42)
    profile = DEMO_CIRCUITS[0]

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.sqlite")
        store = TimeSeriesStore(db_path)

        for t in range(0, 24, 6):
            sample = utilization_to_sample(profile, t, sample_interval_s=60, rng=rng)
            parsed = parse_raw(asdict(sample))
            store.insert(parsed)

        history = store.history_for_circuit(profile.circuit_id)
        assert len(history) == 4
        store.close()
