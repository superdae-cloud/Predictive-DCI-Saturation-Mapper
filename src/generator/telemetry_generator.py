"""
Synthetic telemetry generator / producer CLI.

Simulates a fleet of DCI circuits streaming utilization telemetry, the way
a real deployment would receive it from an sFlow/NetFlow/IPFIX collector
sitting in front of your routers. Instead of decoding real packets, we
compute each circuit's utilization from the trend curves in
circuit_profiles.py and turn that back into plausible byte counters.

Simulated time runs faster than wall-clock time (--sim-hours-per-tick vs
--tick-seconds) so you can watch weeks of trend, including a step-change or
a circuit crossing saturation, unfold in minutes.

Examples:
    # Print samples to stdout, no Kafka needed - good for a first look.
    python -m src.generator.telemetry_generator --dry-run --max-ticks 20

    # Stream into a local Kafka broker (see docker-compose.yml).
    python -m src.generator.telemetry_generator --bootstrap-server localhost:9092
"""

from __future__ import annotations

import argparse
import random
import time

from src.common.models import TelemetrySample
from src.generator.circuit_profiles import DEMO_CIRCUITS, CircuitProfile


def utilization_to_sample(
    profile: CircuitProfile,
    t_hours: float,
    sample_interval_s: int,
    rng: random.Random,
) -> TelemetrySample:
    """Turn a profile's utilization at time t into a byte-counter sample.

    DCI backbone links are rarely perfectly symmetric, so the peak
    direction (whichever of in/out the profile's utilization represents)
    gets the full value and the other direction gets a randomized fraction
    of it, alternating which side leads to avoid an unrealistic constant
    skew.
    """
    util_pct = profile.utilization_pct(t_hours, rng)
    peak_bps = profile.capacity_bps * (util_pct / 100.0)
    other_ratio = rng.uniform(0.55, 0.90)
    other_bps = peak_bps * other_ratio

    if rng.random() < 0.5:
        in_bps, out_bps = peak_bps, other_bps
    else:
        in_bps, out_bps = other_bps, peak_bps

    in_octets = int((in_bps * sample_interval_s) / 8)
    out_octets = int((out_bps * sample_interval_s) / 8)

    return TelemetrySample(
        circuit_id=profile.circuit_id,
        site_a=profile.site_a,
        site_b=profile.site_b,
        link_type=profile.link_type,
        timestamp=TelemetrySample.now_iso(),
        sample_interval_s=sample_interval_s,
        in_octets=in_octets,
        out_octets=out_octets,
        link_capacity_bps=profile.capacity_bps,
    )


def build_producer(args):
    if args.dry_run:
        from src.ingestion.producer import DryRunProducer
        return DryRunProducer(topic=args.topic)
    from src.ingestion.producer import KafkaTelemetryProducer
    return KafkaTelemetryProducer(bootstrap_servers=args.bootstrap_server, topic=args.topic)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server(s)")
    parser.add_argument("--topic", default="dci.telemetry.raw", help="Kafka topic to publish to")
    parser.add_argument("--tick-seconds", type=float, default=5.0, help="Real wall-clock seconds between emitted batches")
    parser.add_argument("--sim-hours-per-tick", type=float, default=1.0, help="Simulated hours advanced per tick")
    parser.add_argument("--sample-interval-s", type=int, default=60, help="Aggregation window each sample represents")
    parser.add_argument("--max-ticks", type=int, default=0, help="Stop after N ticks (0 = run forever)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed, for reproducible demo runs")
    parser.add_argument("--dry-run", action="store_true", help="Print samples instead of publishing to Kafka")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    producer = build_producer(args)

    print(
        f"Starting telemetry generator: {len(DEMO_CIRCUITS)} circuits, "
        f"{args.sim_hours_per_tick}h simulated per {args.tick_seconds}s tick "
        f"(1 simulated week ~= {168 / args.sim_hours_per_tick * args.tick_seconds:.0f}s wall-clock)"
    )

    t_hours = 0.0
    tick = 0
    try:
        while True:
            for profile in DEMO_CIRCUITS:
                sample = utilization_to_sample(profile, t_hours, args.sample_interval_s, rng)
                producer.send(sample)
            producer.flush()

            tick += 1
            t_hours += args.sim_hours_per_tick
            if args.max_ticks and tick >= args.max_ticks:
                break
            time.sleep(args.tick_seconds)
    except KeyboardInterrupt:
        print("\nStopping (Ctrl-C).")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
