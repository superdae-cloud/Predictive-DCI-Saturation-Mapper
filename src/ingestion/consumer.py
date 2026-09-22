"""
Ingestion consumer CLI.

Subscribes to the raw telemetry topic, validates/normalizes each message
(parser.py), persists it (storage.py), and prints a one-line log per
sample -- including a loud marker when a sample crosses a watch threshold,
as a preview of the alerting stage that gets built properly once the
forecasting model (src/modeling/) is in place. This consumer does NOT
decide "will this circuit saturate" -- it just gets clean, ordered
history into storage so that stage has something to work with.

Usage:
    python -m src.ingestion.consumer --db-path data/telemetry.sqlite
"""

from __future__ import annotations

import argparse
import json

from src.ingestion.parser import parse_raw, InvalidTelemetryError
from src.ingestion.storage import TimeSeriesStore

WATCH_THRESHOLD_PCT = 85.0


def handle_message(raw: dict, store: TimeSeriesStore) -> None:
    try:
        sample = parse_raw(raw)
    except InvalidTelemetryError as e:
        print(f"[REJECTED] {e}: {raw}")
        return

    store.insert(sample)
    marker = " <-- WATCH (>{:.0f}%)".format(WATCH_THRESHOLD_PCT) if sample.utilization_pct >= WATCH_THRESHOLD_PCT else ""
    print(f"[{sample.timestamp}] {sample.circuit_id:<20} {sample.utilization_pct:6.2f}%{marker}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="dci.telemetry.raw")
    parser.add_argument("--group-id", default="dci-ingestion-consumer")
    parser.add_argument("--db-path", default="data/telemetry.sqlite")
    parser.add_argument("--replay-file", default=None, help="Read newline-delimited JSON samples from a file instead of Kafka (for offline testing)")
    args = parser.parse_args()

    store = TimeSeriesStore(args.db_path)

    if args.replay_file:
        with open(args.replay_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    handle_message(json.loads(line), store)
        store.close()
        return

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    print(f"Consuming '{args.topic}' from {args.bootstrap_server} (group={args.group_id})...")
    try:
        for msg in consumer:
            handle_message(msg.value, store)
    except KeyboardInterrupt:
        print("\nStopping (Ctrl-C).")
    finally:
        store.close()


if __name__ == "__main__":
    main()
