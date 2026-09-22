"""
Thin wrapper around kafka-python's KafkaProducer.

Kept separate from the generator so the generator's simulation logic has
zero Kafka-specific code in it, and so the same wrapper could later be
reused by a *real* sFlow/NetFlow collector feeding this pipeline instead of
the synthetic generator.

Messages are keyed by circuit_id. That matters: Kafka guarantees ordering
only within a partition, and keying by circuit_id ensures all samples for
one circuit land on the same partition and are therefore consumed in
timestamp order -- which the trend model downstream depends on.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from src.common.models import TelemetrySample

log = logging.getLogger(__name__)


class KafkaTelemetryProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        # Imported lazily so `--dry-run` works in environments without
        # kafka-python installed or without a broker reachable.
        from kafka import KafkaProducer

        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50,          # small batching window, still near-real-time
            acks="all",
            retries=5,
        )

    def send(self, sample: TelemetrySample) -> None:
        from dataclasses import asdict

        self._producer.send(self.topic, key=sample.circuit_id, value=asdict(sample))

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class DryRunProducer:
    """Drop-in replacement for KafkaTelemetryProducer that just prints.

    Lets you develop/demo the generator's trend-curve logic without a Kafka
    broker running at all.
    """

    def __init__(self, topic: str):
        self.topic = topic

    def send(self, sample: TelemetrySample) -> None:
        print(f"[dry-run -> {self.topic}] {sample.to_json()}")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
