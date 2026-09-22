"""
Local time-series storage for ingested telemetry.

SQLite is deliberately boring here: this project's interesting work is the
streaming ingestion and the forecasting, not the storage engine. One table,
one index on (circuit_id, timestamp), good enough to feed the modeling
stage with `SELECT * WHERE circuit_id = ? ORDER BY timestamp`. Swappable
for TimescaleDB/InfluxDB/Parquet-on-object-storage later without touching
anything upstream of it.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.common.models import TelemetrySample

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_samples (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit_id         TEXT NOT NULL,
    site_a             TEXT NOT NULL,
    site_b             TEXT NOT NULL,
    link_type          TEXT NOT NULL,
    timestamp          TEXT NOT NULL,
    sample_interval_s  INTEGER NOT NULL,
    in_octets          INTEGER NOT NULL,
    out_octets         INTEGER NOT NULL,
    link_capacity_bps  INTEGER NOT NULL,
    utilization_pct    REAL NOT NULL,
    ingested_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_circuit_timestamp
    ON telemetry_samples (circuit_id, timestamp);
"""


class TimeSeriesStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def insert(self, sample: TelemetrySample) -> None:
        self._conn.execute(
            """INSERT INTO telemetry_samples
               (circuit_id, site_a, site_b, link_type, timestamp,
                sample_interval_s, in_octets, out_octets, link_capacity_bps,
                utilization_pct, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sample.circuit_id, sample.site_a, sample.site_b, sample.link_type,
                sample.timestamp, sample.sample_interval_s, sample.in_octets,
                sample.out_octets, sample.link_capacity_bps, sample.utilization_pct,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self._conn.commit()

    def history_for_circuit(self, circuit_id: str) -> list[tuple]:
        """Returns (timestamp, sample_interval_s, utilization_pct) ascending.

        sample_interval_s rides along because the modeling stage builds its
        time axis from declared window durations, not observed wall-clock
        gaps between samples -- see src/modeling/forecaster.py for why.
        """
        cur = self._conn.execute(
            """SELECT timestamp, sample_interval_s, utilization_pct FROM telemetry_samples
               WHERE circuit_id = ? ORDER BY timestamp ASC""",
            (circuit_id,),
        )
        return cur.fetchall()

    def circuit_ids(self) -> list[str]:
        cur = self._conn.execute("SELECT DISTINCT circuit_id FROM telemetry_samples")
        return [row[0] for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
