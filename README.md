# Predictive DCI Saturation Mapper

Turns capacity firefighting into a scheduling problem: ingest streaming
telemetry from data center interconnect (DCI) circuits, model each
circuit's trend curve, predict *when* it will hit saturation (weeks out,
not after the fact), and cross-check that predicted exhaustion date
against your capacity-augment / leased-circuit pipeline — flagging any
circuit that will saturate before its planned upgrade lands.

> **Status: all 4 stages built.** Streaming ingestion, trend forecasting,
> augment-pipeline cross-checking, and alerting/dashboard are all done and
> tested — see [Roadmap](#roadmap).

---

## Why this exists

Most network capacity tooling (Kentik, ThousandEyes, Crosswork Planning,
NETSCOUT, TEOCO SmartCircuit, ...) does general capacity monitoring or
historical trend reporting. Dedicated tools that predict *DCI-specific*
saturation weeks ahead **and** check that prediction against your actual
augment pipeline are rare — most shops do this cross-check by hand, in a
spreadsheet, after someone already noticed a link running hot. That's the
gap this project targets.

## How it's meant to work (full vision)

```
 ┌────────────────┐     ┌───────────┐     ┌──────────────────┐     ┌─────────────────────┐
 │ sFlow / NetFlow │ --> │   Kafka   │ --> │  Trend Model       │ --> │ Augment Pipeline      │
 │ telemetry        │     │ (streaming│     │  (forecast days-to-│     │ Cross-Check           │
 │ (or synthetic    │     │  ingest)  │     │   saturation per   │     │ (flag: will saturate  │
 │  generator, today)│    │           │     │   circuit)         │     │  before planned upgrade│
 └────────────────┘     └───────────┘     └──────────────────┘     └─────────────────────┘
        Stage 1              Stage 1              Stage 2                    Stage 3
                                                                          -> Stage 4: alerting
```

**Stage 1 (built):** a synthetic telemetry generator simulates a fleet of
DCI circuits with realistic utilization trend curves and streams
sFlow/NetFlow-shaped samples through Kafka. A consumer validates and
persists them to a local time-series store. This gives every later stage
clean, ordered history to work from — real collector integration can
replace the generator later without touching anything downstream.

**Stage 2 (built):** forecast each circuit's trend curve forward and
estimate a predicted saturation date. This is the "AI" part — recency-
weighted regression (`src/modeling/`) that fits each circuit's history and
solves for when it crosses a saturation threshold.

**Stage 3 (built):** cross-reference each predicted saturation date
against a capacity-augment / leased-circuit pipeline — a CSV of "circuit
X gets upgraded to Y Gbps on date Z" (`src/planning/`).

**Stage 4 (built):** flag/alert any circuit where predicted saturation
< planned upgrade date, i.e. the scheduling conflict this whole project
exists to catch.

## Concepts, briefly

- **sFlow / NetFlow / IPFIX**: protocols routers use to export traffic
  telemetry (packet/byte counters, sampled or full) to a collector. In
  production you'd run something like `goflow2` or `pmacct` in front of
  your routers to decode these into structured records. This project
  models that *decoded* record shape directly (see
  `src/common/models.py`) so the rest of the pipeline is agnostic to
  where the bytes came from — synthetic generator today, a real collector
  later, same schema.
- **Why Kafka**: DCI telemetry is naturally a stream (samples arrive
  continuously, forever) and multiple things eventually want to read it
  (storage, live dashboards, alerting) without slowing down or blocking
  each other. Kafka decouples "something is producing telemetry" from
  "something is consuming it," and keeps per-circuit ordering when you
  key messages by circuit ID (see `src/ingestion/producer.py`).
- **Trend curves**: the synthetic generator models four archetypes
  (`src/generator/circuit_profiles.py`) deliberately chosen to stress the
  forecasting stage differently: steady linear growth (easy), a sudden
  step-change like a new tenant turning up (tests reaction speed), a
  seasonal circuit with daily/weekly cycles on top of growth (tests
  separating signal from noise), and a flat "healthy" plateau (tests that
  the model correctly stays quiet).

## Repo layout

```
src/
  common/models.py         TelemetrySample, SaturationForecast, AugmentPlan,
                              CrossCheckResult - shared record shapes
  generator/                synthetic telemetry (stands in for a real
    circuit_profiles.py       sFlow/NetFlow collector during development)
    telemetry_generator.py
  ingestion/
    producer.py              Kafka producer wrapper
    consumer.py              Kafka consumer -> parse -> store
    parser.py                schema validation / normalization
    storage.py                SQLite time-series store
  modeling/
    trend_fit.py              pure trend-fitting math (weighted least squares)
    forecaster.py              reads storage, emits SaturationForecast per circuit
  planning/
    augment_pipeline.py        loads the planned-upgrade CSV
    cross_check.py               forecast vs. plan -> CrossCheckResult per circuit;
                                    also exposes compute_cross_checks(), the shared
                                    Stage 2+3 orchestration Stage 4 reuses
  alerting/
    notifier.py                 pluggable alert delivery (LogNotifier, SlackWebhookNotifier)
    dashboard.py                  renders a static HTML status snapshot
    pipeline.py                    Stage 4 driver: cross-check -> dashboard -> alerts
scripts/
  generate_demo_augment_plan.py  (re)generates a demo augment_pipeline.csv
                                    calibrated to current forecast data
tests/
  test_pipeline.py           generator -> parser -> storage smoke tests
  test_forecaster.py          trend-fit math + forecaster vs. the four archetypes
  test_cross_check.py          Stage 3 status logic + CSV loading/validation
  test_notifier.py              Stage 4 notifier backends (webhook mocked/failed)
  test_dashboard.py              dashboard HTML rendering + escaping
  test_alerting_pipeline.py       alert dispatch only fires for flagged statuses
docker-compose.yml            single-node Kafka (KRaft) + Kafka UI
```

## Setup

This was developed to run in **WSL2 (Ubuntu) with Docker Desktop's WSL2
backend** — a reasonable default if you're building CLI/Linux skills
alongside this project. Should work the same in any Linux/macOS shell
with Docker installed.

```bash
# 1. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Start Kafka (KRaft mode, no ZooKeeper) + Kafka UI
docker compose up -d
docker compose ps      # wait until kafka is "healthy"
```

Kafka UI will be at http://localhost:8080 once it's up — useful for
watching messages land on the `dci.telemetry.raw` topic in real time.

## Running the demo end-to-end

Open two terminals (both with the venv activated).

**Terminal 1 — start the generator** (simulates ~1 week of trend every
~2.3 minutes at these defaults):

```bash
python -m src.generator.telemetry_generator
```

**Terminal 2 — start the consumer** (persists to SQLite, prints a line
per sample, flags anything crossing 85% utilization):

```bash
python -m src.ingestion.consumer --db-path data/telemetry.sqlite
```

Watch `DCI-ASH-DAL-W1` climb steadily, `DCI-SJC-PDX-W2` jump suddenly
around simulated hour 72, `DCI-NYC-CHI-W1` oscillate daily while trending
up, and `DCI-LON-FRA-W1` stay flat and unflagged. Stop both with Ctrl-C
whenever — state persists in `data/telemetry.sqlite` between runs.

## Running the forecaster (Stage 2)

Once some history is in storage (from either path above):

```bash
python -m src.modeling.forecaster --db-path data/telemetry.sqlite
```

Prints one line per circuit: current utilization, fitted trend
(percentage points/day), and — if the trend is positive and meaningful —
a predicted saturation date and days-out, plus a confidence score (the
fit's weighted R²). `DCI-LON-FRA-W1` (the flat plateau) should always come
back "not on track to saturate"; the other three should each get a
predicted date, with `DCI-SJC-PDX-W2` (the step-change circuit) generally
showing lower confidence right after its jump than `DCI-ASH-DAL-W1` (the
clean linear ramp) — see `docs/architecture.md` for why.

Useful flags: `--threshold-pct` (default 90) and `--half-life-fraction`
(default 0.25 — see architecture notes for what this controls).

## Running the cross-check (Stage 3)

Stage 3 compares each circuit's forecast against a planned-upgrade table
(`data/augment_pipeline.csv`) — the actual scheduling-conflict check this
project exists to automate. That CSV isn't checked into git (see
`.gitignore`): its dates are calibrated against whatever's currently in
storage, so a committed copy would look stale the moment time passes.
Generate one from your current demo data instead:

```bash
python -m scripts.generate_demo_augment_plan --db-path data/telemetry.sqlite
python -m src.planning.cross_check --db-path data/telemetry.sqlite
```

The demo script deliberately sets up all four outcomes so you see each
status at least once: `DCI-ASH-DAL-W1` gets a plan set *after* its
predicted saturation (`at_risk` — the headline scheduling conflict),
`DCI-NYC-CHI-W1` gets a plan set comfortably *before* (`on_track`),
`DCI-SJC-PDX-W2` gets no plan row at all despite already being near
saturation (`missing_plan` — arguably the most urgent status, since
nothing is even scheduled), and `DCI-LON-FRA-W1` also gets no row but
isn't predicted to saturate, so it's correctly not flagged
(`not_on_track`).

For your own data, hand-edit or generate `data/augment_pipeline.csv`
yourself with columns `circuit_id,planned_upgrade_date,new_capacity_bps`
— one row per circuit with a scheduled upgrade; circuits with no planned
upgrade simply have no row.

## Running the alerting pipeline (Stage 4)

Runs the same Stage 2+3 cross-check, then writes a static HTML dashboard
and sends one alert per flagged circuit:

```bash
python -m src.alerting.pipeline --db-path data/telemetry.sqlite \
    --augment-plan-path data/augment_pipeline.csv \
    --dashboard-path data/dashboard.html
```

Open `data/dashboard.html` in a browser for a color-coded table of every
circuit's current utilization, trend, predicted saturation date, planned
upgrade, confidence, and status. By default alerts just print to stdout
(`LogNotifier`) — pass `--slack-webhook-url` to post them to a
[Slack incoming webhook](https://api.slack.com/messaging/webhooks)
instead. A failed webhook delivery falls back to printing rather than
crashing the run; alerting should never be the reason a forecast pipeline
fails.

Run this on a schedule (cron, a CI job, whatever) against a live
deployment's storage and this is the whole alerting loop end to end.

## Circuit topology view

`docs/topology.html` is a standalone, interactive network diagram of the
demo fleet — open it directly in a browser. Each circuit is a clickable
link whose fill shows current utilization against capacity, colored by
its Stage 3 cross-check status. Click a circuit to open a page where you
can model adding bandwidth and see utilization, trend, predicted
saturation, and status recalculate live. It's a snapshot from one
pipeline run (baked-in reference numbers, not a live read of
`telemetry.sqlite`) — see the model note on each circuit's detail page
for exactly how the recalculation works.

### Trying it without Kafka running

Both the generator and consumer work without a broker, for quick
iteration on the trend-curve or parsing logic:

```bash
# Print samples instead of publishing
python -m src.generator.telemetry_generator --dry-run --max-ticks 20

# Feed a file of newline-delimited JSON samples straight into storage
python -m src.ingestion.consumer --replay-file samples.jsonl --db-path data/telemetry.sqlite
```

## Tests

```bash
pytest -q
```

Covers the utilization math, parser validation, each trend archetype's
behavior, a generator-to-storage round trip, the trend-fit math and
forecaster behavior against all four archetypes, the Stage 3 cross-check's
status logic (including CSV loading/validation), and Stage 4's notifier
backends, dashboard rendering, and alert dispatch. The Kafka
producer/consumer branches are exercised manually against
`docker compose` rather than in the test suite, since spinning up a real
broker isn't practical in a unit test.

## Troubleshooting

- **`sqlite3.OperationalError: disk I/O error`** on `--db-path`: SQLite
  needs real file-locking support, which some network drives, some
  cloud-synced folders (OneDrive/Dropbox in certain modes), and some
  virtual/container filesystem mounts don't provide correctly. If you hit
  this, point `--db-path` at a plain local path (e.g. somewhere under
  your Linux home in WSL, not a synced Windows folder) — the code itself
  isn't the problem.
- **Kafka connection refused**: give the broker a few seconds after
  `docker compose up -d` — check `docker compose ps` shows `kafka` as
  `healthy` before starting the generator/consumer.

## Roadmap

- [x] Stage 1 — synthetic telemetry + streaming ingestion (Kafka) + validated storage
- [x] Stage 2 — trend forecasting per circuit (`src/modeling/`): fit each
      circuit's history, predict days-to-saturation, quantify confidence
- [x] Stage 3 — augment/capacity pipeline model (`src/planning/`) + cross-check
      forecasts against planned upgrade dates
- [x] Stage 4 — alerting/flagging (`src/alerting/`) + a static dashboard view
      of every circuit's status
