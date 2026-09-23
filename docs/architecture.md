# Architecture notes

Design decisions and data flow for Stage 1 (ingestion), plus the
extension points later stages will plug into. Written as a reference for
future-me and for anyone reviewing this project, not just a code tour.

## Data flow, Stage 1

```
CircuitProfile.utilization_pct(t)      <- trend curve math (pure function, no I/O)
        |
        v
telemetry_generator.utilization_to_sample()   <- turns a % into plausible in/out octet counters
        |
        v
TelemetrySample (dataclass)             <- the shared record shape, everywhere downstream
        |
        v
KafkaTelemetryProducer.send()           <- keyed by circuit_id -> partition -> ordering preserved
        |
        v
   [ Kafka topic: dci.telemetry.raw ]
        |
        v
KafkaConsumer (consumer.py)
        |
        v
parser.parse_raw()                       <- schema validation; this is where a real
        |                                    sFlow/NetFlow/IPFIX decoder would plug in
        v
TimeSeriesStore.insert()                 <- SQLite, indexed on (circuit_id, timestamp)
```

## Why a dataclass as the spine (`TelemetrySample`)

Every stage — generator, Kafka producer, parser, storage, and eventually
the forecasting stage — passes this one shape around. That's deliberate:
it means the generator can be swapped for a real collector, or SQLite
swapped for TimescaleDB, without any other module changing, as long as
they still produce/consume a `TelemetrySample`. `utilization_pct` is
computed as a property rather than stored redundantly, so there's exactly
one formula for it in the whole codebase.

## Why key Kafka messages by `circuit_id`

Kafka only guarantees ordering *within a partition*, not across an entire
topic. The forecasting stage needs each circuit's samples in timestamp
order to fit a trend — if two samples for `DCI-ASH-DAL-W1` could land on
different partitions and be consumed out of order, the model would see a
scrambled history. Keying by `circuit_id` guarantees same-circuit samples
always land on the same partition, at the cost of needing enough
partitions that no single circuit's volume becomes a bottleneck (a
non-issue at this project's scale).

## Why four trend archetypes, specifically

The point of `circuit_profiles.py` isn't just "generate plausible-looking
numbers" — it's to give Stage 2 (forecasting) a fixture set that exposes
different failure modes a naive model would have:

| Archetype          | What it tests in the forecasting stage                          |
|---------------------|-------------------------------------------------------------------|
| `linear_ramp`        | Baseline: does the simplest possible model even work?             |
| `step_change`        | Does the model react to a regime change, or does it average the jump into a gentler slope and under-predict how soon saturation arrives? |
| `seasonal_growth`     | Does the model separate the real trend from daily/weekly noise, or does it panic on every daily peak? |
| `healthy_plateau`     | Does the model correctly predict "no saturation," i.e. does it have a sane false-positive rate? |

This matters a lot for a portfolio piece: it's easy to make a forecast
demo that looks good on one clean upward line. Having a fixture designed
to break a lazy implementation is what makes the eventual modeling work
credible.

## Why SQLite for now, and its real limitation

SQLite was chosen because Stage 1's job is to prove the ingestion path
works, not to pick a production time-series database prematurely. It's
one file, zero setup, and `history_for_circuit()` gives Stage 2 exactly
what it needs (`SELECT ... WHERE circuit_id = ? ORDER BY timestamp`).

The real limitation to know about: SQLite requires proper POSIX file
locking, which not every filesystem provides — some network shares, some
cloud-sync folders, and some virtual/container mounts will throw
`disk I/O error` on writes. This isn't a code bug; it showed up during
development specifically when testing through a remote filesystem bridge,
and disappeared entirely once the DB lived on a normal local disk. Noted
in the README's Troubleshooting section. If this project moves to
multiple concurrent consumers or a shared/networked deployment, this is
the first component to swap (TimescaleDB or InfluxDB are the natural next
step — the `TimeSeriesStore` interface is small and deliberately easy to
reimplement against either).

## Stage 2 (forecasting) — how it actually works

`TimeSeriesStore.history_for_circuit(circuit_id)` returns
`(timestamp, sample_interval_s, utilization_pct)` triples in ascending
order (`sample_interval_s` was added alongside Stage 2 — see below).
`src/modeling/trend_fit.py` holds the pure math; `src/modeling/forecaster.py`
is the storage-reading driver that turns that into a `SaturationForecast`.

**Why weighted least squares instead of a plain fit.** A plain OLS over a
circuit's entire history is exactly the "naive model" the four archetypes
in `circuit_profiles.py` are built to break (see the README's archetype
table): it blends `step_change`'s pre-jump baseline into the post-jump
level as one gentle slope, understating how close the circuit already is
to its new level. `trend_fit.fit_weighted_linear_trend` instead fits with
exponentially recency-weighted points (`recency_weights`), so old data
barely counts. `tests/test_forecaster.py`'s step-change test asserts this
concretely: it computes what a plain unweighted fit would have predicted
on the same data (>24h out) against what the weighted fit predicts (<1h
out, correctly recognizing the circuit is already at its new plateau).

The half-life for the recency weighting is a **fraction of the observed
span** (`half_life_fraction`, default 0.25), not a fixed duration. That's
deliberate, and ties into the next point.

**Why the time axis is built from `sample_interval_s`, not from diffing
`timestamp`.** This tripped up the first version of this code, worth
recording: `TelemetrySample.timestamp` is real wall-clock time, but the
synthetic generator deliberately compresses simulated weeks into real
minutes for demos (`--sim-hours-per-tick` vs. `--tick-seconds`). Fitting
against observed timestamp deltas meant the trend math saw a demo run's
compressed real-time span (sometimes literally sub-second across hundreds
of samples), producing garbage — slopes of millions of percentage points
per day, "saturation" predicted microseconds in the future. The fix:
`forecaster._cumulative_hours` builds the time axis by summing each
sample's declared `sample_interval_s` instead of diffing `timestamp`.
Since `sample_interval_s` is *defined* as how much real time a sample's
window covers, this reflects what the telemetry claims about elapsed time
regardless of how fast it actually arrived — true whether that's a demo
compressing weeks into minutes or a production pipeline replaying backlog
after a consumer outage. `predicted_saturation_date` is still anchored to
the real last-observed `timestamp` plus that many (now correctly-scaled)
hours, so the output date is meaningful either way. Known limitation:
this assumes contiguous samples with no dropped windows; detecting gaps
would need sequence numbers this schema doesn't carry yet.

**Confidence** is the weighted fit's R² (fraction of weighted variance
explained), clipped to `[0, 1]`. It's a fit-quality signal, not a
statistical prediction interval — good enough to show `DCI-SJC-PDX-W2`
(step-change, right after its jump, mixed pre/post-jump weight) is less
certain than `DCI-ASH-DAL-W1` (clean linear ramp), without pretending to
be more rigorous than it is.

**Not implemented (left as future work, not started):** STL decomposition
or Holt-Winters via `statsmodels` (already a dependency) would separate
`seasonal_growth`'s daily/weekly cycle from its trend more precisely than
recency-weighting alone — worth revisiting if real (non-synthetic) traffic
turns out noisier than this fixture set.

## Stage 3 (augment pipeline cross-check) — how it actually works

`src/planning/augment_pipeline.py` loads `data/augment_pipeline.csv`
(`circuit_id, planned_upgrade_date, new_capacity_bps`) into
`{circuit_id: AugmentPlan}`. `src/planning/cross_check.py` compares that
against each circuit's `SaturationForecast` and produces a
`CrossCheckResult` with one of four statuses: `not_on_track`, `on_track`,
`at_risk`, or `missing_plan`.

**Four statuses, not two.** The original plan (above) was a boolean
"is predicted_date < planned_upgrade_date." Building it surfaced two more
states worth naming explicitly:

- A circuit not predicted to saturate at all (`not_on_track`) shouldn't
  care whether it has a plan — a healthy circuit with no scheduled
  upgrade isn't a problem. This check has to run *first* and short-circuit
  the rest, or a healthy circuit with no plan would wrongly look like
  `missing_plan`.
- A circuit that IS predicted to saturate but has **no plan row at all**
  (`missing_plan`) is arguably more urgent than one with a plan that's
  merely too late (`at_risk`) — nothing is even scheduled. Collapsing
  this into "at_risk" (treating "no plan" as "plan at infinity") would
  bury this distinction.

**Sign convention, since it's easy to get backwards** (a first draft did,
caught immediately by testing both `at_risk` and `on_track` in
`tests/test_cross_check.py`): `days_of_slack = planned_upgrade -
predicted_saturation`. Positive means the upgrade lands *after* the
circuit was already predicted to saturate — too late, `at_risk`. Negative
means the upgrade lands first, with that many days of margin —
`on_track`.

**Why `data/augment_pipeline.csv` isn't committed to git.** Unlike
`TelemetrySample.timestamp` (which is what it is, whenever recorded),
this file's whole purpose is to be compared against a *current* forecast
— and `SaturationForecast.predicted_saturation_date` is always relative
to "now" and however much trend has accumulated in storage. A committed
CSV would show a fixed calendar date that looks increasingly wrong (or
just confusing) the more time passes since it was generated. Instead,
`scripts/generate_demo_augment_plan.py` derives the demo fleet's planned
dates from whatever's *currently* forecast — offsetting each one
deliberately earlier or later — so the same four statuses show up
correctly no matter when or how long you've run the generator. A real
deployment's augment-pipeline CSV, sourced from an actual procurement
system rather than derived from live forecasts, would reasonably be
committed or otherwise version-controlled — this "don't commit" reasoning
is specific to the demo/synthetic data path.

## Stage 4 (alerting / dashboard) — how it actually works

`src/alerting/pipeline.py` is the driver: it calls
`cross_check.compute_cross_checks()` (the same Stage 2+3 orchestration
Stage 3's own CLI uses -- extracted specifically so this stage wouldn't
duplicate that loop), renders a dashboard, and dispatches one alert per
flagged circuit. `CrossCheckResult.status in ("at_risk", "missing_plan")`
turned out to be exactly the predicate this needed, unchanged from the
plan.

**Notifier is a small Protocol, not a class hierarchy** (`src/alerting/
notifier.py`): `LogNotifier` (default, just prints) and
`SlackWebhookNotifier` (posts to a Slack incoming webhook) both satisfy
`Notifier.notify(message: str) -> None`. Same shape as `producer.py`'s
real-backend-plus-dry-run-stand-in pattern, for the same reason: tests and
local dev shouldn't need a live webhook to exercise this code path.
`SlackWebhookNotifier` catches its own delivery failures
(`URLError`/`TimeoutError`/`OSError`) and falls back to printing --
alerting is the last step of the pipeline, not the point of it, and a
flaky webhook should never crash a forecast run.

**Dashboard is static HTML, not a running app** (`src/alerting/
dashboard.py`): one self-contained file, no server, no JS framework,
matching this project's recurring "boring on purpose" bias (see
storage.py's SQLite choice, augment_pipeline.py's CSV choice). Circuit IDs
and every other rendered field go through `html.escape` -- in production
these ultimately trace back to router/collector-supplied data, so treating
them as untrusted before they land in an HTML page costs nothing and
closes an XSS path if that assumption is ever wrong. Like
`augment_pipeline.csv`, the rendered dashboard isn't committed to git (see
`.gitignore`) -- it's a snapshot of "now," and a stale one lying around
the repo would be actively misleading.

**Not implemented:** email and PagerDuty backends (the module docstring's
original brainstorm) — Slack covers the webhook-shaped case well enough
that adding more backends before there's a real one in use would be
speculative. The `Notifier` protocol is the extension point if that
changes.
