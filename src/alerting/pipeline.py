"""
Stage 4: runs Stages 1-3's cross-check for every circuit, writes a static
dashboard, and notifies on anything flagged. This is the "do something
louder than printing to stdout" step docs/architecture.md called for --
CrossCheckResult.status in ("at_risk", "missing_plan") was already the
exact predicate an alert should fire on; this module is mostly wiring.

Usage:
    python -m src.alerting.pipeline --db-path data/telemetry.sqlite \
        --augment-plan-path data/augment_pipeline.csv \
        --dashboard-path data/dashboard.html
        [--slack-webhook-url https://hooks.slack.com/services/...]

Run on a schedule (cron, a CI job, etc.) against a live deployment's
storage, this is the whole alerting loop.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.alerting.dashboard import FLAGGED_STATUSES, render_dashboard
from src.alerting.notifier import Notifier, build_notifier
from src.planning.cross_check import DEFAULT_THRESHOLD_PCT, compute_cross_checks


def alert_message(circuit_id: str, status: str, detail: str) -> str:
    label = "AT RISK" if status == "at_risk" else "MISSING PLAN"
    return f"[{label}] {circuit_id}: {detail}"


def dispatch_alerts(rows, notifier: Notifier) -> int:
    """Sends one notification per flagged circuit. Returns the count sent."""
    sent = 0
    for forecast, result in rows:
        if result.status not in FLAGGED_STATUSES:
            continue
        if result.status == "at_risk":
            detail = (
                f"predicted saturation {result.predicted_saturation_date} is "
                f"{abs(result.days_of_slack):.1f} days before planned upgrade "
                f"{result.planned_upgrade_date}"
            )
        else:
            detail = f"predicted saturation {result.predicted_saturation_date} but no augment plan exists"
        notifier.notify(alert_message(forecast.circuit_id, result.status, detail))
        sent += 1
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-path", default="data/telemetry.sqlite")
    parser.add_argument("--augment-plan-path", default="data/augment_pipeline.csv")
    parser.add_argument("--dashboard-path", default="data/dashboard.html")
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--slack-webhook-url", default=None, help="Post alerts to a Slack incoming webhook instead of just logging them")
    args = parser.parse_args()

    rows = compute_cross_checks(args.db_path, args.augment_plan_path, args.threshold_pct)
    if not rows:
        print(f"No telemetry found in {args.db_path} -- run the generator/consumer first.")
        return

    dashboard_html = render_dashboard(rows)
    Path(args.dashboard_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.dashboard_path).write_text(dashboard_html)
    print(f"Wrote dashboard to {args.dashboard_path}")

    notifier = build_notifier(args.slack_webhook_url)
    sent = dispatch_alerts(rows, notifier)
    print(f"Sent {sent} alert(s)" if sent else "No circuits need attention -- no alerts sent.")


if __name__ == "__main__":
    main()
