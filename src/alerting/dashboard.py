"""
Renders a static HTML snapshot of every circuit's forecast + cross-check
status. Deliberately not a running web app: no server, no JS framework,
one self-contained file you can open directly or attach to a status
email -- the same "boring on purpose" bias as storage.py's choice of
SQLite over a real time-series database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape

from src.common.models import CrossCheckResult, SaturationForecast

# (display label, text color) per CrossCheckResult.status
STATUS_STYLE = {
    "at_risk": ("AT RISK", "#b91c1c"),
    "missing_plan": ("MISSING PLAN", "#b45309"),
    "on_track": ("ON TRACK", "#15803d"),
    "not_on_track": ("HEALTHY", "#15803d"),
}

FLAGGED_STATUSES = ("at_risk", "missing_plan")


def render_dashboard(rows: list[tuple[SaturationForecast, CrossCheckResult]]) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    flagged_count = sum(1 for _, result in rows if result.status in FLAGGED_STATUSES)
    body_rows = "\n".join(_row_html(forecast, result) for forecast, result in rows)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>DCI Saturation Dashboard</title>
<style>
  body {{ font-family: -apple-system, "Segoe UI", sans-serif; margin: 2rem; color: #1f2937; }}
  h1 {{ margin-bottom: 0.25rem; }}
  .meta {{ color: #6b7280; margin-bottom: 1.5rem; font-size: 0.9rem; }}
  .summary {{ margin-bottom: 1rem; font-weight: 600; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #e5e7eb; }}
  th {{ background: #f9fafb; font-size: 0.85rem; text-transform: uppercase; color: #6b7280; }}
  .status {{ font-weight: 700; }}
</style>
</head>
<body>
<h1>DCI Saturation Dashboard</h1>
<div class="meta">Generated {escape(generated_at)}</div>
<div class="summary">{flagged_count} of {len(rows)} circuit(s) need attention</div>
<table>
<tr>
  <th>Circuit</th><th>Current</th><th>Trend (pp/day)</th><th>Predicted saturation</th>
  <th>Planned upgrade</th><th>Confidence</th><th>Status</th>
</tr>
{body_rows}
</table>
</body>
</html>
"""


def _row_html(forecast: SaturationForecast, result: CrossCheckResult) -> str:
    label, color = STATUS_STYLE.get(result.status, (result.status, "#374151"))
    predicted = forecast.predicted_saturation_date or "–"
    planned = result.planned_upgrade_date or "–"
    return (
        "<tr>"
        f"<td>{escape(forecast.circuit_id)}</td>"
        f"<td>{forecast.current_utilization_pct:.2f}%</td>"
        f"<td>{forecast.trend_pct_per_day:+.3f}</td>"
        f"<td>{escape(predicted)}</td>"
        f"<td>{escape(planned)}</td>"
        f"<td>{forecast.confidence:.2f}</td>"
        f'<td class="status" style="color: {color}">{escape(label)}</td>'
        "</tr>"
    )
