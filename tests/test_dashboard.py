"""Tests for the Stage 4 static HTML dashboard renderer."""

from src.alerting.dashboard import render_dashboard
from src.common.models import CrossCheckResult, SaturationForecast

FORECAST_KWARGS = dict(
    generated_at="2026-01-01T00:00:00+00:00",
    threshold_pct=90.0,
    trend_pct_per_day=1.0,
    confidence=0.9,
    sample_count=100,
)


def _row(circuit_id, current_pct, status, predicted=None, planned=None, slack=None):
    forecast = SaturationForecast(
        circuit_id=circuit_id,
        current_utilization_pct=current_pct,
        predicted_saturation_date=predicted,
        days_to_saturation=None,
        **FORECAST_KWARGS,
    )
    result = CrossCheckResult(
        circuit_id=circuit_id,
        status=status,
        predicted_saturation_date=predicted,
        planned_upgrade_date=planned,
        days_of_slack=slack,
    )
    return forecast, result


def test_dashboard_includes_every_circuit_and_its_status_label():
    rows = [
        _row("DCI-A", 95.0, "at_risk", predicted="2026-02-01T00:00:00+00:00", planned="2026-03-01T00:00:00+00:00", slack=28.0),
        _row("DCI-B", 30.0, "not_on_track"),
    ]

    html = render_dashboard(rows)

    assert "DCI-A" in html and "DCI-B" in html
    assert "AT RISK" in html
    assert "HEALTHY" in html


def test_dashboard_summary_counts_only_flagged_statuses():
    rows = [
        _row("DCI-A", 95.0, "at_risk", predicted="2026-02-01T00:00:00+00:00", planned="2026-03-01T00:00:00+00:00", slack=28.0),
        _row("DCI-B", 40.0, "missing_plan", predicted="2026-02-15T00:00:00+00:00"),
        _row("DCI-C", 20.0, "not_on_track"),
        _row("DCI-D", 60.0, "on_track", predicted="2026-04-01T00:00:00+00:00", planned="2026-03-01T00:00:00+00:00", slack=-31.0),
    ]

    html = render_dashboard(rows)

    assert "2 of 4 circuit(s) need attention" in html


def test_dashboard_escapes_untrusted_circuit_id_content():
    rows = [_row("<script>alert(1)</script>", 10.0, "not_on_track")]

    html = render_dashboard(rows)

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
