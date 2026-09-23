"""
Tests for Stage 4's alert dispatch against real (but synthetic)
cross-check rows, built the same way test_cross_check.py does -- direct
SaturationForecast/CrossCheckResult construction, no live storage needed.
"""

from src.alerting.pipeline import dispatch_alerts
from src.common.models import CrossCheckResult, SaturationForecast

FORECAST_KWARGS = dict(
    generated_at="2026-01-01T00:00:00+00:00",
    threshold_pct=90.0,
    current_utilization_pct=80.0,
    trend_pct_per_day=1.0,
    confidence=0.9,
    sample_count=100,
)


class RecordingNotifier:
    def __init__(self):
        self.messages: list[str] = []

    def notify(self, message: str) -> None:
        self.messages.append(message)


def _row(circuit_id, status, predicted=None, planned=None, slack=None):
    forecast = SaturationForecast(
        circuit_id=circuit_id,
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


def test_dispatch_alerts_only_notifies_flagged_circuits():
    rows = [
        _row("DCI-HEALTHY", "not_on_track"),
        _row("DCI-ON-TRACK", "on_track", predicted="2026-04-01T00:00:00+00:00", planned="2026-03-01T00:00:00+00:00", slack=-31.0),
        _row("DCI-AT-RISK", "at_risk", predicted="2026-02-01T00:00:00+00:00", planned="2026-03-01T00:00:00+00:00", slack=28.0),
        _row("DCI-MISSING", "missing_plan", predicted="2026-02-15T00:00:00+00:00"),
    ]
    notifier = RecordingNotifier()

    sent = dispatch_alerts(rows, notifier)

    assert sent == 2
    assert any("DCI-AT-RISK" in m for m in notifier.messages)
    assert any("DCI-MISSING" in m for m in notifier.messages)
    assert not any("DCI-HEALTHY" in m for m in notifier.messages)
    assert not any("DCI-ON-TRACK" in m for m in notifier.messages)


def test_dispatch_alerts_returns_zero_when_nothing_flagged():
    rows = [_row("DCI-HEALTHY", "not_on_track")]

    assert dispatch_alerts(rows, RecordingNotifier()) == 0
