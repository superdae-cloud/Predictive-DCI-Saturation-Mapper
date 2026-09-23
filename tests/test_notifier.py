"""
Tests for Stage 4's notifier abstraction. SlackWebhookNotifier never makes
a real network call here -- urlopen is monkeypatched, since a live webhook
isn't available (or wanted) in a unit test.
"""

import urllib.error

from src.alerting.notifier import LogNotifier, SlackWebhookNotifier, build_notifier


def test_log_notifier_prints_the_message(capsys):
    LogNotifier().notify("something happened")

    captured = capsys.readouterr()
    assert "something happened" in captured.out


def test_slack_webhook_notifier_posts_json_payload(monkeypatch):
    captured_requests = []

    def fake_urlopen(request, timeout):
        captured_requests.append((request, timeout))

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    notifier = SlackWebhookNotifier("https://hooks.slack.test/abc")
    notifier.notify("circuit DCI-X is at risk")

    assert len(captured_requests) == 1
    request, timeout = captured_requests[0]
    assert request.full_url == "https://hooks.slack.test/abc"
    assert b"circuit DCI-X is at risk" in request.data


def test_slack_webhook_notifier_falls_back_to_print_on_failure(monkeypatch, capsys):
    def failing_urlopen(request, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", failing_urlopen)

    notifier = SlackWebhookNotifier("https://hooks.slack.test/abc")
    notifier.notify("circuit DCI-X is at risk")  # must not raise

    captured = capsys.readouterr()
    assert "circuit DCI-X is at risk" in captured.out


def test_build_notifier_picks_slack_when_url_given():
    notifier = build_notifier("https://hooks.slack.test/abc")
    assert isinstance(notifier, SlackWebhookNotifier)


def test_build_notifier_defaults_to_log_notifier():
    notifier = build_notifier(None)
    assert isinstance(notifier, LogNotifier)
