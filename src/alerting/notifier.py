"""
Pluggable alert delivery. Same shape as the generator's producer.py: one
real backend plus a dependency-free stand-in, both behind a single
interface, so tests and local dev never need a live webhook.

Notifying is the last step of the pipeline, not the point of it -- a
flaky webhook should never crash a forecast run. SlackWebhookNotifier
catches its own delivery failures and falls back to logging rather than
propagating.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Protocol


class Notifier(Protocol):
    def notify(self, message: str) -> None: ...


class LogNotifier:
    """Always available; the default. Prints, no network calls."""

    def notify(self, message: str) -> None:
        print(f"[ALERT] {message}")


class SlackWebhookNotifier:
    """Posts to a Slack incoming webhook (https://api.slack.com/messaging/webhooks)."""

    def __init__(self, webhook_url: str, timeout_s: float = 5.0):
        self.webhook_url = webhook_url
        self.timeout_s = timeout_s

    def notify(self, message: str) -> None:
        payload = json.dumps({"text": message}).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(request, timeout=self.timeout_s)
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            print(f"[ALERT - webhook delivery failed ({e})] {message}")


def build_notifier(slack_webhook_url: str | None) -> Notifier:
    if slack_webhook_url:
        return SlackWebhookNotifier(slack_webhook_url)
    return LogNotifier()
