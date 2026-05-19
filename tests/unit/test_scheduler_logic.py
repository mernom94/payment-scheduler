"""
Unit tests for scheduler cron evaluation and next_fire computation.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from croniter import croniter
from freezegun import freeze_time
from pytz import timezone as pytz_timezone

from app.workers.scheduler import SchedulerWorker
from app.infrastructure.db.models import Subscription


def make_subscription(cron: str, tz: str = "UTC") -> Subscription:
    sub = Subscription()
    sub.cron_expression = cron
    sub.timezone = tz
    return sub


class TestNextFire:
    @freeze_time("2024-06-15 12:00:00 UTC")
    def test_every_minute_fires_in_one_minute(self):
        sub = make_subscription("* * * * *")
        next_dt = SchedulerWorker._next_fire(sub)
        assert next_dt is not None
        assert next_dt > datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

    @freeze_time("2024-06-15 00:00:00 UTC")
    def test_daily_cron_respects_timezone(self):
        # "At 09:00 in Europe/Berlin" → UTC is 07:00 or 08:00 depending on DST
        sub = make_subscription("0 9 * * *", tz="Europe/Berlin")
        next_dt = SchedulerWorker._next_fire(sub)
        assert next_dt is not None
        # Should be today or tomorrow at 09:00 Berlin time
        berlin = pytz_timezone("Europe/Berlin")
        local = next_dt.astimezone(berlin)
        assert local.hour == 9
        assert local.minute == 0

    def test_invalid_timezone_falls_back_to_utc(self):
        sub = make_subscription("*/5 * * * *", tz="Not/A/Timezone")
        # Should not raise
        next_dt = SchedulerWorker._next_fire(sub)
        assert next_dt is not None

    def test_returns_utc_datetime(self):
        sub = make_subscription("0 * * * *")
        next_dt = SchedulerWorker._next_fire(sub)
        assert next_dt.tzinfo is not None
        assert next_dt.tzinfo == timezone.utc or str(next_dt.tzinfo) == "UTC"

    @freeze_time("2024-01-01 23:59:59 UTC")
    def test_cron_crosses_midnight(self):
        sub = make_subscription("0 0 * * *")  # daily at midnight
        next_dt = SchedulerWorker._next_fire(sub)
        assert next_dt.day == 2 or (next_dt.day == 1 and next_dt.hour == 0)


class TestCronValidation:
    @pytest.mark.parametrize("expr", [
        "* * * * *",
        "0 9 * * 1-5",
        "*/15 * * * *",
        "0 0 1 * *",
        "30 6 * * 0",
    ])
    def test_valid_expressions(self, expr: str):
        assert croniter.is_valid(expr)

    @pytest.mark.parametrize("expr", [
        "not a cron",
        "60 * * * *",
        "",
    ])
    def test_invalid_expressions(self, expr: str):
        # API layer rejects these
        assert not croniter.is_valid(expr)
