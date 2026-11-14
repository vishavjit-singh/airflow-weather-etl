import random

import pytest
import requests

from weather_etl.client import BadRequest, OpenMeteoClient, SourceError
from weather_etl.fixtures import FakeResponse


class ScriptedSession:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        o = self.outcomes.pop(0)
        if isinstance(o, Exception):
            raise o
        return o


def client(settings, *outcomes):
    s = ScriptedSession(*outcomes)
    sleeps = []
    return OpenMeteoClient(settings.source, session=s, sleep=sleeps.append, rng=random.Random(1)), s, sleeps


def test_params(settings, day):
    c, _, _ = client(settings)
    p = c.params(settings.city("jaipur"), day(0))
    assert p["start_date"] == p["end_date"] == "2026-10-01"
    assert p["hourly"] == "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
    assert p["timezone"] == "UTC"


def test_retries_transient_errors_with_exponential_backoff(settings, day):
    c, s, sleeps = client(settings, FakeResponse(503), requests.ReadTimeout(), FakeResponse(502),
                          FakeResponse(200, {"hourly": {}}))
    out = c.fetch_day(settings.city("delhi"), day(0))
    assert [a.status for a in out.attempts] == [503, None, 502, 200]
    assert len(sleeps) == 3
    for n, wait in enumerate(sleeps, start=1):          # base * 2^(n-1) + jitter in [0, base)
        assert 2 ** (n - 1) <= wait < 2 ** (n - 1) + 1


def test_retry_after_is_respected_and_capped(settings, day):
    c, _, sleeps = client(settings, FakeResponse(429, headers={"Retry-After": "7"}),
                          FakeResponse(429, headers={"Retry-After": "600"}), FakeResponse(200, {}))
    c.fetch_day(settings.city("delhi"), day(0))
    assert sleeps == [7.0, settings.source.backoff_max_s]


def test_gives_up_after_max_attempts(settings, day):
    c, s, sleeps = client(settings, *[FakeResponse(503)] * settings.source.max_attempts)
    with pytest.raises(SourceError, match="gave up after 5 attempts"):
        c.fetch_day(settings.city("delhi"), day(0))
    assert len(s.calls) == 5 and len(sleeps) == 4          # no pointless sleep after the last attempt


def test_client_errors_fail_fast(settings, day):
    c, s, sleeps = client(settings, FakeResponse(400, {"reason": "bad"}))
    with pytest.raises(BadRequest):
        c.fetch_day(settings.city("delhi"), day(0))
    assert len(s.calls) == 1 and sleeps == []


def test_fixture_replays_planned_faults_only_on_first_try(fixture_client, settings, day):
    first = fixture_client(1).fetch_day(settings.city("delhi"), day(1))
    assert [a.status for a in first.attempts] == [503, 503, 200]
    second = fixture_client(2).fetch_day(settings.city("delhi"), day(1))
    assert [a.status for a in second.attempts] == [200]
