"""Open-Meteo archive client with retries, exponential backoff, jitter and Retry-After support.

HTTP-level retries handle short blips (429, 5xx, timeouts) inside one task attempt. Longer outages
fail the task and are retried by Airflow (task ``retries`` with exponential backoff), so the two
layers never multiply into a retry storm: at most ``max_attempts`` calls per task try.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import date

import requests

from .config import City, SourceSettings

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceError(RuntimeError):
    """The API could not be reached or kept failing after all attempts (Airflow will retry the task)."""


class BadRequest(RuntimeError):
    """A non-retryable 4xx: retrying will not help, so fail fast."""


@dataclass
class Attempt:
    attempt: int
    status: int | None
    error: str | None
    wait_s: float
    elapsed_ms: float


@dataclass
class FetchResult:
    payload: dict
    attempts: list[Attempt] = field(default_factory=list)


class OpenMeteoClient:
    def __init__(self, settings: SourceSettings, session: requests.Session | None = None, sleep=time.sleep,
                 rng: random.Random | None = None):
        self.s = settings
        self.session = session or requests.Session()
        self.sleep = sleep
        self.rng = rng or random.Random()

    def params(self, city: City, day: date) -> dict:
        return {"latitude": city.lat, "longitude": city.lon, "start_date": day.isoformat(),
                "end_date": day.isoformat(), "hourly": ",".join(self.s.hourly), "timezone": "UTC"}

    def backoff(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), self.s.backoff_max_s)
            except ValueError:
                pass
        base = self.s.backoff_base_s * 2 ** (attempt - 1)
        return min(base + self.rng.uniform(0, self.s.backoff_base_s), self.s.backoff_max_s)

    def fetch_day(self, city: City, day: date) -> FetchResult:
        attempts: list[Attempt] = []
        for n in range(1, self.s.max_attempts + 1):
            start = time.perf_counter()
            status, error, retry_after = None, None, None
            try:
                resp = self.session.get(self.s.base_url, params=self.params(city, day), timeout=self.s.timeout_s)
                status = resp.status_code
                if status == 200:
                    attempts.append(Attempt(n, status, None, 0.0, (time.perf_counter() - start) * 1000))
                    return FetchResult(resp.json(), attempts)
                if status not in RETRYABLE_STATUS:
                    raise BadRequest(f"{city.id} {day}: HTTP {status}: {resp.text[:200]}")
                retry_after = resp.headers.get("Retry-After")
                error = f"HTTP {status}"
            except (requests.Timeout, requests.ConnectionError) as exc:
                error = type(exc).__name__
            wait = self.backoff(n, retry_after) if n < self.s.max_attempts else 0.0
            attempts.append(Attempt(n, status, error, round(wait, 3), (time.perf_counter() - start) * 1000))
            if wait:
                self.sleep(wait)
        raise SourceError(f"{city.id} {day}: gave up after {self.s.max_attempts} attempts ({attempts[-1].error})")
