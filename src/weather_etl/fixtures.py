"""Deterministic stand-in for the Open-Meteo archive API (synthetic data, same JSON shape).

Used by `make demo`, the tests and the CI Airflow run, so results are reproducible without network
access. It also replays the failures a real run meets:

* transient ``503`` and ``429 Retry-After`` responses and a read timeout (HTTP-level retries)
* a day that is still incomplete in the archive on the first task try (task-level retry)
* a sensor glitch (impossible temperature) and a few missing values (QC flags, not failures)
"""
from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta, timezone

import requests

CLIMATE = {  # rough October baselines: (mean °C, diurnal amplitude, humidity %, rain chance per hour)
    "chandigarh": (24.0, 7.0, 55, 0.010), "ludhiana": (24.5, 7.5, 52, 0.008), "jaipur": (27.0, 8.0, 40, 0.004),
    "delhi": (26.5, 6.5, 50, 0.006), "mumbai": (29.0, 3.0, 75, 0.040), "bengaluru": (23.5, 5.0, 70, 0.050),
}

# (city, day index from start, task try) → list of HTTP outcomes before success
HTTP_FAULTS = {
    ("delhi", 1): ["503", "503"],
    ("mumbai", 3): ["429"],
    ("jaipur", 4): ["timeout"],
    ("bengaluru", 5): ["502"],
}
INCOMPLETE_FIRST_TRY = {("ludhiana", 6)}            # archive lag: only 19 hours on the first task try
GLITCH = {("chandigarh", 2): 13}                     # hour with a 999 °C reading
NULLS = {("mumbai", 2): [4, 5]}                       # missing hours for wind


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = "" if payload is None else "{...}"

    def json(self) -> dict:
        return self._payload


def series(city_id: str, day: date, seed: int = 7) -> dict:
    mean, amp, hum, rain_p = CLIMATE.get(city_id, (25.0, 6.0, 55, 0.01))
    rng = random.Random(f"{seed}-{city_id}-{day.isoformat()}")
    drift = (day.toordinal() % 30) / 30 * -2.5            # October cooling
    t0 = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    out = {"time": [], "temperature_2m": [], "relative_humidity_2m": [], "precipitation": [], "wind_speed_10m": []}
    raining = 0
    for h in range(24):
        local_h = (h + 5.5) % 24                           # IST
        diurnal = math.sin((local_h - 9) / 24 * 2 * math.pi)
        if raining == 0 and rng.random() < rain_p:
            raining = rng.randint(1, 4)
        rain = round(rng.uniform(0.2, 6.0), 1) if raining else 0.0
        raining = max(0, raining - 1)
        temp = mean + drift + amp * diurnal + rng.gauss(0, 0.6) - (2.0 if rain else 0)
        out["time"].append((t0 + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M"))
        out["temperature_2m"].append(round(temp, 1))
        rh = hum - 15 * diurnal + rng.gauss(0, 4) + 20 * bool(rain)
        out["relative_humidity_2m"].append(int(max(5, min(100, rh))))
        out["precipitation"].append(rain)
        out["wind_speed_10m"].append(round(max(0.0, 8 + 6 * diurnal + rng.gauss(0, 2)), 1))
    return out


class FixtureSession:
    """Quacks like ``requests.Session`` for ``OpenMeteoClient``. ``task_try`` is set by the runner."""

    def __init__(self, start: date, hourly: tuple[str, ...] | None = None):
        self.start = start
        self.task_try = 1
        self.calls: dict[tuple[str, str], int] = {}
        self.city_by_coords: dict[tuple[float, float], str] = {}
        self.hourly = hourly

    def register(self, cities) -> FixtureSession:
        self.city_by_coords = {(round(c.lat, 4), round(c.lon, 4)): c.id for c in cities}
        return self

    def get(self, url: str, params: dict, timeout: float):
        city = self.city_by_coords[(round(float(params["latitude"]), 4), round(float(params["longitude"]), 4))]
        day = date.fromisoformat(params["start_date"])
        idx = (day - self.start).days
        key = (city, day.isoformat())
        n = self.calls[key] = self.calls.get(key, 0) + 1
        faults = HTTP_FAULTS.get((city, idx), []) if self.task_try == 1 else []
        if n <= len(faults):
            f = faults[n - 1]
            if f == "timeout":
                raise requests.ReadTimeout("read timed out (fixture)")
            if f == "429":
                return FakeResponse(429, headers={"Retry-After": "2"})
            return FakeResponse(int(f))
        hourly = series(city, day)
        if (city, idx) in INCOMPLETE_FIRST_TRY and self.task_try == 1:
            hourly = {k: v[:19] for k, v in hourly.items()}
        if (city, idx) in GLITCH:
            hourly["temperature_2m"][GLITCH[(city, idx)]] = 999.0
        for h in NULLS.get((city, idx), []):
            hourly["wind_speed_10m"][h] = None
        payload = {"latitude": params["latitude"], "longitude": params["longitude"], "timezone": "UTC",
                   "hourly_units": {"temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm",
                                    "wind_speed_10m": "km/h"},
                   "hourly": hourly}
        return FakeResponse(200, payload)
