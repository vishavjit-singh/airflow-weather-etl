"""Typed view of config/pipeline.yaml."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class City:
    id: str
    name: str
    lat: float
    lon: float
    timezone: str


@dataclass(frozen=True)
class SourceSettings:
    base_url: str
    hourly: tuple[str, ...]
    timeout_s: float
    max_attempts: int
    backoff_base_s: float
    backoff_max_s: float


@dataclass(frozen=True)
class QualitySettings:
    expected_hours: int
    min_hours: int
    max_null_share: float
    ranges: dict[str, tuple[float, float]]


@dataclass(frozen=True)
class Settings:
    source: SourceSettings
    cities: tuple[City, ...]
    quality: QualitySettings
    start_date: date
    sla_minutes: int

    def city(self, city_id: str) -> City:
        return next(c for c in self.cities if c.id == city_id)


def load_settings(path: str | Path | None = None) -> Settings:
    raw = yaml.safe_load(Path(path or DEFAULT_CONFIG).read_text())
    s, q = raw["source"], raw["quality"]
    cities = tuple(City(**c) for c in raw["cities"])
    if len({c.id for c in cities}) != len(cities):
        raise ValueError("city ids must be unique")
    missing = set(s["hourly"]) - set(q["ranges"])
    if missing:
        raise ValueError(f"no range configured for {sorted(missing)}")
    return Settings(
        source=SourceSettings(base_url=s["base_url"], hourly=tuple(s["hourly"]), timeout_s=float(s["timeout_s"]),
                              max_attempts=int(s["max_attempts"]), backoff_base_s=float(s["backoff_base_s"]),
                              backoff_max_s=float(s["backoff_max_s"])),
        cities=cities,
        quality=QualitySettings(expected_hours=int(q["expected_hours"]), min_hours=int(q["min_hours"]),
                                max_null_share=float(q["max_null_share"]),
                                ranges={k: (float(v[0]), float(v[1])) for k, v in q["ranges"].items()}),
        start_date=date.fromisoformat(str(raw["schedule"]["start_date"])),
        sla_minutes=int(raw["schedule"]["sla_minutes"]),
    )
