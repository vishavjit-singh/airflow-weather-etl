"""Open-Meteo hourly payload → validated rows (one per city and UTC hour)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from .config import QualitySettings


class PayloadError(ValueError):
    """The response does not have the expected structure (a contract break, not bad data)."""


def to_rows(payload: dict, city_id: str, day: date, measures: tuple[str, ...], quality: QualitySettings) -> list[dict]:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or "time" not in hourly:
        raise PayloadError("payload has no hourly.time")
    times = hourly["time"]
    for m in measures:
        if m not in hourly:
            raise PayloadError(f"payload is missing hourly.{m}")
        if len(hourly[m]) != len(times):
            raise PayloadError(f"hourly.{m} has {len(hourly[m])} values for {len(times)} timestamps")
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    rows: dict[str, dict] = {}
    for i, t in enumerate(times):
        ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        if not start <= ts < end:
            continue                                     # outside the data interval: never load it
        row = {"city_id": city_id, "observed_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}
        flags = []
        for m in measures:
            v = hourly[m][i]
            lo, hi = quality.ranges[m]
            if v is not None and not lo <= float(v) <= hi:
                flags.append(f"{m}:out_of_range({v})")
                v = None                                 # keep the hour, drop the impossible value
            row[m] = None if v is None else float(v)
        row["qc_flags"] = ";".join(flags) or None
        rows[row["observed_at"]] = row                   # duplicate timestamps: last one wins
    return [rows[k] for k in sorted(rows)]
