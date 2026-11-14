import pytest

from weather_etl.fixtures import series
from weather_etl.quality import DataIncomplete, DataQualityError, check_day
from weather_etl.transform import PayloadError, to_rows

M = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m")


def payload(hourly):
    return {"hourly": hourly}


def test_rows_are_utc_hours_inside_the_interval(settings, day):
    h = series("jaipur", day(0))
    h["time"].append("2026-10-02T00:00")                 # next day's first hour sneaks in
    for m in M:
        h[m].append(1.0)
    rows = to_rows(payload(h), "jaipur", day(0), M, settings.quality)
    assert len(rows) == 24
    assert rows[0]["observed_at"] == "2026-10-01T00:00:00Z" and rows[-1]["observed_at"] == "2026-10-01T23:00:00Z"


def test_out_of_range_values_are_nulled_and_flagged(settings, day):
    h = series("jaipur", day(0))
    h["temperature_2m"][5] = 999.0
    h["relative_humidity_2m"][6] = -3
    rows = to_rows(payload(h), "jaipur", day(0), M, settings.quality)
    assert rows[5]["temperature_2m"] is None and "temperature_2m:out_of_range(999.0)" in rows[5]["qc_flags"]
    assert rows[6]["relative_humidity_2m"] is None
    assert sum(1 for r in rows if r["qc_flags"]) == 2


def test_duplicate_timestamps_keep_the_last_value(settings, day):
    h = series("jaipur", day(0))
    h["time"].append(h["time"][3])
    for m in M:
        h[m].append(h[m][3])
    h["temperature_2m"][-1] = 11.1
    rows = to_rows(payload(h), "jaipur", day(0), M, settings.quality)
    assert len(rows) == 24 and rows[3]["temperature_2m"] == 11.1


@pytest.mark.parametrize("bad, msg", [({}, "no hourly.time"),
                                      ({"time": ["2026-10-01T00:00"]}, "missing hourly.temperature_2m")])
def test_contract_breaks_raise(settings, day, bad, msg):
    with pytest.raises(PayloadError, match=msg):
        to_rows(payload(bad), "x", day(0), M, settings.quality)


def test_mismatched_lengths_raise(settings, day):
    h = series("jaipur", day(0))
    h["precipitation"] = h["precipitation"][:-1]
    with pytest.raises(PayloadError, match="23 values for 24"):
        to_rows(payload(h), "jaipur", day(0), M, settings.quality)


def test_quality_checks(settings, day):
    rows = to_rows(payload(series("delhi", day(0))), "delhi", day(0), M, settings.quality)
    results = check_day(rows, M, settings.quality)
    assert {r["status"] for r in results} == {"pass"}
    with pytest.raises(DataIncomplete) as e:
        check_day(rows[:21], M, settings.quality)
    assert e.value.results[0] == {"check": "hours_present", "value": 21, "threshold": 22, "status": "fail"}
    for r in rows[:3]:
        r["wind_speed_10m"] = None                       # 3/24 = 12.5% > 10%
    with pytest.raises(DataQualityError, match="null_share:wind_speed_10m"):
        check_day(rows, M, settings.quality)
