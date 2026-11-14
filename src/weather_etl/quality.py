"""Data-quality checks for one city-day. Failures raise so the Airflow task fails and is retried."""
from __future__ import annotations

from .config import QualitySettings


class DataIncomplete(RuntimeError):
    """Too few hours: the archive has not caught up yet. Retrying later usually fixes it."""


class DataQualityError(RuntimeError):
    """Too many missing values for a measure: the day should not be published as-is."""


def check_day(rows: list[dict], measures: tuple[str, ...], q: QualitySettings) -> list[dict]:
    """Return check results; raise when a blocking check fails (results are attached to the exception)."""
    results = [{"check": "hours_present", "value": len(rows), "threshold": q.min_hours,
                "status": "pass" if len(rows) >= q.min_hours else "fail"}]
    n = max(len(rows), 1)
    for m in measures:
        share = sum(1 for r in rows if r[m] is None) / n
        results.append({"check": f"null_share:{m}", "value": round(share, 4), "threshold": q.max_null_share,
                        "status": "pass" if share <= q.max_null_share else "fail"})
    flagged = sum(1 for r in rows if r.get("qc_flags"))
    results.append({"check": "qc_flagged_hours", "value": flagged, "threshold": None,
                    "status": "warn" if flagged else "pass"})
    if results[0]["status"] == "fail":
        err = DataIncomplete(f"{len(rows)} of {q.expected_hours} hours present (minimum {q.min_hours})")
        err.results = results
        raise err
    bad = [r["check"] for r in results if r["status"] == "fail"]
    if bad:
        err = DataQualityError(f"failed checks: {', '.join(bad)}")
        err.results = results
        raise err
    return results
