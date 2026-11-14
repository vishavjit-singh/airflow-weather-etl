"""README images and summary numbers, computed from the loaded tables and the run logs."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Patch  # noqa: E402

from .config import Settings  # noqa: E402
from .db import Database  # noqa: E402

INK, MUTED, GRID = "#0e1a2b", "#5b6676", "#e3e7ee"
ACCENT, SIGNAL, BAD, GOOD, PURPLE = "#2a78d6", "#eda100", "#e34948", "#1baf7a", "#6250d6"
CITY_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#6250d6", "#eda100", "#d6458f"]

plt.rcParams.update({
    "font.family": "DejaVu Sans", "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.titleweight": "bold", "axes.titlesize": 13, "axes.titlecolor": INK,
    "axes.titlelocation": "left", "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 150,
})


def _attempts(path: Path) -> list[dict]:
    return [json.loads(x) for x in Path(path).read_text().splitlines() if x.strip()] if Path(path).exists() else []


def _save(fig, out: Path, name: str) -> Path:
    fig.tight_layout()
    p = out / name
    fig.savefig(p, facecolor="white")
    plt.close(fig)
    return p


def summary(db: Database, attempts_file: Path) -> dict:
    att = _attempts(attempts_file)
    runs = db.query("SELECT * FROM etl_task_runs")
    ti = {(r["city_id"], r["day"]) for r in runs}
    retried = {(r["city_id"], r["day"]) for r in runs if r["try_number"] > 1}
    flagged = db.query("SELECT city_id, observed_at, qc_flags FROM hourly_observations WHERE qc_flags IS NOT NULL")
    return {
        "days": len({r["day"] for r in runs}),
        "cities": len({r["city_id"] for r in runs}),
        "mapped_task_instances": len(ti),
        "task_instances_needing_a_retry": len(retried),
        "task_failures_by_reason": dict(Counter(r["detail"].split(":")[0] for r in runs if r["state"] == "failed")),
        "http_calls": len(att),
        "http_retries_by_reason": dict(Counter(a["error"] for a in att if a["status"] != 200)),
        "backoff_seconds_total": round(sum(a["wait_s"] for a in att), 1),
        "hourly_rows": db.query("SELECT COUNT(*) AS n FROM hourly_observations")[0]["n"],
        "daily_rows": db.query("SELECT COUNT(*) AS n FROM daily_summary")[0]["n"],
        "qc_flagged_hours": [f"{f['city_id']} {f['observed_at']} {f['qc_flags']}" for f in flagged],
        "dq_checks": dict(Counter(r["status"] for r in db.query("SELECT status FROM dq_results"))),
    }


def architecture(out: Path) -> Path:
    fig, ax = plt.subplots(figsize=(13, 5.4))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 5.8)
    ax.axis("off")

    def box(x, y, w, h, title, sub, color=INK):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12", fc="white",
                                    ec=color, lw=2))
        ax.text(x + w / 2, y + h * 0.7, title, ha="center", va="center", fontsize=11, weight="bold", color=color)
        ax.text(x + w / 2, y + h * 0.32, sub, ha="center", va="center", fontsize=8.2, color=MUTED, linespacing=1.4)

    def arrow(x1, y1, x2, y2, color=MUTED):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6, color=color))

    ax.text(0.15, 5.5, "airflow-weather-etl: daily, idempotent, retried and watched", fontsize=14, weight="bold",
            color=INK)
    box(0.2, 2.2, 2.2, 1.5, "Open-Meteo", "archive API (free, no key)\n6 cities · 4 hourly\nmeasures", ACCENT)
    box(2.9, 3.35, 2.6, 1.45, "extract  ×6 (mapped)", "pool open_meteo_api\nHTTP retry: 429/5xx/timeout\n"
        "backoff + Retry-After", PURPLE)
    box(2.9, 1.05, 2.6, 1.45, "raw landing", "data/raw/day=…/city.json\noverwritten per interval", INK)
    box(6.0, 3.35, 2.8, 1.45, "transform_load  ×6", "validate + QC flags\nDQ checks (raise → retry)\n"
        "upsert + rebuild day", PURPLE)
    box(9.3, 3.35, 3.5, 1.45, "PostgreSQL", "hourly_observations (PK city, hour)\ndaily_summary · dq_results\n"
        "etl_task_runs", GOOD)
    box(6.0, 1.05, 2.8, 1.45, "report", "trigger_rule=all_done\nrows · flags · failures", INK)
    box(9.3, 1.05, 3.5, 1.45, "Dataset outlet", "postgres://…/daily_summary\n→ downstream DAGs run on data,\n"
        "not on a clock", SIGNAL)
    arrow(2.4, 3.2, 2.9, 3.8)
    arrow(4.2, 3.35, 4.2, 2.5)
    arrow(5.5, 4.07, 6.0, 4.07)
    arrow(8.8, 4.07, 9.3, 4.07)
    arrow(7.4, 3.35, 7.4, 2.5)
    arrow(8.8, 3.6, 9.3, 2.2, SIGNAL)
    ax.text(0.2, 0.75, "Task policy: retries=3 · exponential backoff (2 → 30 min) · execution_timeout 10 min · "
            "SLA 60 min on transform_load · on_failure + sla_miss callbacks · catchup=True, max_active_runs=3",
            fontsize=8.5, color="#9a6a00")
    ax.text(0.2, 0.35, "Same task code runs in Airflow (PostgresHook) and in the local runner (SQLite) used by "
            "make demo and the tests.", fontsize=8.5, color=MUTED)
    return _save(fig, out, "architecture.png")


def daily_temperatures(settings: Settings, db: Database, out: Path) -> Path:
    rows = db.query("SELECT * FROM daily_summary ORDER BY day")
    days = sorted({r["day"] for r in rows})
    fig, axes = plt.subplots(2, 3, figsize=(13, 6.2), sharex=True, sharey=True)
    for ax, city, color in zip(axes.flat, settings.cities, CITY_COLORS):
        r = [x for x in rows if x["city_id"] == city.id]
        x = np.arange(len(r))
        ax.fill_between(x, [v["temp_min"] for v in r], [v["temp_max"] for v in r], color=color, alpha=0.18,
                        label="min–max")
        ax.plot(x, [v["temp_mean"] for v in r], color=color, lw=2, marker="o", ms=3.5, label="mean")
        for i, v in enumerate(r):
            if v["precip_total"]:
                ax.text(i, v["temp_max"] + 0.7, f"{v['precip_total']:g} mm", ha="center", fontsize=7, color=ACCENT)
        ax.set_title(city.name, fontsize=11)
        ax.grid(axis="y", color=GRID)
        ax.set_xticks(range(len(days)), [d[5:] for d in days], fontsize=8)
    axes[0][0].set_ylabel("°C")
    axes[1][0].set_ylabel("°C")
    axes[0][0].legend(frameon=False, fontsize=8, loc="upper right")
    fig.suptitle("daily_summary: temperature range and mean per city (synthetic fixture data; blue = rain total)",
                 x=0.01, ha="left", fontsize=13, weight="bold", color=INK)
    return _save(fig, out, "daily_temperatures.png")


def task_grid(settings: Settings, db: Database, out: Path) -> Path:
    runs = db.query("SELECT * FROM etl_task_runs")
    days = sorted({r["day"] for r in runs})
    states = {"ok": (GOOD, "success, first try"), "http": (ACCENT, "success after HTTP retries"),
              "qc": (SIGNAL, "success with QC-flagged hours"),
              "retry": (PURPLE, "task failed, Airflow retry succeeded"),
              "failed": (BAD, "failed after all retries")}
    fig, ax = plt.subplots(figsize=(11, 3.9))
    for i, city in enumerate(settings.cities):
        for j, day in enumerate(days):
            r = [x for x in runs if x["city_id"] == city.id and x["day"] == day]
            final = max(r, key=lambda x: (x["try_number"], x["task_id"] == "transform_load"))
            if final["state"] != "success":
                key = "failed"
            elif any(x["try_number"] > 1 for x in r):
                key = "retry"
            elif any(x["task_id"] == "extract" and x["http_calls"] > 1 for x in r):
                key = "http"
            elif any(x["detail"] for x in r if x["state"] == "success"):
                key = "qc"
            else:
                key = "ok"
            ax.add_patch(plt.Rectangle((j + 0.06, i + 0.08), 0.88, 0.84, color=states[key][0]))
            calls = sum(x["http_calls"] for x in r if x["task_id"] == "extract")
            label = f"try {max(x['try_number'] for x in r)}" if key == "retry" else (
                f"{calls} calls" if key == "http" else ("flag" if key == "qc" else ""))
            if label:
                ax.text(j + 0.5, i + 0.5, label, ha="center", va="center", fontsize=8, color="white")
    ax.set_xlim(0, len(days))
    ax.set_ylim(0, len(settings.cities))
    ax.invert_yaxis()
    ax.set_xticks(np.arange(len(days)) + 0.5, [d[5:] for d in days])
    ax.set_yticks(np.arange(len(settings.cities)) + 0.5, [c.name for c in settings.cities])
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.legend(handles=[Patch(color=c, label=lbl) for c, lbl in states.values()], frameon=False, fontsize=8,
              ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.1))
    ax.set_title("Mapped task instances per DAG run (one cell = one city on one logical date)")
    return _save(fig, out, "task_grid.png")


def http_and_qc(db: Database, attempts_file: Path, out: Path) -> Path:
    att = _attempts(attempts_file)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.0), gridspec_kw={"width_ratios": [1, 1.5]})
    c = Counter("200 OK" if a["status"] == 200 else a["error"] for a in att)
    labels = sorted(c, key=lambda k: (k != "200 OK", k))
    a1.barh(labels, [c[k] for k in labels], color=[GOOD if k == "200 OK" else SIGNAL for k in labels], height=0.6)
    for i, k in enumerate(labels):
        waits = [a["wait_s"] for a in att if (a["error"] or "200 OK") == k and a["status"] != 200]
        a1.text(c[k], i, f" {c[k]}" + (f"  (backoff {sum(waits):.1f} s)" if waits else ""), va="center", fontsize=9)
    a1.invert_yaxis()
    a1.set_xscale("log")
    a1.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    a1.set_xlim(0.8, max(c.values()) * 8)
    a1.grid(axis="x", color=GRID)
    a1.set_title("HTTP attempts by outcome (log scale)")
    flagged = db.query("SELECT city_id, observed_at FROM hourly_observations WHERE qc_flags IS NOT NULL")
    if flagged:
        city, ts = flagged[0]["city_id"], flagged[0]["observed_at"]
        day = ts[:10]
        rows = db.query("SELECT observed_at, temperature_2m, qc_flags FROM hourly_observations WHERE city_id = :c "
                        "AND observed_at >= :s AND observed_at < :e ORDER BY observed_at",
                        {"c": city, "s": f"{day}T00:00:00Z", "e": f"{day}T23:59:59Z"})
        x = [int(r["observed_at"][11:13]) for r in rows]
        a2.plot(x, [r["temperature_2m"] for r in rows], color=ACCENT, marker="o", ms=4, lw=1.8)
        for r in rows:
            if r["qc_flags"]:
                h = int(r["observed_at"][11:13])
                a2.axvspan(h - 0.5, h + 0.5, color=BAD, alpha=0.15)
                a2.text(h + 0.7, np.nanmax([v["temperature_2m"] or np.nan for v in rows]) - 1,
                        f"{r['qc_flags']}\nstored as NULL, hour kept, qc_flags set", fontsize=8.5, color=BAD)
        a2.set_xlabel("hour (UTC)")
        a2.set_ylabel("°C")
        a2.grid(color=GRID)
        a2.set_title(f"QC: impossible reading removed ({city}, {day})")
    return _save(fig, out, "http_and_qc.png")


def render_all(settings: Settings, db: Database, attempts_file: Path, out: Path) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    return [architecture(out), daily_temperatures(settings, db, out), task_grid(settings, db, out),
            http_and_qc(db, attempts_file, out)]
