"""``python -m weather_etl <command>``"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import click

from .config import ROOT, load_settings
from .db import Database, schema_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
DEFAULT_DB = f"sqlite:///{ROOT / 'data' / 'weather.db'}"


@click.group()
@click.option("--config", "config_path", default=None, type=click.Path(exists=True, dir_okay=False))
@click.pass_context
def cli(ctx, config_path):
    """Weather ETL: Open-Meteo → PostgreSQL/SQLite, the same tasks as the Airflow DAG."""
    ctx.obj = load_settings(config_path)


@cli.command()
@click.option("--start", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--end", type=click.DateTime(["%Y-%m-%d"]), required=True)
@click.option("--source", type=click.Choice(["fixture", "api"]), default="fixture",
              help="fixture = deterministic synthetic responses (default); api = the real Open-Meteo archive")
@click.option("--db", "db_url", default=DEFAULT_DB, help="sqlite:///path or postgresql://user:pw@host/db")
@click.option("--raw-dir", default=str(ROOT / "data" / "raw"))
@click.pass_obj
def run(settings, start, end, source, db_url, raw_dir):
    """Backfill [start, end] day by day, like `airflow dags backfill`."""
    from .runner import run as run_local

    db = Database.from_url(db_url)
    out = run_local(settings, db, start.date(), end.date(), Path(raw_dir), source,
                    attempts_file=ROOT / "data" / "http_attempts.jsonl")
    click.echo(json.dumps(out, indent=2))
    if out["failed"]:
        raise SystemExit(1)


@cli.command()
@click.option("--db", "db_url", default=DEFAULT_DB)
@click.option("--out", default=str(ROOT / "docs" / "images"))
@click.pass_obj
def report(settings, db_url, out):
    """Charts from the loaded tables, the task log and the HTTP attempt log."""
    from .report import render_all

    for p in render_all(settings, Database.from_url(db_url), ROOT / "data" / "http_attempts.jsonl", Path(out)):
        click.echo(f"wrote {p}")


@cli.command()
@click.option("--db", "db_url", default=DEFAULT_DB)
@click.pass_obj
def summary(settings, db_url):
    """The numbers quoted in the README, computed from the tables."""
    from .report import summary as make_summary

    click.echo(json.dumps(make_summary(Database.from_url(db_url), ROOT / "data" / "http_attempts.jsonl"), indent=2))


@cli.command()
@click.option("--dialect", type=click.Choice(["postgres", "sqlite"]), default="postgres")
def schema(dialect):
    """Print the DDL for a dialect."""
    click.echo(schema_sql(dialect), nl=False)


def main():
    cli()


if __name__ == "__main__":
    main()
