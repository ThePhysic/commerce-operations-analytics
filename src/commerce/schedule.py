"""Persist expected daily coverage before attempting source-file reads."""
from __future__ import annotations

import json

from .db import ROOT


def daily_sources():
    config = json.loads((ROOT / "contracts" / "quality.json").read_text(encoding="utf-8"))
    return [name for name, rule in config["sources"].items() if rule.get("daily_complete")]


def register_expected_start(conn, expected_start=None):
    """Preserve the earliest required date, including failed first attempts.

    Existing installations bootstrap from certified deliveries. Scheduled runs
    explicitly declare their logical date BEFORE opening the manifest, so a
    missing first file cannot move the completeness boundary forward tomorrow.
    """
    conn.execute("""create table if not exists meta.source_expectations (
      source text primary key, expected_start date not null,
      declared_at timestamptz not null default now())""")
    sources = daily_sources()
    with conn.transaction():
        for source in sources:
            if expected_start is not None:
                conn.execute("""insert into meta.source_expectations(source,expected_start) values(%s,%s)
                  on conflict(source) do update set expected_start=least(meta.source_expectations.expected_start,excluded.expected_start)""", (source, expected_start))
            conn.execute("""insert into meta.source_expectations(source,expected_start)
              select source,min(partition_date) from meta.files
              where source=%s and file_failures=0 and rejected=0 group by source
              on conflict(source) do update set expected_start=least(meta.source_expectations.expected_start,excluded.expected_start)""", (source,))
    return {row["source"]: row["expected_start"] for row in conn.execute("select source,expected_start from meta.source_expectations")}
