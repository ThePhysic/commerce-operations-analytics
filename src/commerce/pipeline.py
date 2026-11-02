"""Serialized ingestion, isolated dbt builds, and transactional mart publication."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

from .db import ROOT, locked_connection
from .ingest import ingest, read_manifest, unresolved_quarantine


def data_root():
    return Path(os.getenv("COMMERCE_DATA_DIR", ROOT / "data"))


def _date_range(start, end):
    return {start + timedelta(days=n) for n in range((end - start).days + 1)}


def accepted_horizon(conn):
    """Earliest safe report date for the current durable canonical state.

    A failed run can accept records without publishing them. Their event dates
    still bound a later retry, including refunds on old sales and fulfillment
    timestamps on old order lines. Only normalized, source-defined fields count.
    """
    return conn.execute("""select max(value::date) horizon from landing.records r
      cross join lateral (values
        (case r.source when 'order_lines' then r.payload->>'sale_date'
          when 'refunds' then r.payload->>'refund_date'
          when 'inventory' then r.payload->>'snapshot_date'
          when 'products' then left(r.payload->>'effective_at',10)
          when 'locations' then left(r.payload->>'effective_at',10) end),
        (case when r.source='order_lines' then left(r.payload->>'paid_at',10) end),
        (case when r.source='order_lines' then left(r.payload->>'fulfilled_at',10) end)
      ) dates(value) where value is not null""").fetchone()["horizon"]


def _change_dates(conn, change):
    source = change["source"]
    if source in {"products", "locations"}:
        return None
    dates = set()
    for payload in (change["before_payload"], change["after_payload"]):
        if not payload:
            continue
        if source == "refunds":
            row = conn.execute("select payload->>'sale_date' d from landing.records where source='order_lines' and business_key=%s", (payload["line_id"],)).fetchone()
            if row:
                dates.add(date.fromisoformat(row["d"]))
        else:
            dates.add(date.fromisoformat(payload["sale_date" if source == "order_lines" else "snapshot_date"]))
    return dates


def _check_sources(conn, as_of):
    config = json.loads((ROOT / "contracts" / "quality.json").read_text())
    unresolved = unresolved_quarantine(conn)
    if unresolved:
        raise ValueError(f"Unresolved quarantined source records; re-deliver matching corrections or explicitly resolve: {unresolved}")
    rows = {r["source"]: r for r in conn.execute("select * from meta.source_status")}
    for source, rule in config["sources"].items():
        if source not in rows:
            raise ValueError(f"Missing required source: {source}")
        age = (as_of - rows[source]["latest_partition"]).days
        if age > rule["max_age_days"]:
            raise ValueError(f"Stale source {source}: {age} days at as_of={as_of}")
        if rule.get("daily_complete"):
            partitions = {r["d"] for r in conn.execute("select distinct partition_date d from meta.files where source=%s and partition_date<=%s", (source, as_of))}
            expected = _date_range(min(partitions), as_of)
            missing = expected - partitions
            if missing:
                raise ValueError(f"Incomplete source {source}; missing partitions {sorted(missing)}")


def _run_dbt(conn, run_id, schema, *, full_refresh):
    artifact = data_root() / "artifacts" / run_id
    artifact.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(PGHOST=conn.info.host, PGPORT=str(conn.info.port), PGUSER=conn.info.user,
               PGPASSWORD=conn.info.password or "", PGDATABASE=conn.info.dbname,
               DBT_TARGET_SCHEMA=schema, COMMERCE_RUN_ID=run_id, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               DBT_TARGET_PATH=str(artifact / "target"), DBT_LOG_PATH=str(artifact / "logs"))
    command = [sys.executable, "-c", "from dbt.cli.main import cli; cli()", "build", "--project-dir", str(ROOT / "warehouse"), "--profiles-dir", str(ROOT / "warehouse"), "--no-use-colors"]
    if full_refresh:
        command.append("--full-refresh")
    with (artifact / "dbt.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
    results_file = artifact / "target" / "run_results.json"
    results = json.loads(results_file.read_text()) if results_file.exists() else {"results": []}
    summary = [{"unique_id": x["unique_id"], "status": x["status"], "failures": x.get("failures")} for x in results["results"]]
    conn.execute("update meta.runs set dbt_results=%s where run_id=%s", (Jsonb(summary), run_id))
    if result.returncode or not summary or any(x["status"] in {"fail", "error", "skipped"} for x in summary):
        raise RuntimeError(f"dbt build failed; inspect {artifact / 'dbt.log'}")
    return artifact


def _publish(conn, run_id, schema, as_of, changes, *, fail_at=None):
    tables = [r["tablename"] for r in conn.execute("select tablename from pg_tables where schemaname=%s order by tablename", (schema,))]
    if not tables or "mart_daily_location_sales" not in tables:
        raise RuntimeError("Missing analytical models")
    counts = {t: conn.execute(sql.SQL("select count(*) n from {}.{}").format(sql.Identifier(schema), sql.Identifier(t))).fetchone()["n"] for t in tables}
    totals = [r["row"] for r in conn.execute(sql.SQL("select to_jsonb(t) as row from (select sale_date::text, location_id, gross_sales_cents,discount_cents,refund_cents,net_sales_cents from {}.mart_daily_location_sales order by sale_date,location_id) t").format(sql.Identifier(schema))).fetchall()]
    with conn.transaction():
        # All reader views and the publication/audit ledger move together.
        for index, table in enumerate(tables):
            conn.execute(sql.SQL("create or replace view analytics.{} as select * from {}.{}").format(sql.Identifier(table), sql.Identifier(schema), sql.Identifier(table)))
            if index == 0 and fail_at == "during-publish":
                raise RuntimeError("Injected failure during-publish after first view replacement")
        pub = conn.execute("""insert into meta.publications(run_id,schema_name,as_of,source_change_id,row_counts,sales_totals)
          values(%s,%s,%s,%s,%s,%s) returning publication_id""",
          (run_id, schema, as_of, max([c["id"] for c in changes], default=0), Jsonb(counts), Jsonb(totals))).fetchone()["publication_id"]
        for change in changes:
            conn.execute("insert into meta.publication_sources values(%s,%s)", (pub, change["id"]))
        conn.execute("update meta.runs set status='published',ended_at=now() where run_id=%s", (run_id,))
    return {"run_id": run_id, "publication_id": pub, "schema": schema, "row_counts": counts}


def run(manifest: Path, *, full_refresh=False, start=None, end=None, as_of=None, lookback_days=7, fail_at=None):
    if (start is None) != (end is None) or (start and start > end):
        raise ValueError("Provide a valid inclusive --start and --end together")
    if lookback_days < 0:
        raise ValueError("lookback_days must be nonnegative")
    source_manifest, manifest_hash, verified = read_manifest(manifest)
    explicit_as_of = as_of is not None
    as_of = as_of or max(date.fromisoformat(e["partition"]) for e, _ in verified)
    run_id = uuid.uuid4().hex
    schema = "build_" + run_id
    started = time.perf_counter()
    with locked_connection() as conn:
        previous = conn.execute("select * from meta.publications order by publication_id desc limit 1").fetchone()
        horizon = accepted_horizon(conn)
        if explicit_as_of and horizon and as_of < horizon:
            raise ValueError(f"as_of {as_of} precedes accepted source horizon {horizon}; complete that reporting horizon or query a retained publication")
        if horizon and not explicit_as_of:
            as_of = max(as_of, horizon)
        if previous and not explicit_as_of:
            as_of = max(as_of, previous["as_of"])
        if end and end > as_of:
            raise ValueError("Backfill end cannot be after as_of")
        if previous and as_of < previous["as_of"]:
            raise ValueError("as_of cannot move backward; query retained publication schemas for past reports")
        # A killed process leaves an inspectable candidate; the lock proves it is no longer running.
        conn.execute("update meta.runs set status='abandoned',ended_at=now(),error_category='interrupted' where status='running'")
        conn.execute("""insert into meta.runs(run_id,status,manifest_path,manifest_hash,candidate_schema,data_start,data_end,as_of,source_manifest)
          values(%s,'running',%s,%s,%s,%s,%s,%s,%s)""", (run_id, str(manifest.resolve()), manifest_hash, schema, start, end, as_of, Jsonb(source_manifest)))
        try:
            counts = ingest(conn, run_id, verified, data_root() / "raw", as_of)
            if fail_at == "after-ingest":
                raise RuntimeError("Injected failure after-ingest")
            max_rejected = json.loads((ROOT / "contracts" / "quality.json").read_text())["max_rejected_rows"]
            if counts["rejected"] > max_rejected:
                raise ValueError(f"Quality gate: {counts['rejected']} quarantined rows; fix source/version and rerun")
            _check_sources(conn, as_of)
            pending = conn.execute("""select c.* from meta.source_changes c where not exists
              (select 1 from meta.publication_sources p where p.source_change_id=c.id) order by c.id""").fetchall()
            catalog_change = any(c["source"] in {"products", "locations"} for c in pending)
            full_refresh = full_refresh or previous is None or catalog_change
            if full_refresh:
                selected = {date.fromisoformat(r["d"]) for r in conn.execute("""select distinct coalesce(payload->>'sale_date',payload->>'snapshot_date') d
                  from landing.records where source in ('order_lines','inventory')""")}
                applied = pending
            else:
                dates_by_change = [(c, _change_dates(conn, c)) for c in pending]
                if start:
                    selected = _date_range(start, end)
                    applied = [c for c, ds in dates_by_change if ds and ds.issubset(selected)]
                    outside = [c for c, ds in dates_by_change if not ds or not ds.issubset(selected)]
                    if outside:
                        raise ValueError("Pending changes outside backfill range; include all changed old/new dates or full-refresh")
                else:
                    selected = _date_range(as_of - timedelta(days=lookback_days), as_of)
                    outside = [(c, ds) for c, ds in dates_by_change if not ds or not ds.issubset(selected)]
                    if outside:
                        raise ValueError("Pending changes outside lookback window or unresolved refund references; use explicit backfill/full-refresh")
                    applied = pending
            with conn.transaction():
                conn.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
                if previous and not full_refresh:
                    tables = conn.execute("select tablename from pg_tables where schemaname=%s", (previous["schema_name"],)).fetchall()
                    for row in tables:
                        conn.execute(sql.SQL("create table {}.{} (like {}.{} including all)").format(sql.Identifier(schema), sql.Identifier(row["tablename"]), sql.Identifier(previous["schema_name"]), sql.Identifier(row["tablename"])))
                        conn.execute(sql.SQL("insert into {}.{} select * from {}.{}").format(sql.Identifier(schema), sql.Identifier(row["tablename"]), sql.Identifier(previous["schema_name"]), sql.Identifier(row["tablename"])))
                for d in sorted(selected):
                    conn.execute("insert into meta.reprocess_dates values(%s,%s)", (run_id, d))
                if selected:
                    conn.execute("update meta.runs set data_start=%s,data_end=%s where run_id=%s", (min(selected), max(selected), run_id))
            _run_dbt(conn, run_id, schema, full_refresh=full_refresh)
            if fail_at == "before-publish":
                raise RuntimeError("Injected failure before-publish")
            result = _publish(conn, run_id, schema, as_of, applied, fail_at=fail_at)
            result["ingestion"] = counts
            result["full_refresh"] = full_refresh
            return result
        except Exception as exc:
            conn.execute("update meta.runs set status='failed',ended_at=now(),error_category=%s,error=%s where run_id=%s", (type(exc).__name__, str(exc), run_id))
            raise
        finally:
            conn.execute("update meta.runs set elapsed_seconds=%s where run_id=%s", (time.perf_counter() - started, run_id))
