"""Repeatable, bounded synthetic workloads; no unmeasured speed claims."""
from __future__ import annotations

import json
import os
import platform
import statistics
import threading
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import psutil
from psycopg import sql
from psycopg.conninfo import make_conninfo

from .db import connect
from .generate import generate, write_dataset
from .ingest import iter_rows, read_manifest
from .pipeline import data_root, run


def _measure(fn):
    stop = threading.Event()
    peak = [0]
    process = psutil.Process()

    def sample():
        while not stop.is_set():
            rss = 0
            for child in [process] + process.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak[0] = max(peak[0], rss)
            stop.wait(0.025)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    start = time.perf_counter()
    try:
        result = fn()
        return result, {"elapsed_seconds": time.perf_counter()-start, "peak_client_tree_rss_bytes": peak[0]}
    finally:
        stop.set()
        monitor.join()


def _query_evidence(last_date):
    """Measure the same selective analytical query on each published variant."""
    with connect() as conn:
        schema = conn.execute("select schema_name from meta.publications order by publication_id desc limit 1").fetchone()["schema_name"]
        conn.execute(sql.SQL("analyze {}.fct_order_line").format(sql.Identifier(schema)))
        statement = "select sum(gross_sales_cents) from analytics.fct_order_line where sale_date=%s"
        latencies = []
        for _ in range(10):
            started = time.perf_counter()
            conn.execute(statement, (last_date,)).fetchone()
            latencies.append((time.perf_counter() - started) * 1000)
        plan = conn.execute("explain (analyze,buffers,format json) " + statement, (last_date,)).fetchone()["QUERY PLAN"]
        return {"query_latency_ms": {"samples": latencies, "median": statistics.median(latencies)}, "query_plan": plan}


def benchmark(*, output: Path, small=100, large=1000, repeats=3):
    if min(small, large, repeats) < 1 or large <= small:
        raise ValueError("Require 0 < small < large and repeats >= 1")
    original_url = os.environ.get("COMMERCE_DATABASE_URL")
    database = "commerce_bench_" + uuid.uuid4().hex[:12]
    root = data_root() / "benchmark" / database
    with connect() as admin:
        # Create/drop only a uniquely named disposable benchmark database.
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(database)))
        conninfo = admin.info.get_parameters()
        conninfo["dbname"] = database
        conninfo["password"] = admin.info.password or ""
        os.environ["COMMERCE_DATABASE_URL"] = make_conninfo(**conninfo)
        report = {
            "synthetic": True, "seed": 42, "repeats": repeats,
            "measured_at_utc": datetime.now(timezone.utc).isoformat(),
            "context": {"os": platform.platform(), "python": platform.python_version(),
                        "cpu": platform.processor(), "logical_cpus": psutil.cpu_count(),
                        "host_memory_bytes": psutil.virtual_memory().total,
                        "postgres": admin.execute("select version() v").fetchone()["v"]},
            "method": "7 sale days; baseline then correction of last sale partition; compare incremental and full rebuild from identical canonical inputs; 25ms RSS sampling of client/dbt process tree; ten client-roundtrip query latency samples and EXPLAIN ANALYZE BUFFERS after ANALYZE for each incremental/full publication.",
            "limitations": ["PostgreSQL memory is excluded from client RSS; Compose caps server at 384MiB.",
                            "Incremental isolation copies every prior physical table and preserves its indexes; dbt tests and staging can scan all rows.",
                            "Rows/sec below is selected order-line throughput over entire run time, not total database tuples scanned.",
                            "Each timed incremental run ingests new correction versions; the following timed full run replays the same manifest with unchanged canonical source rows. End-to-end timings include this difference in source update work.",
                            "Incremental always precedes full within each pair; warm-cache and sequence effects can bias comparisons.",
                            "Client-tree RSS sums process RSS, including shared pages counted separately by each process; it is not unique physical memory.",
                            "Measurements run on a developer workstation; other local processes are not isolated or controlled.",
                            "Small local synthetic workloads; warm caches, no distributed scaling claim; sampling can miss short memory spikes."],
            "workloads": [],
        }
        try:
            for label, size in (("small", small), ("larger", large)):
                # Fresh database between sizes avoids mixing fixture identities.
                with connect() as conn:
                    schemas = conn.execute("select nspname from pg_namespace where nspname in ('landing','meta','analytics') or nspname like 'build\\_%' escape '\\'").fetchall()
                    for s in schemas:
                        conn.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(s["nspname"])))
                manifest = generate(root / label / "baseline", days=7, orders_per_day=size, seed=42)
                _, _, entries = read_manifest(manifest)
                line_entries = [(e, b) for e, b in entries if e["source"] == "order_lines" and e["row_count"]]
                last_date = max(e["partition"] for e, _ in line_entries)
                selected = [row for e, body in line_entries if e["partition"] == last_date for row in iter_rows("order_lines", body)]
                total_lines = sum(e["row_count"] for e, _ in line_entries)
                as_of = max(e["partition"] for e, _ in entries)
                run(manifest, full_refresh=True)
                samples = []
                for repetition in range(repeats):
                    changed = []
                    for row in selected:
                        row = dict(row)
                        row["version"] = repetition+2
                        # Versioned correction changes business output for noncancelled lines.
                        row["unit_price_cents"] = int(row["unit_price_cents"])+repetition+1
                        changed.append(row)
                    delta = write_dataset(root / label / f"delta-{repetition}", {"order_lines": changed})
                    incremental, inc_measure = _measure(lambda: run(delta, start=date.fromisoformat(last_date), end=date.fromisoformat(last_date), as_of=date.fromisoformat(as_of)))
                    inc_measure.update(_query_evidence(last_date))
                    with connect() as conn:
                        expected = conn.execute("select sales_totals from meta.publications order by publication_id desc limit 1").fetchone()["sales_totals"]
                    full, full_measure = _measure(lambda: run(delta, full_refresh=True, as_of=date.fromisoformat(as_of)))
                    full_measure.update(_query_evidence(last_date))
                    with connect() as conn:
                        actual = conn.execute("select sales_totals from meta.publications order by publication_id desc limit 1").fetchone()["sales_totals"]
                    if actual != expected:
                        raise AssertionError("Benchmark incremental/full analytics differ")
                    for measure, lines, result in ((inc_measure, len(selected), incremental), (full_measure, total_lines, full)):
                        measure.update(selected_order_lines=lines, selected_lines_per_second=lines/measure["elapsed_seconds"], accepted_input_rows=result["ingestion"]["accepted"], changed_input_rows=result["ingestion"]["changed"])
                    samples.append({"repetition": repetition+1, "incremental": inc_measure, "full": full_measure, "sales_totals_identical": True})
                with connect() as conn:
                    counts = conn.execute("select row_counts from meta.publications order by publication_id desc limit 1").fetchone()["row_counts"]
                report["workloads"].append({"label": label, "orders_per_day": size, "sale_days": 7, "order_lines": total_lines,
                    "selected_partition": last_date, "row_counts": counts, "samples": samples,
                    "median_full_seconds": statistics.median(s["full"]["elapsed_seconds"] for s in samples),
                    "median_incremental_seconds": statistics.median(s["incremental"]["elapsed_seconds"] for s in samples),
                    "query_latency_ms": samples[-1]["full"]["query_latency_ms"], "query_plan": samples[-1]["full"]["query_plan"]})
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2, default=str)+"\n")
            return {"report": str(output), "workloads": len(report["workloads"]), "repeats": repeats}
        finally:
            if original_url is None:
                os.environ.pop("COMMERCE_DATABASE_URL", None)
            else:
                os.environ["COMMERCE_DATABASE_URL"] = original_url
            admin.execute(sql.SQL("drop database {} with (force)").format(sql.Identifier(database)))
