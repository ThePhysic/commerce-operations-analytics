"""Command-line entry points for the local batch platform."""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import date
from pathlib import Path

from .db import ROOT, connect, initialize, locked_connection
from .generate import generate
from .pipeline import accepted_horizon, data_root, run
from .reader import pinned_publication


def emit(value):
    print(json.dumps(value, indent=2, default=str))


def quarantine(action, *, id=None, reason=None, all=False):
    """Inspect retained failures or record an explicit operator disposition."""
    if action == "resolve" and (not isinstance(reason, str) or not reason.strip()):
        raise ValueError("A nonempty operator resolution reason is required")
    with locked_connection() as conn:
        if action == "list":
            return conn.execute("""select id,issue_kind,source,business_key,source_version,raw_path,reason,run_id,
              created_at,resolved_at,resolved_by_run_id,resolution_reason from meta.quarantine
              where (%s or resolved_at is null) order by id""", (all,)).fetchall()
        if action != "resolve":
            raise ValueError("Unknown quarantine action")
        run_id = uuid.uuid4().hex
        with conn.transaction():
            row = conn.execute("select * from meta.quarantine where id=%s for update", (id,)).fetchone()
            if row is None:
                raise ValueError(f"Unknown quarantine ID: {id}")
            if row["resolved_at"] is not None:
                raise ValueError(f"Quarantine {id} is already resolved; its original audit is preserved")
            conn.execute("""insert into meta.runs(run_id,status,ended_at)
              values(%s,'operator-resolved',now())""", (run_id,))
            return conn.execute("""update meta.quarantine set resolved_at=now(),resolved_by_run_id=%s,
              resolution_reason=%s where id=%s returning id,source,business_key,source_version,raw_path,
              resolved_at,resolved_by_run_id,resolution_reason""", (run_id, "Operator disposition: " + reason.strip(), id)).fetchone()


def status():
    with connect() as conn:
        initialize(conn)
        publications = conn.execute("select * from meta.publications order by publication_id desc limit 2").fetchall()
        last_good = publications[0] if publications else None
        sources = conn.execute("select *,extract(epoch from now()-last_received_at) wall_clock_age_seconds from meta.source_status order by source").fetchall()
        certified = {r["source"]: r["partition"] for r in conn.execute("""select source,max(partition_date) partition
          from meta.files where file_failures=0 and rejected=0 group by source""")}
        horizon = accepted_horizon(conn)
        target = max([d for d in (horizon, last_good["as_of"] if last_good else None) if d is not None], default=None)
        config = json.loads((ROOT / "contracts" / "quality.json").read_text())
        for source in sources:
            source["latest_certified_partition"] = certified.get(source["source"])
            source["reporting_lag_days"] = (target - source["latest_certified_partition"]).days if target and source["latest_certified_partition"] else None
            source["freshness_threshold_days"] = config["sources"][source["source"]]["max_age_days"]
        current_counts = last_good["row_counts"] if last_good else {}
        prior_counts = publications[1]["row_counts"] if len(publications)>1 else {}
        return {
            "last_good": last_good,
            "reporting_target": target,
            "sources": sources,
            "row_count_deltas": {k: current_counts.get(k,0)-prior_counts.get(k,0) for k in current_counts.keys() | prior_counts.keys()},
            "run_failure_rate": conn.execute("select coalesce(count(*) filter(where status in ('failed','abandoned'))::numeric/nullif(count(*),0),0) rate from meta.runs where status<>'operator-resolved'").fetchone()["rate"],
            "unresolved_quarantine_count": conn.execute("select count(*) n from meta.quarantine where resolved_at is null").fetchone()["n"],
            "recent_runs": conn.execute("select * from meta.runs order by started_at desc limit 10").fetchall(),
            "pending_source_changes": conn.execute("select count(*) n from meta.source_changes c where not exists(select 1 from meta.publication_sources p where p.source_change_id=c.id)").fetchone()["n"],
        }


def query(name, publication_id=None):
    with pinned_publication(publication_id=publication_id) as pinned:
        conn = pinned.connection
        if name == "changes":
            pubs = conn.execute("select * from meta.publications where publication_id<=%s order by publication_id desc limit 2", (pinned.publication_id,)).fetchall()
            if not pubs:
                return {"message": "No publication yet"}
            latest = pubs[0]
            before = {(r["sale_date"], r["location_id"]): r for r in pubs[1]["sales_totals"]} if len(pubs) > 1 else {}
            after = {(r["sale_date"], r["location_id"]): r for r in latest["sales_totals"]}
            deltas = []
            for key in sorted(before.keys() | after.keys()):
                old = before.get(key, {}).get("net_sales_cents", 0)
                new = after.get(key, {}).get("net_sales_cents", 0)
                if old != new:
                    deltas.append({"sale_date": key[0], "location_id": key[1], "before_net_cents": old, "after_net_cents": new, "delta_cents": new-old})
            sources = conn.execute("""select c.source,c.business_key,c.before_payload,c.after_payload,c.raw_path
              from meta.publication_sources p join meta.source_changes c on c.id=p.source_change_id
              where p.publication_id=%s order by c.id""", (latest["publication_id"],)).fetchall()
            prior_counts = pubs[1]["row_counts"] if len(pubs)>1 else {}
            row_deltas = {k: latest["row_counts"].get(k,0)-prior_counts.get(k,0) for k in latest["row_counts"].keys() | prior_counts.keys()}
            return {"publication_id": latest["publication_id"], "previous_id": pubs[1]["publication_id"] if len(pubs)>1 else None, "sales_deltas": deltas, "source_changes": sources, "row_counts": latest["row_counts"], "row_count_deltas": row_deltas}
        return pinned.query(name)


def demo(output=None):
    output = Path(output) if output else data_root() / "demo"
    manifest = generate(output, days=7, orders_per_day=20, seed=42)
    initial = run(manifest, full_refresh=True)
    with connect() as conn:
        before = conn.execute("select sales_totals,row_counts from meta.publications where publication_id=%s", (initial["publication_id"],)).fetchone()
    replay = run(manifest)
    with connect() as conn:
        after = conn.execute("select sales_totals,row_counts from meta.publications where publication_id=%s", (replay["publication_id"],)).fetchone()
    if before != after:
        raise AssertionError("Replay changed warehouse counts/totals")
    return {"synthetic": True, "initial": initial, "replay": replay, "replay_identical": True, "sales": query("sales"), "next": "commerce status; commerce query --name changes; pytest -m integration"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Synthetic commerce batch analytics platform")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--output", type=Path, default=data_root() / "incoming")
    gen.add_argument("--days", type=int, default=7)
    gen.add_argument("--orders-per-day", type=int, default=20)
    gen.add_argument("--seed", type=int, default=42)
    load = sub.add_parser("run")
    load.add_argument("--manifest", type=Path, required=True)
    load.add_argument("--full-refresh", action="store_true")
    load.add_argument("--start", type=date.fromisoformat)
    load.add_argument("--end", type=date.fromisoformat)
    load.add_argument("--as-of", type=date.fromisoformat)
    load.add_argument("--lookback-days", type=int, default=7)
    load.add_argument("--expected-start", type=date.fromisoformat,
                      help="Persist required daily coverage before reading files; scheduled logical date")
    load.add_argument("--fail-at", choices=["after-ingest", "before-publish", "during-publish"])
    sub.add_parser("status")
    quarantined = sub.add_parser("quarantine", help="Inspect or explicitly resolve retained quarantine records")
    quarantine_actions = quarantined.add_subparsers(dest="action", required=True)
    listed = quarantine_actions.add_parser("list")
    listed.add_argument("--all", action="store_true", help="Include resolved records")
    resolved = quarantine_actions.add_parser("resolve")
    resolved.add_argument("--id", type=int, required=True)
    resolved.add_argument("--reason", required=True)
    q = sub.add_parser("query")
    q.add_argument("--name", required=True, choices=["sales", "refunds", "fulfillment", "inventory", "refund_events", "changes"])
    q.add_argument("--publication-id", type=int, help="Query a retained publication instead of selecting the latest")
    sub.add_parser("publication", help="Show metadata for the publication visible to a new reader")
    d = sub.add_parser("demo")
    d.add_argument("--output", type=Path)
    b = sub.add_parser("benchmark")
    b.add_argument("--output", type=Path, default=data_root() / "benchmark.json")
    b.add_argument("--small", type=int, default=100)
    b.add_argument("--large", type=int, default=1000)
    b.add_argument("--repeats", type=int, default=3)
    args = vars(parser.parse_args(argv))
    command = args.pop("command")
    try:
        if command == "generate":
            emit({"manifest": str(generate(**args)), "synthetic": True})
        elif command == "run":
            emit(run(**args))
        elif command == "status":
            emit(status())
        elif command == "query":
            emit(query(**args))
        elif command == "publication":
            with pinned_publication() as pinned:
                emit(pinned.publication)
        elif command == "quarantine":
            emit(quarantine(**args))
        elif command == "demo":
            emit(demo(**args))
        elif command == "benchmark":
            from .benchmark import benchmark
            emit(benchmark(**args))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
