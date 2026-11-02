"""Connections and durable audit schema; SQL values are always bound parameters."""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ROOT = Path(os.getenv("COMMERCE_ROOT", Path.cwd() if (Path.cwd() / "warehouse").is_dir() else Path(__file__).resolve().parents[2]))


def connect():
    url = os.getenv("COMMERCE_DATABASE_URL")
    return psycopg.connect(url or "", row_factory=dict_row, autocommit=True)


@contextmanager
def locked_connection():
    with connect() as conn:
        # One session lock covers source changes, dbt, and publication.
        if not conn.execute("select pg_try_advisory_lock(70402026)").fetchone()["pg_try_advisory_lock"]:
            raise RuntimeError("Another commerce pipeline holds the writer lock; retry later")
        try:
            initialize(conn)
            yield conn
        finally:
            conn.execute("select pg_advisory_unlock(70402026)")


def initialize(conn):
    conn.execute("""
    create schema if not exists landing;
    create schema if not exists meta;
    create schema if not exists analytics;
    create table if not exists meta.runs (
      run_id text primary key, status text not null, started_at timestamptz default now(),
      ended_at timestamptz, manifest_path text, manifest_hash text, candidate_schema text,
      data_start date, data_end date, as_of date, received bigint default 0,
      accepted bigint default 0, rejected bigint default 0, changed bigint default 0,
      error_category text, error text, dbt_results jsonb, elapsed_seconds double precision);
    alter table meta.runs add column if not exists source_manifest jsonb;
    create table if not exists landing.records (
      source text not null, business_key text not null, version integer not null,
      payload jsonb not null, payload_hash text not null, raw_path text not null,
      run_id text not null references meta.runs, ingested_at timestamptz default now(),
      primary key(source,business_key));
    create table if not exists meta.files (
      run_id text not null references meta.runs, source text not null, source_path text not null,
      raw_path text not null, sha256 text not null, schema_version integer not null,
      partition_date date not null, expected_rows bigint not null, received bigint not null,
      accepted bigint not null, rejected bigint not null,
      primary key(run_id,source,source_path));
    create table if not exists meta.quarantine (
      id bigint generated always as identity primary key, run_id text not null references meta.runs,
      source text not null, raw_path text not null, row_number bigint not null,
      payload jsonb not null, reason text not null, created_at timestamptz default now());
    create table if not exists meta.source_changes (
      id bigint generated always as identity primary key, run_id text not null references meta.runs,
      source text not null, business_key text not null, before_payload jsonb, after_payload jsonb not null,
      raw_path text not null);
    create table if not exists meta.publications (
      publication_id bigint generated always as identity primary key,
      run_id text unique not null references meta.runs, schema_name text unique not null,
      published_at timestamptz default now(), as_of date not null, source_change_id bigint not null,
      row_counts jsonb not null, sales_totals jsonb not null);
    create table if not exists meta.publication_sources (
      publication_id bigint references meta.publications, source_change_id bigint references meta.source_changes,
      primary key(publication_id,source_change_id));
    create table if not exists meta.reprocess_dates (
      run_id text references meta.runs, business_date date, primary key(run_id,business_date));
    create or replace view meta.source_status as
      select source,max(partition_date) latest_partition,max(r.started_at) last_received_at,
      sum(f.received) received,sum(f.rejected) rejected
      from meta.files f join meta.runs r using(run_id) group by source;
    """)
