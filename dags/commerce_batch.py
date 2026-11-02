"""Finite source-partition ingestion, dbt validation, and atomic mart publication."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="commerce_batch",
    description="Synthetic commerce daily batch, quality gates, and publication",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "synthetic-commerce", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["synthetic", "batch", "dbt"],
    doc_md="""
The source producer delivers `/workspace/data/scheduled/YYYY-MM-DD/manifest.json`
before each run. Use scripts/stage_intervals.py for deterministic local delivery.
The task ingests those finite files and reconciles a bounded seven-day window,
builds/tests an isolated dbt schema, and publishes only after every gate passes.
For the deterministic fixture, use the documented historical backfill dates.
Airflow metadata uses its own SQLite file; commerce facts use PostgreSQL.
""",
) as dag:
    publish = BashOperator(
        task_id="validate_transform_publish",
        bash_command=(
            'exec /opt/commerce-venv/bin/commerce run '
            '--manifest "/workspace/data/scheduled/$PARTITION_DATE/manifest.json" '
            '--as-of "$PARTITION_DATE" --lookback-days 7'
        ),
        env={"PARTITION_DATE": "{{ data_interval_start | ds }}"},
        append_env=True,
        execution_timeout=timedelta(minutes=15),
        do_xcom_push=False,
    )
