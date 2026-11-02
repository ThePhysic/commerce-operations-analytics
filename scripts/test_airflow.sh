#!/usr/bin/env bash
# Requires a dedicated, empty commerce database; never run against shared data.
set -euo pipefail

export AIRFLOW_HOME
AIRFLOW_HOME="$(mktemp -d /tmp/commerce-airflow-smoke.XXXXXX)"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING
export COMMERCE_ROOT=/opt/commerce
export COMMERCE_DATA_DIR=/workspace/data

/opt/commerce-venv/bin/commerce generate --output /workspace/data/incoming --days 7 --orders-per-day 20 --seed 42
/opt/commerce-venv/bin/python /opt/commerce/scripts/stage_intervals.py \
    --manifest /workspace/data/incoming/manifest.json --output /workspace/data/scheduled
python -c "from airflow.models import DagBag; b=DagBag('/opt/airflow/dags',include_examples=False); assert not b.import_errors,b.import_errors; d=b.get_dag('commerce_batch'); assert d.max_active_runs==1; assert d.get_task('validate_transform_publish').execution_timeout.total_seconds()==900; print('DAG parsed with bounded task')"
airflow db migrate
airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-01
/opt/commerce-venv/bin/python -c "from commerce.db import connect; c=connect(); r=c.execute(\"select status,dbt_results from meta.runs order by started_at desc limit 1\").fetchone(); assert r['status']=='published',r; assert r['dbt_results']; assert not any(x['status'] in {'fail','error','skipped'} for x in r['dbt_results']); assert c.execute('select count(*) n from analytics.fct_order_line').fetchone()['n']==27; print('Airflow scheduled partition published 27 order lines with all dbt gates passing')"
