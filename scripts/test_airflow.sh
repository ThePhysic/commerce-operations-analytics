#!/usr/bin/env bash
# Requires a dedicated, empty commerce database; never run against shared data.
set -euo pipefail

export AIRFLOW_HOME
AIRFLOW_HOME="$(mktemp -d /opt/airflow/state/commerce-airflow-smoke.XXXXXX)"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
export COMMERCE_ROOT=/opt/commerce
export COMMERCE_DATA_DIR=/workspace/data

python -c "from pathlib import Path; p=Path('/opt/airflow/state/uid-permission-probe'); p.write_text('ok'); p.unlink()"
test -f /workspace/data/scheduled/2024-01-01/manifest.json
python -c "import os; from airflow.models import DagBag; assert os.geteuid()!=0; b=DagBag('/opt/airflow/dags',include_examples=False); assert not b.import_errors,b.import_errors; d=b.dags['commerce_batch']; t=d.get_task('validate_transform_publish'); assert d.max_active_runs==1; assert t.execution_timeout.total_seconds()==900; assert t.depends_on_past; assert t.retries==1; print('DAG parsed with bounded task and previous-interval dependency')"
/opt/commerce-venv/bin/python -c "from commerce.db import connect; c=connect(); assert c.info.dbname.endswith('_test'), 'Refusing a non-test database'; assert c.execute(\"select to_regclass('meta.publications') p\").fetchone()['p'] is None, 'Require an empty test database'"
airflow db migrate
first_manifest=/workspace/data/scheduled/2024-01-01/manifest.json
mv "$first_manifest" "${first_manifest}.withheld"
trap 'if [[ -f "${first_manifest}.withheld" ]]; then mv "${first_manifest}.withheld" "$first_manifest"; fi' EXIT
# Disable only this injected failure's retry delay; the DAG's configured retry is asserted above.
if airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-02 --disable-retry; then
    echo "Missing first delivery unexpectedly succeeded" >&2
    exit 1
fi
/opt/commerce-venv/bin/python -c "from commerce.db import connect; c=connect(); assert c.execute('select count(*) n from meta.publications').fetchone()['n']==0; print('Missing first interval prevented later publication')"
mv "${first_manifest}.withheld" "$first_manifest"
python /opt/commerce/scripts/recover_intervals.py --start 2024-01-01 --end 2024-01-02 --reason "Restored withheld synthetic first delivery"
airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-02
/opt/commerce-venv/bin/python -c "from commerce.db import connect; c=connect(); r=c.execute(\"select status,dbt_results from meta.runs order by started_at desc limit 1\").fetchone(); assert r['status']=='published',r; assert r['dbt_results']; assert not any(x['status'] in {'fail','error','skipped'} for x in r['dbt_results']); assert c.execute('select count(*) n from analytics.fct_order_line').fetchone()['n']==54; assert c.execute('select count(distinct sale_date) n from analytics.fct_order_line').fetchone()['n']==2; print('Recovery published both daily intervals with all dbt gates passing')"
# An old task can be cleared after a later publication without moving the report clock backward.
airflow tasks clear commerce_batch --start-date 2024-01-01 --end-date 2024-01-01 --yes
airflow dags backfill commerce_batch --start-date 2024-01-01 --end-date 2024-01-01
/opt/commerce-venv/bin/python -c "from commerce.db import connect; c=connect(); assert c.execute('select count(*) n from analytics.fct_order_line').fetchone()['n']==54; assert str(c.execute('select as_of from meta.publications order by publication_id desc limit 1').fetchone()['as_of'])=='2024-01-02'; print('Historical retry preserved 54 facts and current reporting horizon')"
