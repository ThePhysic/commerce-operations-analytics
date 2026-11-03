"""Clear inactive commerce intervals and release blocked Airflow run slots.

Run with the Airflow image's Python after pausing the DAG and restoring sources.
The pinned Airflow clear command leaves already-running DagRuns running even
when their tasks never started; those slots can prevent an older day recovering.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

import pendulum
from sqlalchemy import select

from airflow.jobs.job import Job
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, JobState, TaskInstanceState


def recover(start: date, end: date, reason: str) -> int:
    if start > end:
        raise ValueError("Provide a valid inclusive recovery range")
    if not reason.strip():
        raise ValueError("A nonempty audited recovery reason is required")
    beginning = pendulum.datetime(start.year, start.month, start.day, tz="UTC")
    next_day = end + timedelta(days=1)
    ending = pendulum.datetime(next_day.year, next_day.month, next_day.day, tz="UTC")
    dag = DagBag("/opt/airflow/dags", include_examples=False).dags["commerce_batch"]
    with create_session() as session:
        # Serialize the lease/state checks with metadata writes on the local
        # SQLite backend. Other supported backends lock the selected run rows.
        if session.get_bind().dialect.name == "sqlite":
            session.connection().exec_driver_sql("BEGIN IMMEDIATE")
        model = session.get(DagModel, dag.dag_id)
        if model is not None and not model.is_paused:
            raise RuntimeError("Pause commerce_batch before recovering its intervals")
        for job in session.scalars(select(Job).where(Job.state == JobState.RUNNING)):
            if not job.is_alive():
                continue
            if job.job_type == "BackfillJob" or (job.job_type == "LocalTaskJob" and job.dag_id == dag.dag_id):
                raise RuntimeError("Live task/backfill work exists; wait for it to stop before recovery")
            if model is None and job.job_type == "SchedulerJob":
                raise RuntimeError("Stop the scheduler or register and pause commerce_batch before recovery")
        runs = session.scalars(select(DagRun).where(
            DagRun.dag_id == dag.dag_id, DagRun.execution_date >= beginning,
            DagRun.execution_date < ending).with_for_update()).all()
        run_ids = [run.run_id for run in runs]
        if not runs:
            raise ValueError("No commerce intervals exist in the requested recovery range")
        active = session.scalars(select(TaskInstance).where(
            TaskInstance.dag_id == dag.dag_id, TaskInstance.run_id.in_(run_ids),
            TaskInstance.state.in_([TaskInstanceState.RUNNING, TaskInstanceState.QUEUED,
                                   TaskInstanceState.RESTARTING, TaskInstanceState.SCHEDULED]))).all()
        if active:
            raise RuntimeError("Selected intervals still have active tasks; let them stop before recovery")
        old_states = {run.run_id: run.state for run in runs}
        # Native clearing resets only finished DagRuns. Finish an inactive
        # blocked run first, then use that API to retain task-instance history.
        for run in runs:
            if run.state == DagRunState.RUNNING:
                run.set_state(DagRunState.FAILED)
        session.flush()
        dag.clear(start_date=beginning, end_date=ending - timedelta(microseconds=1),
                  include_subdags=False, include_parentdag=False, dag_run_state=DagRunState.QUEUED,
                  session=session)
        for run in runs:
            session.refresh(run)
        audit = {"action": "recover_intervals", "dag_id": dag.dag_id,
                 "at_utc": pendulum.now("UTC").isoformat(), "reason": reason.strip(),
                 "start": str(start), "end": str(end),
                 "runs": [{"run_id": run.run_id, "old_state": old_states[run.run_id],
                           "new_state": run.state} for run in runs]}
    print(json.dumps(audit))
    return len(runs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    recover(args.start, args.end, args.reason)
