"""
Airflow DAG — HDB Resale ELT Pipeline
======================================
Wraps each pipeline step as an Airflow PythonOperator task.

Task dependency chain:
  ingest_data >> validate_data >> transform_data >> create_curated_tables

Schedule: weekly (every Monday at 00:00 UTC)

To test locally (Airflow must be installed separately — see README):
  pip install apache-airflow
  export AIRFLOW_HOME=$(pwd)/airflow_home
  airflow db migrate
  airflow standalone
  # Then open http://localhost:8080 and trigger 'hdb_resale_pipeline'

Note: The heavy logic lives in src/ — the DAG just wires task execution.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the project root importable when Airflow loads this DAG
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ── Default arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
dag = DAG(
    dag_id="hdb_resale_pipeline",
    default_args=default_args,
    description="End-to-end ELT pipeline for Singapore HDB resale data",
    schedule_interval="@weekly",
    catchup=False,
    tags=["hdb", "data-engineering", "elt", "batch"],
)

# ── Task callables ────────────────────────────────────────────────────────────

def task_ingest(**context):
    from src.ingest import ingest
    ingest()


def task_validate(**context):
    from src.validate import validate
    validate()


def task_transform(**context):
    from src.transform import transform
    transform()


def task_curate(**context):
    from src.curate import curate
    curate()


# ── Operators ─────────────────────────────────────────────────────────────────

ingest_op = PythonOperator(
    task_id="ingest_data",
    python_callable=task_ingest,
    dag=dag,
)

validate_op = PythonOperator(
    task_id="validate_data",
    python_callable=task_validate,
    dag=dag,
)

transform_op = PythonOperator(
    task_id="transform_data",
    python_callable=task_transform,
    dag=dag,
)

curate_op = PythonOperator(
    task_id="create_curated_tables",
    python_callable=task_curate,
    dag=dag,
)

# ── Task dependencies ─────────────────────────────────────────────────────────
ingest_op >> validate_op >> transform_op >> curate_op
