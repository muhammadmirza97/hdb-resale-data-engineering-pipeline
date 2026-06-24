"""
Airflow DAG — HDB Resale Data Engineering + ML Pipeline
=========================================================
Wraps every pipeline step as a PythonOperator task.

Task dependency chain (8 tasks):
  ingest_data
    >> validate_data
    >> clean_transform_data
    >> create_curated_tables
    >> train_model
    >> generate_predictions
    >> evaluate_model
    >> check_alerts

Schedule: daily at midnight UTC
  - Most tasks run at midnight (@daily)
  - evaluate_model is separately scheduled at 08:00 UTC via its own DAG
    (or via the `evaluation_schedule` cron in config.yaml)

To run locally (Airflow must be installed separately):
  pip install -r requirements-airflow.txt
  export AIRFLOW_HOME=$(pwd)/airflow_home
  airflow db migrate
  airflow standalone          # opens http://localhost:8080
  # Trigger 'hdb_resale_pipeline' manually or wait for schedule

Note: All heavy logic lives in src/ — the DAG just wires task execution.
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
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
dag = DAG(
    dag_id="hdb_resale_pipeline",
    default_args=default_args,
    description=(
        "End-to-end HDB resale data engineering + ML pipeline: "
        "ingest → validate → clean → transform → curate → train → predict → evaluate → alerts"
    ),
    schedule_interval="@daily",
    catchup=False,
    tags=["hdb", "data-engineering", "elt", "ml", "batch"],
)

# ── Task callables ────────────────────────────────────────────────────────────
def task_ingest(**context):
    from src.ingest import ingest
    ingest()


def task_validate(**context):
    from src.validate import validate
    validate()


def task_clean_transform(**context):
    from src.clean import clean
    from src.transform import transform
    clean()
    transform()


def task_curate(**context):
    from src.curate import curate
    curate()


def task_train(**context):
    from src.train_model import train
    train()


def task_predict(**context):
    from src.predict import predict
    predict()


def task_evaluate(**context):
    from src.evaluate_model import evaluate
    evaluate()


def task_alerts(**context):
    from src.alerts import check_alerts
    check_alerts()


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

clean_transform_op = PythonOperator(
    task_id="clean_transform_data",
    python_callable=task_clean_transform,
    dag=dag,
)

curate_op = PythonOperator(
    task_id="create_curated_tables",
    python_callable=task_curate,
    dag=dag,
)

train_op = PythonOperator(
    task_id="train_model",
    python_callable=task_train,
    dag=dag,
)

predict_op = PythonOperator(
    task_id="generate_predictions",
    python_callable=task_predict,
    dag=dag,
)

evaluate_op = PythonOperator(
    task_id="evaluate_model",
    python_callable=task_evaluate,
    dag=dag,
)

alerts_op = PythonOperator(
    task_id="check_alerts",
    python_callable=task_alerts,
    dag=dag,
)

# ── Task dependency chain ─────────────────────────────────────────────────────
(
    ingest_op
    >> validate_op
    >> clean_transform_op
    >> curate_op
    >> train_op
    >> predict_op
    >> evaluate_op
    >> alerts_op
)
