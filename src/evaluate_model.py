"""
Step 8 — Evaluate Model
========================
Compares predicted vs actual resale prices from the predictions table.
Computes MAE, RMSE, R2, MAPE.
Saves results to:
  - reports/model_evaluation.csv  (appended — keeps history)
  - DuckDB table: model_evaluation
"""

import logging

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)

from src.config_loader import load_config
from src.paths import EVAL_REPORT_PATH
from src.storage import get_connection, table_exists

logger = logging.getLogger(__name__)


def evaluate() -> dict:
    """
    Evaluate predictions against actuals.
    Returns a metrics dict.
    """
    config = load_config()
    pred_table = config["tables"]["predictions_table"]
    eval_table = config["tables"]["evaluation_table"]

    conn = get_connection()

    if not table_exists(conn, pred_table):
        conn.close()
        raise RuntimeError(f"Table '{pred_table}' not found — run predict.py first.")

    df = conn.execute(
        f"SELECT resale_price, predicted_price FROM {pred_table}"
    ).df()

    y_true = df["resale_price"]
    y_pred = df["predicted_price"]

    metrics = {
        "evaluated_at": pd.Timestamp.now().isoformat(),
        "n_samples":    len(df),
        "mae":          round(float(mean_absolute_error(y_true, y_pred)), 2),
        "rmse":         round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 2),
        "r2":           round(float(r2_score(y_true, y_pred)), 4),
        "mape":         round(float(mean_absolute_percentage_error(y_true, y_pred)), 4),
        "median_abs_error": round(float((y_true - y_pred).abs().median()), 0),
    }

    results_df = pd.DataFrame([metrics])

    # Append to CSV (keep history across runs)
    EVAL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if EVAL_REPORT_PATH.exists():
        existing = pd.read_csv(EVAL_REPORT_PATH)
        results_df = pd.concat([existing, results_df], ignore_index=True)
    results_df.to_csv(EVAL_REPORT_PATH, index=False)

    # Upsert to DuckDB
    conn.execute(f"CREATE OR REPLACE TABLE {eval_table} AS SELECT * FROM results_df")
    conn.close()

    logger.info(f"Evaluation | MAE={metrics['mae']:,.0f}  RMSE={metrics['rmse']:,.0f}  R2={metrics['r2']:.4f}")
    print(f"[EVALUATE] MAE      : ${metrics['mae']:>12,.0f}")
    print(f"[EVALUATE] RMSE     : ${metrics['rmse']:>12,.0f}")
    print(f"[EVALUATE] R2       : {metrics['r2']:.4f}")
    print(f"[EVALUATE] MAPE     : {metrics['mape']:.2%}")
    print(f"[EVALUATE] Report   : {EVAL_REPORT_PATH}")

    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    evaluate()
