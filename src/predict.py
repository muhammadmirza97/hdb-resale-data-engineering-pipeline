"""
Step 7 — Generate Predictions
==============================
Loads the trained model from models/hdb_resale_model.joblib.
Runs batch inference on the full clean_hdb_resale table.
Saves results to:
  - DuckDB table: predictions
  - Parquet file: data/predictions/hdb_resale_predictions.parquet

Each prediction row includes the actual resale_price so the evaluate
step can compare predicted vs actual directly.
"""

import logging
from pathlib import Path

import joblib
import pandas as pd

from src.config_loader import load_config
from src.paths import MODEL_PATH, PREDICTIONS_DIR
from src.storage import get_connection, export_table_to_parquet

logger = logging.getLogger(__name__)


def predict() -> int:
    """
    Run batch inference on clean_hdb_resale.
    Returns the number of predictions generated.
    """
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. Run train_model.py first."
        )

    config = load_config()
    clean_table = config["tables"]["clean_table"]
    pred_table = config["tables"]["predictions_table"]
    ml_cfg = config["ml"]

    numeric_features: list = ml_cfg["features"]["numeric"]
    categorical_features: list = ml_cfg["features"]["categorical"]
    all_features = numeric_features + categorical_features
    target = ml_cfg["target"]

    model = joblib.load(MODEL_PATH)
    logger.info(f"Model loaded from {MODEL_PATH}")

    conn = get_connection()
    cols_needed = ["month", "town", "flat_type", "floor_area_sqm",
                   "storey_mid", "sale_year", "sale_month",
                   "remaining_lease_months", "flat_age", "resale_price"]
    df = conn.execute(f"SELECT {', '.join(cols_needed)} FROM {clean_table}").df()

    df = df.dropna(subset=all_features + [target])
    X = df[all_features]

    df["predicted_price"] = model.predict(X).round(0)
    df["prediction_error"] = (df["predicted_price"] - df[target]).round(0)
    df["abs_error"] = df["prediction_error"].abs()
    df["pct_error"] = (df["abs_error"] / df[target] * 100).round(2)
    df["predicted_at"] = pd.Timestamp.now().isoformat()

    predictions_df = df[[
        "month", "town", "flat_type", "floor_area_sqm", "storey_mid",
        "sale_year", "sale_month", "remaining_lease_months", "flat_age",
        "resale_price", "predicted_price", "prediction_error",
        "abs_error", "pct_error", "predicted_at",
    ]]

    conn.execute(f"CREATE OR REPLACE TABLE {pred_table} AS SELECT * FROM predictions_df")

    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PREDICTIONS_DIR / "hdb_resale_predictions.parquet"
    export_table_to_parquet(conn, pred_table, parquet_path)
    conn.close()

    n = len(predictions_df)
    logger.info(f"Predictions saved | rows={n:,}  table={pred_table}")
    print(f"[PREDICT] Predictions: {n:,} rows → {pred_table}")
    print(f"[PREDICT] Parquet    : {parquet_path}")
    return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    predict()
