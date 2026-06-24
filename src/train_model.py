"""
Step 6 — Train Model
====================
Reads clean_hdb_resale from DuckDB.
Trains two models (baseline Linear Regression + Random Forest).
Tracks experiments with MLflow (local file-based — no server needed).
Saves the best model to models/hdb_resale_model.joblib.
Saves evaluation results to reports/model_evaluation.csv and DuckDB.

Features reused from Week 1 capstone:
  Numeric : floor_area_sqm, storey_mid, sale_year, sale_month,
            remaining_lease_months, flat_age
  Categorical: town, flat_type  (OneHotEncoded)
  Target  : resale_price
"""

import logging
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config_loader import load_config
from src.paths import BASE_DIR, EVAL_REPORT_PATH, MODEL_PATH, MODELS_DIR
from src.storage import get_connection

logger = logging.getLogger(__name__)


def _build_preprocessor(numeric_features: list, categorical_features: list) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
        ]
    )


def train() -> list[dict]:
    """
    Train Linear Regression and Random Forest models.
    Returns a list of evaluation metric dicts for each model.
    """
    config = load_config()
    clean_table = config["tables"]["clean_table"]
    ml_cfg = config["ml"]

    numeric_features: list = ml_cfg["features"]["numeric"]
    categorical_features: list = ml_cfg["features"]["categorical"]
    all_features = numeric_features + categorical_features
    target = ml_cfg["target"]
    test_size = ml_cfg["test_size"]
    random_state = ml_cfg["random_state"]
    n_estimators = ml_cfg["n_estimators"]
    max_depth = ml_cfg["max_depth"]

    # Load data
    conn = get_connection()
    df = conn.execute(f"SELECT {', '.join(all_features + [target])} FROM {clean_table}").df()
    conn.close()

    df = df.dropna(subset=all_features + [target])
    X = df[all_features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    logger.info(f"Train={len(X_train):,}  Test={len(X_test):,}")
    print(f"[TRAIN] Train samples: {len(X_train):,}   Test samples: {len(X_test):,}")

    preprocessor = _build_preprocessor(numeric_features, categorical_features)

    models_to_train = {
        "LinearRegression": Pipeline([
            ("pre", preprocessor),
            ("model", LinearRegression()),
        ]),
        "RandomForest": Pipeline([
            ("pre", preprocessor),
            ("model", RandomForestRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
                n_jobs=-1,
            )),
        ]),
    }

    # MLflow — local SQLite tracking (no server required)
    # MLflow 2.x+ recommends sqlite:// over file:// for local tracking
    mlruns_dir = BASE_DIR / "mlruns"
    mlruns_dir.mkdir(parents=True, exist_ok=True)
    mlflow_db = mlruns_dir / "mlflow.db"
    mlflow.set_tracking_uri(f"sqlite:///{mlflow_db}")
    mlflow.set_experiment("hdb-resale-price-prediction")

    all_results: list[dict] = []
    best_pipeline = None
    best_r2 = float("-inf")

    for name, pipeline in models_to_train.items():
        with mlflow.start_run(run_name=name):
            pipeline.fit(X_train, y_train)
            y_pred = pipeline.predict(X_test)

            mae  = mean_absolute_error(y_test, y_pred)
            rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
            r2   = r2_score(y_test, y_pred)
            mape = mean_absolute_percentage_error(y_test, y_pred)

            mlflow.log_params({
                "model_type":   name,
                "features":     str(all_features),
                "n_train":      len(X_train),
                "n_test":       len(X_test),
                "test_size":    test_size,
            })
            mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2, "mape": mape})
            mlflow.sklearn.log_model(pipeline, "model")

            result = {
                "model": name,
                "mae": round(mae, 2),
                "rmse": round(rmse, 2),
                "r2": round(r2, 4),
                "mape": round(mape, 4),
                "n_train": len(X_train),
                "n_test": len(X_test),
            }
            all_results.append(result)

            logger.info(f"{name} | MAE={mae:,.0f}  RMSE={rmse:,.0f}  R2={r2:.4f}")
            print(f"[TRAIN] {name:<20} MAE={mae:>12,.0f}  RMSE={rmse:>12,.0f}  R2={r2:.4f}")

            if r2 > best_r2:
                best_r2 = r2
                best_pipeline = pipeline
                best_name = name

    # Save best model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_pipeline, MODEL_PATH)
    logger.info(f"Best model: {best_name} (R2={best_r2:.4f}) saved → {MODEL_PATH}")
    print(f"[TRAIN] Best model   : {best_name}  (R2={best_r2:.4f})")
    print(f"[TRAIN] Model saved  : {MODEL_PATH}")

    # Save evaluation CSV
    results_df = pd.DataFrame(all_results)
    EVAL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(EVAL_REPORT_PATH, index=False)

    # Save to DuckDB
    conn = get_connection()
    conn.execute(f"""
        CREATE OR REPLACE TABLE {config['tables']['evaluation_table']} AS
        SELECT * FROM results_df
    """)
    conn.close()

    print(f"[TRAIN] Evaluation   : {EVAL_REPORT_PATH}")
    return all_results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    train()
