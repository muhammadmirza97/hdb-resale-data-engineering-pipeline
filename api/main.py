"""
HDB Resale Price Prediction API
================================
FastAPI app that serves the trained model via REST endpoints.

Endpoints:
  GET  /health                   — liveness check
  GET  /towns                    — list all distinct towns
  GET  /flat-types               — list all distinct flat types
  GET  /summary/town/{town}      — median price, count, avg floor area for a town
  POST /predict                  — predict resale price for a single listing

Run locally:
  uvicorn api.main:app --host 0.0.0.0 --port 8011 --reload

Docker:
  Defined in docker-compose.yml as hdb-api service (port 8011)
"""

import sys
from pathlib import Path

# Allow running from project root without installing the package
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.config_loader import load_config
from src.paths import MODEL_PATH
from src.storage import get_connection

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="HDB Resale Price Prediction API",
    description="Serves predictions from the trained RandomForest/LinearRegression model.",
    version="1.0.0",
)

# ── State: loaded once at startup ─────────────────────────────────────────────
_model = None


@app.on_event("startup")
def load_model() -> None:
    global _model
    if MODEL_PATH.exists():
        _model = joblib.load(MODEL_PATH)
    else:
        # API still starts; /predict returns 503 until model is trained
        _model = None


# ── Pydantic schemas ──────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    town: str = Field(..., example="JURONG WEST")
    flat_type: str = Field(..., example="4 ROOM")
    floor_area_sqm: float = Field(..., gt=0, example=90.0)
    storey_mid: float = Field(..., gt=0, example=8.0)
    sale_year: int = Field(..., ge=1990, le=2100, example=2024)
    sale_month: int = Field(..., ge=1, le=12, example=6)
    remaining_lease_months: int = Field(..., gt=0, example=900)
    flat_age: int = Field(..., ge=0, example=30)


class PredictResponse(BaseModel):
    predicted_price: float
    input: dict
    model_path: str


# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_config():
    return load_config()


def _get_conn():
    return get_connection()


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health", tags=["Meta"])
def health():
    return {
        "status": "ok",
        "model_loaded": _model is not None,
        "model_path": str(MODEL_PATH) if MODEL_PATH.exists() else None,
    }


@app.get("/towns", tags=["Reference Data"])
def get_towns():
    """Return all distinct town names from the clean table."""
    config = _get_config()
    clean_table = config["tables"]["clean_table"]
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT town FROM {clean_table} ORDER BY town"
        ).fetchall()
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=503, detail=f"Database error: {exc}")
    conn.close()
    return {"towns": [r[0] for r in rows]}


@app.get("/flat-types", tags=["Reference Data"])
def get_flat_types():
    """Return all distinct flat types from the clean table."""
    config = _get_config()
    clean_table = config["tables"]["clean_table"]
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"SELECT DISTINCT flat_type FROM {clean_table} ORDER BY flat_type"
        ).fetchall()
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=503, detail=f"Database error: {exc}")
    conn.close()
    return {"flat_types": [r[0] for r in rows]}


@app.get("/summary/town/{town}", tags=["Analytics"])
def get_town_summary(town: str):
    """Return key price statistics for a specific town."""
    config = _get_config()
    clean_table = config["tables"]["clean_table"]
    conn = _get_conn()
    try:
        row = conn.execute(f"""
            SELECT
                COUNT(*)                        AS transaction_count,
                ROUND(MEDIAN(resale_price), 0)  AS median_price,
                ROUND(AVG(resale_price), 0)     AS avg_price,
                ROUND(MIN(resale_price), 0)     AS min_price,
                ROUND(MAX(resale_price), 0)     AS max_price,
                ROUND(AVG(floor_area_sqm), 1)   AS avg_floor_area_sqm,
                ROUND(MEDIAN(price_per_sqm), 0) AS median_price_per_sqm
            FROM {clean_table}
            WHERE UPPER(town) = UPPER('{town}')
        """).fetchone()
    except Exception as exc:
        conn.close()
        raise HTTPException(status_code=503, detail=f"Database error: {exc}")
    conn.close()

    if row is None or row[0] == 0:
        raise HTTPException(status_code=404, detail=f"Town '{town}' not found.")

    return {
        "town": town.upper(),
        "transaction_count": row[0],
        "median_price": row[1],
        "avg_price": row[2],
        "min_price": row[3],
        "max_price": row[4],
        "avg_floor_area_sqm": row[5],
        "median_price_per_sqm": row[6],
    }


@app.post("/predict", response_model=PredictResponse, tags=["Predictions"])
def predict(req: PredictRequest):
    """Predict resale price for a single HDB flat listing."""
    if _model is None:
        raise HTTPException(
            status_code=503,
            detail="Model not loaded. Run 'python -m src.run_pipeline' first.",
        )

    config = _get_config()
    numeric_features: list = config["ml"]["features"]["numeric"]
    categorical_features: list = config["ml"]["features"]["categorical"]
    all_features = numeric_features + categorical_features

    input_data = {
        "town":                    req.town.upper(),
        "flat_type":               req.flat_type.upper(),
        "floor_area_sqm":          req.floor_area_sqm,
        "storey_mid":              req.storey_mid,
        "sale_year":               req.sale_year,
        "sale_month":              req.sale_month,
        "remaining_lease_months":  req.remaining_lease_months,
        "flat_age":                req.flat_age,
    }

    df = pd.DataFrame([input_data])[all_features]

    try:
        prediction = float(_model.predict(df)[0])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {exc}")

    return PredictResponse(
        predicted_price=round(prediction, 0),
        input=input_data,
        model_path=str(MODEL_PATH),
    )
