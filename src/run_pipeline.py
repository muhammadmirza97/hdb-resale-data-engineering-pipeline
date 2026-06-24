"""
Pipeline Entry Point
====================
Runs the full ELT + ML pipeline in order:

  0. Setup      -resolve and print all paths
  1. Ingest     -download CSV -> raw_hdb_resale (DuckDB)
  2. Validate   -data quality checks -> quality report
  3. Clean      -dedup + filter -> stg_hdb_resale (DuckDB)
  4. Transform  -SQL feature engineering -> clean_hdb_resale (DuckDB) + Parquet
  5. Curate     -analytics tables -> DuckDB + curated Parquet files
  6. Train      -train LinearRegression + RandomForest -> save best model
  7. Predict    -batch inference -> predictions table + Parquet
  8. Evaluate   -metrics vs actuals -> model_evaluation table + CSV
  9. Alerts     -anomaly detection -> alerts table + CSV

Usage:
  python -m src.run_pipeline
  python -m src.run_pipeline --skip-ml    (steps 1-5 only)
"""

import argparse
import io
import logging
import sys
import time

# Ensure stdout/stderr handle Unicode characters on Windows
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import yaml

from src.paths import BASE_DIR, LOG_PATH, ensure_dirs, print_paths
from src.ingest import ingest
from src.validate import validate
from src.clean import clean
from src.transform import transform
from src.curate import curate
from src.train_model import train
from src.predict import predict
from src.evaluate_model import evaluate
from src.alerts import check_alerts

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

DIVIDER = "-" * 58
TOTAL_STEPS = 9


def setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    level_str = cfg.get("logging", {}).get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s -%(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _step(number: int, title: str, total: int = TOTAL_STEPS) -> None:
    print(f"\n{DIVIDER}")
    print(f"  Step {number} of {total}: {title}")
    print(DIVIDER)


def run_pipeline(skip_ml: bool = False) -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    total = 5 if skip_ml else TOTAL_STEPS
    start = time.perf_counter()

    logger.info("=" * 58)
    logger.info("HDB Resale Data Engineering Pipeline -START")
    logger.info("=" * 58)

    # ── 0. Setup ──────────────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Setup - resolving project paths")
    print(DIVIDER)
    ensure_dirs()
    print_paths()
    logger.info("Directories verified")

    # ── 1. Ingest ──────────────────────────────────────────────────
    _step(1, "Ingest -download CSV -> raw_hdb_resale", total)
    ingest()
    logger.info("Step 1 complete: ingest")

    # ── 2. Validate ────────────────────────────────────────────────
    _step(2, "Validate -data quality checks", total)
    validate()
    logger.info("Step 2 complete: validate")

    # ── 3. Clean ───────────────────────────────────────────────────
    _step(3, "Clean -dedup + filter -> stg_hdb_resale", total)
    clean()
    logger.info("Step 3 complete: clean")

    # ── 4. Transform ───────────────────────────────────────────────
    _step(4, "Transform -feature engineering -> clean_hdb_resale", total)
    transform()
    logger.info("Step 4 complete: transform")

    # ── 5. Curate ──────────────────────────────────────────────────
    _step(5, "Curate -analytics tables", total)
    curate()
    logger.info("Step 5 complete: curate")

    if skip_ml:
        elapsed = time.perf_counter() - start
        _print_summary(elapsed, ml_skipped=True)
        logger.info(f"Pipeline (ELT only) complete in {elapsed:.1f}s")
        return

    # ── 6. Train ───────────────────────────────────────────────────
    _step(6, "Train -LinearRegression + RandomForest (MLflow)", total)
    train()
    logger.info("Step 6 complete: train")

    # ── 7. Predict ─────────────────────────────────────────────────
    _step(7, "Predict -batch inference on clean_hdb_resale", total)
    predict()
    logger.info("Step 7 complete: predict")

    # ── 8. Evaluate ────────────────────────────────────────────────
    _step(8, "Evaluate -metrics vs actuals", total)
    evaluate()
    logger.info("Step 8 complete: evaluate")

    # ── 9. Alerts ──────────────────────────────────────────────────
    _step(9, "Alerts -anomaly detection", total)
    check_alerts()
    logger.info("Step 9 complete: alerts")

    elapsed = time.perf_counter() - start
    _print_summary(elapsed, ml_skipped=False)
    logger.info(f"Pipeline complete in {elapsed:.1f}s")


def _print_summary(elapsed: float, ml_skipped: bool) -> None:
    print(f"\n{'=' * 58}")
    print(f"  Pipeline finished in {elapsed:.1f}s")
    print(f"  Outputs:")
    print(f"    warehouse/hdb_resale.duckdb         (all tables)")
    print(f"    data/raw/hdb_resale_raw.csv")
    print(f"    data/processed/hdb_resale_clean.parquet")
    print(f"    data/curated/                        (analytics Parquet)")
    print(f"    reports/data_quality_report.csv")
    if not ml_skipped:
        print(f"    models/hdb_resale_model.joblib")
        print(f"    data/predictions/hdb_resale_predictions.parquet")
        print(f"    reports/model_evaluation.csv")
        print(f"    reports/alerts.csv")
        print(f"    mlruns/                              (MLflow experiment)")
    print(f"    logs/pipeline.log")
    print(f"{'=' * 58}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HDB Resale ELT + ML Pipeline")
    parser.add_argument(
        "--skip-ml",
        action="store_true",
        help="Run only ELT steps (1-5), skip train/predict/evaluate/alerts",
    )
    args = parser.parse_args()
    run_pipeline(skip_ml=args.skip_ml)
