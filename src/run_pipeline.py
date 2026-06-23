"""
Pipeline Entry Point
====================
Runs the full ELT pipeline in order:

  1. Setup  — resolve and print all paths
  2. Ingest — download CSV → raw DuckDB table
  3. Validate — quality checks → quality report
  4. Transform — SQL transforms → clean DuckDB table + Parquet
  5. Curate — analytics tables → DuckDB + curated Parquet files

Usage:
  python -m src.run_pipeline
  python -m src.run_pipeline              (from the project root)
"""

import argparse
import logging
import sys
import time

import yaml

from src.paths import BASE_DIR, LOG_PATH, ensure_dirs, print_paths
from src.ingest import ingest
from src.validate import validate
from src.transform import transform
from src.curate import curate

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

DIVIDER = "─" * 54


def setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    level_str = cfg.get("logging", {}).get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _step(number: int, title: str) -> None:
    print(f"\n{DIVIDER}")
    print(f"  Step {number} of 5 — {title}")
    print(DIVIDER)


def run_pipeline() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    start = time.perf_counter()
    logger.info("=" * 54)
    logger.info("HDB Resale Data Pipeline — START")
    logger.info("=" * 54)

    # ── 0. Setup ──────────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  Setup — resolving project paths")
    print(DIVIDER)
    ensure_dirs()
    print_paths()
    logger.info("Directories verified")

    # ── 1. Ingest ─────────────────────────────────────────────
    _step(1, "Ingest")
    ingest()
    logger.info("Step 1 complete")

    # ── 2. Validate ───────────────────────────────────────────
    _step(2, "Validate")
    validate()
    logger.info("Step 2 complete")

    # ── 3. Transform ──────────────────────────────────────────
    _step(3, "Transform")
    transform()
    logger.info("Step 3 complete")

    # ── 4. Curate ─────────────────────────────────────────────
    _step(4, "Curate — analytics tables")
    curate()
    logger.info("Step 4 complete")

    # ── Summary ───────────────────────────────────────────────
    elapsed = time.perf_counter() - start
    print(f"\n{'=' * 54}")
    print(f"  Pipeline finished in {elapsed:.1f}s")
    print(f"  Outputs:")
    print(f"    data/raw/hdb_resale_raw.csv")
    print(f"    data/processed/hdb_resale_clean.parquet")
    print(f"    data/curated/  (3 Parquet files)")
    print(f"    warehouse/hdb_resale.duckdb")
    print(f"    reports/data_quality_report.csv")
    print(f"    logs/pipeline.log")
    print(f"{'=' * 54}\n")
    logger.info(f"Pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HDB Resale ELT Pipeline")
    parser.parse_args()  # allows --help; no flags needed for basic run
    run_pipeline()
