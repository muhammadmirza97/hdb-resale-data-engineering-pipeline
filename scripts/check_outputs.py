"""
Check Outputs
=============
Verifies that all expected pipeline output files and DuckDB tables exist.
Run this after `python -m src.run_pipeline` to confirm everything was produced.

Usage:
  python scripts/check_outputs.py
  python scripts/check_outputs.py --skip-ml
"""

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.paths import BASE_DIR
from src.storage import get_connection, table_exists

GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"
BOLD  = "\033[1m"


def check(label: str, condition: bool) -> bool:
    status = f"{GREEN}OK{RESET}" if condition else f"{RED}MISSING{RESET}"
    print(f"  {'[OK]    ' if condition else '[MISSING]'} {label}")
    return condition


def run_checks(skip_ml: bool = False) -> int:
    failures = 0
    print(f"\n{BOLD}=== Output File Checks ==={RESET}")

    file_checks = [
        ("data/raw/hdb_resale_raw.csv",           BASE_DIR / "data/raw/hdb_resale_raw.csv"),
        ("data/processed/hdb_resale_clean.parquet", BASE_DIR / "data/processed/hdb_resale_clean.parquet"),
        ("data/curated/monthly_town_prices.parquet", BASE_DIR / "data/curated/monthly_town_prices.parquet"),
        ("data/curated/flat_type_summary.parquet",   BASE_DIR / "data/curated/flat_type_summary.parquet"),
        ("data/curated/town_price_ranking.parquet",  BASE_DIR / "data/curated/town_price_ranking.parquet"),
        ("reports/data_quality_report.csv",          BASE_DIR / "reports/data_quality_report.csv"),
        ("logs/pipeline.log",                        BASE_DIR / "logs/pipeline.log"),
        ("warehouse/hdb_resale.duckdb",              BASE_DIR / "warehouse/hdb_resale.duckdb"),
    ]

    if not skip_ml:
        file_checks += [
            ("models/hdb_resale_model.joblib",               BASE_DIR / "models/hdb_resale_model.joblib"),
            ("data/predictions/hdb_resale_predictions.parquet", BASE_DIR / "data/predictions/hdb_resale_predictions.parquet"),
            ("reports/model_evaluation.csv",                 BASE_DIR / "reports/model_evaluation.csv"),
            ("reports/alerts.csv",                           BASE_DIR / "reports/alerts.csv"),
        ]

    for label, path in file_checks:
        if not check(label, path.exists()):
            failures += 1

    print(f"\n{BOLD}=== DuckDB Table Checks ==={RESET}")
    try:
        conn = get_connection()
        tables_to_check = [
            "raw_hdb_resale",
            "stg_hdb_resale",
            "clean_hdb_resale",
            "monthly_town_prices",
            "flat_type_summary",
            "town_price_ranking",
        ]
        if not skip_ml:
            tables_to_check += ["predictions", "model_evaluation", "alerts"]

        for t in tables_to_check:
            exists = table_exists(conn, t)
            if not check(f"DuckDB table: {t}", exists):
                failures += 1

        if table_exists(conn, "clean_hdb_resale"):
            row_count = conn.execute("SELECT COUNT(*) FROM clean_hdb_resale").fetchone()[0]
            print(f"\n  clean_hdb_resale rows: {row_count:,}")

        if not skip_ml and table_exists(conn, "predictions"):
            pred_count = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
            print(f"  predictions rows     : {pred_count:,}")

        conn.close()
    except Exception as e:
        print(f"  {RED}DuckDB error: {e}{RESET}")
        failures += 1

    print(f"\n{BOLD}=== Summary ==={RESET}")
    if failures == 0:
        print(f"  {GREEN}All checks passed!{RESET}")
    else:
        print(f"  {RED}{failures} check(s) failed.{RESET} Run 'python -m src.run_pipeline' to generate missing outputs.")

    return failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ml", action="store_true")
    args = parser.parse_args()
    sys.exit(run_checks(skip_ml=args.skip_ml))
