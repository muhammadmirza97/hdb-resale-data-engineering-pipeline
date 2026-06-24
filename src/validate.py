"""
Step 2 — Validate
=================
Reads the raw staging table from DuckDB and runs data quality checks.

Checks performed:
  - Row count
  - Column count
  - Expected schema (column presence)
  - Missing values per column
  - Duplicate rows
  - Data types
  - Sanity checks on key numeric columns (resale_price, floor_area_sqm)

Saves a detailed report to reports/data_quality_report.csv.
Logs a WARNING for any FAIL-level finding, but does not halt the pipeline
so the operator can review the report and decide whether to proceed.
"""

import logging

import pandas as pd

from src.config_loader import load_config
from src.paths import QUALITY_REPORT_PATH
from src.storage import get_connection

logger = logging.getLogger(__name__)

DEFAULT_EXPECTED_COLS = [
    "month", "town", "flat_type", "block", "street_name",
    "storey_range", "floor_area_sqm", "flat_model",
    "lease_commence_date", "remaining_lease", "resale_price",
]


def validate() -> pd.DataFrame:
    """
    Run all quality checks against the raw DuckDB staging table.
    Returns the quality report as a DataFrame.
    """
    config = load_config()
    raw_table = config["tables"]["raw_table"]
    expected_cols = config.get("expected_columns", DEFAULT_EXPECTED_COLS)

    conn = get_connection()
    results: list[dict] = []

    def add(check: str, value, status: str) -> None:
        results.append({"Check": check, "Value": str(value), "Status": status})

    # 1. Row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    add("Row Count", f"{row_count:,}", "OK" if row_count > 0 else "FAIL")

    # 2. Column info
    col_info = conn.execute(
        f"SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = '{raw_table}'"
    ).fetchall()
    col_names = [r[0] for r in col_info]
    col_types = {r[0]: r[1] for r in col_info}
    add("Column Count", len(col_names), "OK")

    # 3. Expected columns present
    missing_cols = [c for c in expected_cols if c not in col_names]
    add(
        "Expected Columns",
        "All present" if not missing_cols else f"Missing: {missing_cols}",
        "FAIL" if missing_cols else "OK",
    )

    # 4. Duplicate rows
    total = conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    distinct = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {raw_table})"
    ).fetchone()[0]
    dupes = total - distinct
    add("Duplicate Rows", f"{dupes:,}", "WARN" if dupes > 0 else "OK")

    # 5. Missing values per column
    for col in col_names:
        null_count = conn.execute(
            f'SELECT COUNT(*) FROM {raw_table} WHERE "{col}" IS NULL'
        ).fetchone()[0]
        pct = round(null_count / max(row_count, 1) * 100, 2)
        add(
            f"Nulls [{col}]",
            f"{null_count:,} ({pct}%)",
            "WARN" if null_count > 0 else "OK",
        )

    # 6. Data types
    for col, dtype in col_types.items():
        add(f"Data Type [{col}]", dtype, "OK")

    # 7. Numeric sanity checks
    if "resale_price" in col_names:
        bad = conn.execute(
            f"SELECT COUNT(*) FROM {raw_table} WHERE resale_price <= 0 OR resale_price IS NULL"
        ).fetchone()[0]
        add("resale_price <= 0 or NULL", bad, "FAIL" if bad > 0 else "OK")

    if "floor_area_sqm" in col_names:
        bad = conn.execute(
            f"SELECT COUNT(*) FROM {raw_table} WHERE floor_area_sqm <= 0 OR floor_area_sqm IS NULL"
        ).fetchone()[0]
        add("floor_area_sqm <= 0 or NULL", bad, "FAIL" if bad > 0 else "OK")

    conn.close()

    # Save report
    report = pd.DataFrame(results)
    QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(QUALITY_REPORT_PATH, index=False)

    fail_count = int((report["Status"] == "FAIL").sum())
    warn_count = int((report["Status"] == "WARN").sum())
    ok_count   = int((report["Status"] == "OK").sum())

    logger.info(f"Validation complete | OK={ok_count}  WARN={warn_count}  FAIL={fail_count}")
    print(f"[VALIDATE] Report    : {QUALITY_REPORT_PATH}")
    print(f"[VALIDATE] OK={ok_count}  WARN={warn_count}  FAIL={fail_count}")

    if fail_count > 0:
        logger.warning(f"{fail_count} FAIL(s) found — review {QUALITY_REPORT_PATH}")

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    validate()
