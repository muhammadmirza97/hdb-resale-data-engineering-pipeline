"""
Step 4 — Transform  (ELT style)
================================
Reads from the staging table (stg_hdb_resale) created by clean.py.
Runs SQL feature engineering entirely inside DuckDB — no pandas needed.
Creates the fully-featured clean table: clean_hdb_resale.

Features derived (reused from Week 1 capstone logic):
  month               → sale_year, sale_month
  storey_range        → storey_min, storey_max, storey_mid
  remaining_lease     → remaining_lease_months
  lease_commence_date → flat_age  (sale_year − lease start year)
  resale_price /      → price_per_sqm
  floor_area_sqm

Output:
  DuckDB table : clean_hdb_resale
  Parquet file : data/processed/hdb_resale_clean.parquet
"""

import logging

from src.config_loader import load_config
from src.paths import CLEAN_PARQUET_PATH
from src.storage import get_connection, export_table_to_parquet

logger = logging.getLogger(__name__)

_TRANSFORM_SQL = """
CREATE OR REPLACE TABLE {clean_table} AS
SELECT
    -- original columns kept as-is
    month,
    town,
    flat_type,
    block,
    street_name,
    storey_range,
    floor_area_sqm,
    flat_model,
    lease_commence_date,
    remaining_lease,
    resale_price,

    -- date features
    YEAR(STRPTIME(month, '%Y-%m'))  AS sale_year,
    MONTH(STRPTIME(month, '%Y-%m')) AS sale_month,

    -- storey features: "10 TO 12" → 10, 12, 11.0
    TRY_CAST(split_part(TRIM(storey_range), ' TO ', 1) AS INTEGER)  AS storey_min,
    TRY_CAST(split_part(TRIM(storey_range), ' TO ', 2) AS INTEGER)  AS storey_max,
    (
        TRY_CAST(split_part(TRIM(storey_range), ' TO ', 1) AS INTEGER)
        + TRY_CAST(split_part(TRIM(storey_range), ' TO ', 2) AS INTEGER)
    ) / 2.0 AS storey_mid,

    -- remaining lease: "61 years 04 months" → total months
    (
        COALESCE(TRY_CAST(regexp_extract(remaining_lease, '(\\d+)\\s+years?',  1) AS INTEGER), 0) * 12
        + COALESCE(TRY_CAST(regexp_extract(remaining_lease, '(\\d+)\\s+months?', 1) AS INTEGER), 0)
    ) AS remaining_lease_months,

    -- flat age = sale year − year lease commenced
    YEAR(STRPTIME(month, '%Y-%m')) - TRY_CAST(lease_commence_date AS INTEGER) AS flat_age,

    -- price per square metre
    resale_price / floor_area_sqm AS price_per_sqm

FROM {stg_table}
"""


def transform() -> int:
    """
    Run SQL feature engineering on the staging table.
    Creates clean_hdb_resale and exports to Parquet.
    Returns the number of rows in the clean table.
    """
    config = load_config()
    stg_table = config["tables"]["stg_table"]
    clean_table = config["tables"]["clean_table"]

    conn = get_connection()

    sql = _TRANSFORM_SQL.format(stg_table=stg_table, clean_table=clean_table)
    logger.info(f"Running SQL transform: {stg_table} → {clean_table}")
    conn.execute(sql)

    row_count = conn.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    col_count = conn.execute(
        f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{clean_table}'"
    ).fetchone()[0]

    logger.info(f"'{clean_table}' created | rows={row_count:,}  cols={col_count}")
    print(f"[TRANSFORM] Clean table  : {clean_table}")
    print(f"[TRANSFORM] Rows         : {row_count:,}   Columns: {col_count}")

    export_table_to_parquet(conn, clean_table, CLEAN_PARQUET_PATH)
    print(f"[TRANSFORM] Parquet      : {CLEAN_PARQUET_PATH}")

    conn.close()
    return row_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    transform()
