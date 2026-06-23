"""
Step 3 — Transform  (ELT style)
================================
All transformations run as DuckDB SQL against the raw staging table.
No pandas used here — data never leaves the database engine.

Transformations applied (reused from Week 1 capstone logic):
  ┌─────────────────────────────────────────────────────────────────┐
  │  Raw column          → Derived column(s)                        │
  │  month               → sale_year, sale_month                    │
  │  storey_range        → storey_min, storey_max, storey_mid       │
  │  remaining_lease     → remaining_lease_months                   │
  │  lease_commence_date → flat_age  (sale_year - start year)       │
  │  resale_price /      → price_per_sqm                            │
  │  floor_area_sqm                                                 │
  └─────────────────────────────────────────────────────────────────┘

Output:
  DuckDB table : clean_hdb_resale
  Parquet file : data/processed/hdb_resale_clean.parquet
"""

import logging

import yaml

from src.paths import BASE_DIR, CLEAN_PARQUET_PATH
from src.storage import get_connection, export_table_to_parquet

logger = logging.getLogger(__name__)

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── SQL transformation ────────────────────────────────────────────────────────
# DuckDB handles all parsing in-database:
#   STRPTIME(month, '%Y-%m')            → date for year/month extraction
#   split_part(storey_range, ' TO ', n) → storey floor numbers
#   regexp_extract(remaining_lease, …)  → years and months from text
#   QUALIFY ROW_NUMBER() OVER (…) = 1  → deduplication (DuckDB window function)

_TRANSFORM_SQL = """
CREATE OR REPLACE TABLE {clean_table} AS
WITH
-- Step 1: deduplicate (keep first occurrence of every identical row)
deduped AS (
    SELECT *
    FROM {raw_table}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            month, town, flat_type, block, street_name,
            storey_range, floor_area_sqm, flat_model,
            lease_commence_date, remaining_lease, resale_price
        ORDER BY (SELECT NULL)
    ) = 1
),

-- Step 2: filter out rows where critical numeric columns are invalid
filtered AS (
    SELECT *
    FROM deduped
    WHERE
        resale_price   IS NOT NULL AND resale_price   > 0
        AND floor_area_sqm IS NOT NULL AND floor_area_sqm > 0
        AND month          IS NOT NULL
        AND storey_range   IS NOT NULL
        AND remaining_lease IS NOT NULL
),

-- Step 3: parse and derive all new columns
parsed AS (
    SELECT
        -- original columns
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

        -- date features: month → year + month number
        YEAR(STRPTIME(month, '%Y-%m'))  AS sale_year,
        MONTH(STRPTIME(month, '%Y-%m')) AS sale_month,

        -- storey features: "10 TO 12" → 10, 12, 11.0
        TRY_CAST(split_part(TRIM(storey_range), ' TO ', 1) AS INTEGER)  AS storey_min,
        TRY_CAST(split_part(TRIM(storey_range), ' TO ', 2) AS INTEGER)  AS storey_max,
        (
            TRY_CAST(split_part(TRIM(storey_range), ' TO ', 1) AS INTEGER)
            + TRY_CAST(split_part(TRIM(storey_range), ' TO ', 2) AS INTEGER)
        ) / 2.0 AS storey_mid,

        -- lease: "61 years 04 months" → total months
        (
            COALESCE(
                TRY_CAST(regexp_extract(remaining_lease, '(\\d+)\\s+years?', 1) AS INTEGER),
                0
            ) * 12
            +
            COALESCE(
                TRY_CAST(regexp_extract(remaining_lease, '(\\d+)\\s+months?', 1) AS INTEGER),
                0
            )
        ) AS remaining_lease_months,

        -- flat age = sale year − year lease started
        YEAR(STRPTIME(month, '%Y-%m')) - TRY_CAST(lease_commence_date AS INTEGER) AS flat_age,

        -- price density
        resale_price / floor_area_sqm AS price_per_sqm

    FROM filtered
)
SELECT * FROM parsed
"""


def transform() -> int:
    """
    Run SQL transformation and export clean table to Parquet.
    Returns the number of rows in the clean table.
    """
    config = _load_config()
    raw_table = config["tables"]["raw_table"]
    clean_table = config["tables"]["clean_table"]

    conn = get_connection()

    # Run the transformation
    sql = _TRANSFORM_SQL.format(raw_table=raw_table, clean_table=clean_table)
    logger.info(f"Running SQL transformation: {raw_table} → {clean_table}")
    conn.execute(sql)

    # Count results
    row_count = conn.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    col_count = conn.execute(
        f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{clean_table}'"
    ).fetchone()[0]

    logger.info(f"Clean table '{clean_table}' created | rows={row_count:,}  cols={col_count}")
    print(f"[TRANSFORM] Clean table  : {clean_table}")
    print(f"[TRANSFORM] Rows         : {row_count:,}")
    print(f"[TRANSFORM] Columns      : {col_count}")

    # Export to Parquet
    export_table_to_parquet(conn, clean_table, CLEAN_PARQUET_PATH)
    print(f"[TRANSFORM] Parquet file : {CLEAN_PARQUET_PATH}")

    conn.close()
    return row_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    transform()
