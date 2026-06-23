"""
Step 4 — Curate  (analytics tables)
=====================================
Creates three analytics-ready DuckDB tables from clean_hdb_resale
and exports each one to data/curated/ as a Parquet file.

Tables:
  monthly_town_prices  — aggregated by year, month, town
  flat_type_summary    — aggregated by flat type
  town_price_ranking   — towns ranked by median price per sqm
"""

import logging

import yaml

from src.paths import BASE_DIR, CURATED_DIR
from src.storage import get_connection, export_table_to_parquet

logger = logging.getLogger(__name__)

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def curate() -> dict[str, int]:
    """
    Build all curated analytics tables and export to Parquet.
    Returns a dict mapping table_name → row_count.
    """
    config = _load_config()
    clean_table = config["tables"]["clean_table"]

    conn = get_connection()
    CURATED_DIR.mkdir(parents=True, exist_ok=True)

    row_counts: dict[str, int] = {}

    # ── 1. Monthly town prices ────────────────────────────────────────────────
    conn.execute(f"""
        CREATE OR REPLACE TABLE monthly_town_prices AS
        SELECT
            sale_year,
            sale_month,
            town,
            COUNT(*)                       AS transaction_count,
            ROUND(AVG(resale_price), 0)    AS avg_resale_price,
            ROUND(MEDIAN(resale_price), 0) AS median_resale_price,
            ROUND(AVG(price_per_sqm), 2)   AS avg_price_per_sqm
        FROM {clean_table}
        GROUP BY sale_year, sale_month, town
        ORDER BY sale_year, sale_month, town
    """)
    n = export_table_to_parquet(conn, "monthly_town_prices", CURATED_DIR / "monthly_town_prices.parquet")
    row_counts["monthly_town_prices"] = n
    print(f"[CURATE] monthly_town_prices  : {n:,} rows → {CURATED_DIR / 'monthly_town_prices.parquet'}")

    # ── 2. Flat type summary ──────────────────────────────────────────────────
    conn.execute(f"""
        CREATE OR REPLACE TABLE flat_type_summary AS
        SELECT
            flat_type,
            COUNT(*)                        AS transaction_count,
            ROUND(AVG(resale_price), 0)     AS avg_resale_price,
            ROUND(MEDIAN(resale_price), 0)  AS median_resale_price,
            ROUND(AVG(floor_area_sqm), 1)   AS avg_floor_area_sqm,
            ROUND(AVG(price_per_sqm), 2)    AS avg_price_per_sqm,
            ROUND(MIN(resale_price), 0)     AS min_price,
            ROUND(MAX(resale_price), 0)     AS max_price
        FROM {clean_table}
        GROUP BY flat_type
        ORDER BY avg_resale_price
    """)
    n = export_table_to_parquet(conn, "flat_type_summary", CURATED_DIR / "flat_type_summary.parquet")
    row_counts["flat_type_summary"] = n
    print(f"[CURATE] flat_type_summary    : {n:,} rows → {CURATED_DIR / 'flat_type_summary.parquet'}")

    # ── 3. Town price ranking ─────────────────────────────────────────────────
    conn.execute(f"""
        CREATE OR REPLACE TABLE town_price_ranking AS
        SELECT
            town,
            COUNT(*)                                              AS transaction_count,
            ROUND(MEDIAN(price_per_sqm), 2)                      AS median_price_per_sqm,
            ROUND(AVG(price_per_sqm), 2)                         AS avg_price_per_sqm,
            RANK() OVER (ORDER BY MEDIAN(price_per_sqm) DESC)    AS price_rank
        FROM {clean_table}
        GROUP BY town
        ORDER BY price_rank
    """)
    n = export_table_to_parquet(conn, "town_price_ranking", CURATED_DIR / "town_price_ranking.parquet")
    row_counts["town_price_ranking"] = n
    print(f"[CURATE] town_price_ranking   : {n:,} rows → {CURATED_DIR / 'town_price_ranking.parquet'}")

    # Summary
    all_tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    logger.info(f"Curated tables complete. All DuckDB tables: {all_tables}")
    print(f"[CURATE] All DuckDB tables   : {all_tables}")

    conn.close()
    return row_counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    curate()
