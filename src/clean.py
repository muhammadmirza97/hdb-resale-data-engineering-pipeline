"""
Step 3 — Clean
==============
Reads raw_hdb_resale from DuckDB.
Applies deduplication and filters out rows with invalid critical values.
Creates a staging table: stg_hdb_resale.

This step deliberately does NOT add derived columns — it only removes
bad data. Feature engineering happens in the following transform step.
"""

import logging

from src.config_loader import load_config
from src.storage import get_connection

logger = logging.getLogger(__name__)


def clean() -> int:
    """
    Deduplicate and filter raw data.
    Creates stg_hdb_resale in DuckDB.
    Returns the number of clean staging rows.
    """
    config = load_config()
    raw_table = config["tables"]["raw_table"]
    stg_table = config["tables"]["stg_table"]

    conn = get_connection()

    # Count before cleaning
    raw_count = conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]

    conn.execute(f"""
        CREATE OR REPLACE TABLE {stg_table} AS
        SELECT DISTINCT *
        FROM {raw_table}
        WHERE
            resale_price   IS NOT NULL AND resale_price   > 0
            AND floor_area_sqm IS NOT NULL AND floor_area_sqm > 0
            AND month          IS NOT NULL
            AND storey_range   IS NOT NULL
            AND remaining_lease IS NOT NULL
            AND lease_commence_date IS NOT NULL
    """)

    stg_count = conn.execute(f"SELECT COUNT(*) FROM {stg_table}").fetchone()[0]
    conn.close()

    removed = raw_count - stg_count
    logger.info(f"Clean complete | raw={raw_count:,}  staging={stg_count:,}  removed={removed:,}")
    print(f"[CLEAN] Raw rows     : {raw_count:,}")
    print(f"[CLEAN] Removed      : {removed:,}  (duplicates + invalid)")
    print(f"[CLEAN] Staging rows : {stg_count:,}  → {stg_table}")

    return stg_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    clean()
