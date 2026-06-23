"""
Step 1 — Ingest
===============
ELT Extract step:
  1. Read the HDB resale CSV from a GitHub URL or a local file (config-driven).
  2. Save an unchanged raw copy to data/raw/hdb_resale_raw.csv.
  3. Load the raw CSV into DuckDB staging table  `raw_hdb_resale`.

Downstream steps (validate, transform) all read from DuckDB,
not from the CSV, so this is the only place that touches the raw file.
"""

import logging
import os

import pandas as pd
import yaml

from src.paths import RAW_CSV_PATH, BASE_DIR
from src.storage import get_connection

logger = logging.getLogger(__name__)

CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _read_source(config: dict) -> pd.DataFrame:
    """Download from URL or read from local path, based on config."""
    source_type = config["data_source"].get("type", "url")
    csv_url = config["data_source"].get("csv_url", "")
    local_path = config["data_source"].get("local_csv_path", str(RAW_CSV_PATH))

    if source_type == "url" and csv_url:
        logger.info(f"Downloading CSV from URL: {csv_url}")
        print(f"[INGEST] Source      : URL")
        print(f"[INGEST] Downloading : {csv_url}")
        return pd.read_csv(csv_url)

    if os.path.exists(local_path):
        logger.info(f"Reading CSV from local path: {local_path}")
        print(f"[INGEST] Source      : local file")
        print(f"[INGEST] Reading     : {local_path}")
        return pd.read_csv(local_path)

    raise FileNotFoundError(
        f"No valid data source found. "
        f"Set csv_url in config.yaml or place a file at: {local_path}"
    )


def ingest() -> int:
    """
    Full ingest step.
    Returns the number of rows loaded into the DuckDB staging table.
    """
    config = _load_config()
    raw_table = config["tables"]["raw_table"]

    # 1. Read source
    df = _read_source(config)
    rows, cols = df.shape
    logger.info(f"Raw data loaded | rows={rows:,}  cols={cols}")
    print(f"[INGEST] Rows loaded : {rows:,}")
    print(f"[INGEST] Columns     : {cols}")

    # 2. Save unchanged raw copy
    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_CSV_PATH, index=False)
    logger.info(f"Raw CSV saved → {RAW_CSV_PATH}")
    print(f"[INGEST] Raw CSV     : {RAW_CSV_PATH}")

    # 3. Load into DuckDB staging table
    conn = get_connection()
    conn.execute(f"CREATE OR REPLACE TABLE {raw_table} AS SELECT * FROM df")
    staged_rows = conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    conn.close()

    logger.info(f"Staged {staged_rows:,} rows into DuckDB table '{raw_table}'")
    print(f"[INGEST] DuckDB table: {raw_table}  ({staged_rows:,} rows)")

    return staged_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ingest()
