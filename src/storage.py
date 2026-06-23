"""
DuckDB connection helpers.

All modules that need a database connection call get_connection() from here
so the DB path is never duplicated.
"""

import logging
from pathlib import Path

import duckdb

from src.paths import DB_PATH

logger = logging.getLogger(__name__)


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return an open DuckDB connection to the project warehouse."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    logger.debug(f"DuckDB connected: {db_path}")
    return conn


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Return True if a table exists in the connected DuckDB database."""
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


def export_table_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Path,
) -> int:
    """
    Export a DuckDB table to a Parquet file.
    Returns the number of rows exported.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Exported {table_name} → {output_path} ({row_count:,} rows)")
    return row_count


def list_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Return a list of all table names in the connected database."""
    rows = conn.execute("SHOW TABLES").fetchall()
    return [row[0] for row in rows]
