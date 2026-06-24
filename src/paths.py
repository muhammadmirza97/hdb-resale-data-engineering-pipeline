"""
Centralised path definitions — Day 1 lab style.

All other modules import from here so that file paths are never
hard-coded or duplicated across the codebase.

Usage:
    from src.paths import BASE_DIR, RAW_DIR, DB_PATH, ...
"""

from pathlib import Path


def find_project_base_dir() -> Path:
    """
    Walk up from this file's location until we find the project root.
    The project root is identified by the presence of config/config.yaml.
    Works correctly whether called from src/, the project root, or Docker.
    """
    current = Path(__file__).resolve().parent  # starts at src/
    for candidate in [current.parent, current, *current.parents]:
        if (candidate / "config" / "config.yaml").exists():
            return candidate
    # Fallback: treat the parent of src/ as the root
    return Path(__file__).resolve().parent.parent


# ── Root ──────────────────────────────────────────────────────────────────────
BASE_DIR: Path = find_project_base_dir()

# ── Data directories ──────────────────────────────────────────────────────────
DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
CURATED_DIR: Path = DATA_DIR / "curated"

# ── Infrastructure directories ────────────────────────────────────────────────
WAREHOUSE_DIR: Path = BASE_DIR / "warehouse"
REPORTS_DIR: Path = BASE_DIR / "reports"
LOGS_DIR: Path = BASE_DIR / "logs"

# ── Key file paths ────────────────────────────────────────────────────────────
DB_PATH: Path = WAREHOUSE_DIR / "hdb_resale.duckdb"
RAW_CSV_PATH: Path = RAW_DIR / "hdb_resale_raw.csv"
CLEAN_PARQUET_PATH: Path = PROCESSED_DIR / "hdb_resale_clean.parquet"
QUALITY_REPORT_PATH: Path = REPORTS_DIR / "data_quality_report.csv"
LOG_PATH: Path = LOGS_DIR / "pipeline.log"

# ── ML paths ──────────────────────────────────────────────────────────────────
MODELS_DIR: Path = BASE_DIR / "models"
PREDICTIONS_DIR: Path = DATA_DIR / "predictions"
MODEL_PATH: Path = MODELS_DIR / "hdb_resale_model.joblib"
EVAL_REPORT_PATH: Path = REPORTS_DIR / "model_evaluation.csv"
ALERTS_REPORT_PATH: Path = REPORTS_DIR / "alerts.csv"


def ensure_dirs() -> None:
    """Create all required directories if they do not already exist."""
    for directory in [RAW_DIR, PROCESSED_DIR, CURATED_DIR, PREDICTIONS_DIR,
                      WAREHOUSE_DIR, REPORTS_DIR, LOGS_DIR, MODELS_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def print_paths() -> None:
    """Print the resolved paths so the operator can verify the layout at startup."""
    width = 18
    print(f"{'BASE_DIR':<{width}}: {BASE_DIR}")
    print(f"{'RAW_DIR':<{width}}: {RAW_DIR}")
    print(f"{'PROCESSED_DIR':<{width}}: {PROCESSED_DIR}")
    print(f"{'CURATED_DIR':<{width}}: {CURATED_DIR}")
    print(f"{'WAREHOUSE_DIR':<{width}}: {WAREHOUSE_DIR}")
    print(f"{'REPORTS_DIR':<{width}}: {REPORTS_DIR}")
    print(f"{'LOGS_DIR':<{width}}: {LOGS_DIR}")
    print(f"{'DB_PATH':<{width}}: {DB_PATH}")
    print(f"{'RAW_CSV_PATH':<{width}}: {RAW_CSV_PATH}")
    print(f"{'CLEAN_PARQUET':<{width}}: {CLEAN_PARQUET_PATH}")
    print(f"{'MODELS_DIR':<{width}}: {MODELS_DIR}")
    print(f"{'PREDICTIONS_DIR':<{width}}: {PREDICTIONS_DIR}")
    print(f"{'MODEL_PATH':<{width}}: {MODEL_PATH}")


if __name__ == "__main__":
    ensure_dirs()
    print_paths()
