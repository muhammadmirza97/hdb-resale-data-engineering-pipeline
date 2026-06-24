# Project Map — HDB Resale Data Engineering Pipeline

This document is a guided tour of the entire repository.
It explains what every folder and file does, how the project runs from start to finish,
and where to look when you want to make changes.

No code knowledge is required to read this document.

---

## Project Overview

This project takes raw Singapore HDB resale transaction data and runs it through
a structured, automated sequence of steps — a **data pipeline** — that cleans,
validates, and organises the data into a form that analysts or machine learning
models can use directly.

It was built as the **Week 2 SNAIC Capstone**, extending the Week 1 ML notebook
into a proper **data engineering** project.

**Design pattern used: ELT (Extract, Load, Transform)**

Traditional pipelines transform data first, then load it.
This project does it differently:
1. **Extract** — download the raw CSV
2. **Load** — put it straight into a database (DuckDB) without changing it
3. **Transform** — run SQL queries inside the database to clean and reshape the data

This is the modern data engineering approach — the database engine does the heavy
lifting, not Python code.

---

## Folder Structure

```
hdb-resale-data-engineering-pipeline/
│
├── config/                  ← Settings and configuration
├── data/
│   ├── raw/                 ← Original downloaded file (untouched)
│   ├── processed/           ← Cleaned data in Parquet format
│   └── curated/             ← Pre-aggregated analytics tables as Parquet
├── dags/                    ← Airflow scheduling definition
├── logs/                    ← Pipeline run history
├── notebooks/               ← Original Week 1 ML notebook (reference only)
├── reports/                 ← Data quality check results
├── scripts/                 ← One-time utility scripts
├── src/                     ← All pipeline Python code lives here
└── warehouse/               ← DuckDB database file (the SQL engine's storage)
```

### What each top-level folder is for

| Folder | Purpose | Gets created by |
|---|---|---|
| `config/` | Stores `config.yaml` — the single place to change URLs, table names, paths | Already exists |
| `data/raw/` | Stores the downloaded CSV exactly as received — never modified | `ingest.py` |
| `data/processed/` | Stores the cleaned, feature-engineered dataset as a Parquet file | `transform.py` |
| `data/curated/` | Stores three pre-built analytics tables as Parquet files | `curate.py` |
| `warehouse/` | Holds `hdb_resale.duckdb` — the database file containing all SQL tables | `storage.py` |
| `reports/` | Holds `data_quality_report.csv` — the output of the validation step | `validate.py` |
| `logs/` | Holds `pipeline.log` — a timestamped record of every pipeline run | `run_pipeline.py` |
| `notebooks/` | The original Week 1 ML notebook — kept for reference, not used by the pipeline | Copied at setup |
| `dags/` | The Airflow DAG file — used only if you install Airflow separately | Already exists |
| `scripts/` | One-time helpers (e.g. creating folder structure from scratch) | Already exists |

> **Note on generated folders:** `data/raw/`, `data/processed/`, `data/curated/`,
> `warehouse/`, `reports/`, and `logs/` are all auto-created by the pipeline
> at runtime if they do not exist. You never need to create them manually.

---

## Important Files

### The control centre

| File | One-line description |
|---|---|
| `config/config.yaml` | **The settings file.** Controls where data comes from, what tables are named, and where logs go. This is the first file to check when something needs to change. |
| `src/paths.py` | **The address book.** Defines every file path used across the project using Python's `pathlib`. All other modules import paths from here — nothing is hard-coded elsewhere. |

### The pipeline steps (in execution order)

| File | Step | What it does |
|---|---|---|
| `src/ingest.py` | Step 1 | Downloads the CSV from GitHub (or reads a local file). Saves an unchanged copy to `data/raw/`. Loads the raw data into DuckDB as a staging table called `raw_hdb_resale`. |
| `src/validate.py` | Step 2 | Reads `raw_hdb_resale` from DuckDB. Runs quality checks: row count, column names, null values, duplicates, data types, numeric sanity. Saves a detailed report to `reports/data_quality_report.csv`. |
| `src/transform.py` | Step 3 | Runs a single large SQL query inside DuckDB. Deduplicates rows, filters bad data, and creates new computed columns (`sale_year`, `storey_mid`, `remaining_lease_months`, `flat_age`, `price_per_sqm`). Saves the clean table as `data/processed/hdb_resale_clean.parquet`. |
| `src/curate.py` | Step 4 | Creates three analytics-ready summary tables in DuckDB (`monthly_town_prices`, `flat_type_summary`, `town_price_ranking`). Exports each as a Parquet file to `data/curated/`. |

### The helpers

| File | What it does |
|---|---|
| `src/storage.py` | Contains helper functions for working with DuckDB: opening a connection (`get_connection()`), checking if a table exists (`table_exists()`), and exporting any table to Parquet (`export_table_to_parquet()`). |
| `src/run_pipeline.py` | **The main entry point.** Calls all four steps in order. Sets up logging. Prints a summary at the end. Run with `python -m src.run_pipeline`. |
| `src/__init__.py` | An empty file that tells Python the `src/` folder is a module. Required for `python -m src.run_pipeline` to work. Do not delete. |

### Infrastructure files

| File | What it does |
|---|---|
| `Dockerfile` | Instructions for building a Docker container image. Uses Python 3.11 slim. Installs requirements, copies the project, and sets the default command to run the pipeline. |
| `docker-compose.yml` | A one-command way to run the pipeline in Docker. Mounts `data/`, `warehouse/`, `reports/`, `logs/` back to your local machine so results are saved after the container stops. |
| `requirements.txt` | Lists the Python packages needed: `pandas`, `duckdb`, `pyarrow`, `pyyaml`, `requests`. Airflow is commented out as optional. |
| `.gitignore` | Tells git which files to skip. Large generated files (CSVs, Parquet, DuckDB, logs) are excluded so they don't get committed. |
| `dags/hdb_resale_pipeline_dag.py` | An Airflow DAG that wraps the four pipeline steps as scheduled tasks. Only relevant if you install Airflow. The pipeline runs fine without it. |
| `scripts/setup_project.py` | A one-time helper that creates all empty folders with `.gitkeep` files. Only needed on a fresh clone if folders are missing. |
| `notebooks/Capstone_1_...ipynb` | The original Week 1 ML notebook. Kept for reference. Not connected to the pipeline. |

---

## How the Project Runs

### The entry point

The single command that runs everything:

```bash
python -m src.run_pipeline
```

This must be run from the **project root folder** (the folder that contains `config/`, `src/`, etc.).

### Execution order

```
python -m src.run_pipeline
        │
        ├── 0. Setup
        │       src/paths.py        → find the project root, create all folders, print paths
        │
        ├── 1. Ingest               src/ingest.py
        │       ↓ reads config.yaml
        │       ↓ downloads CSV from GitHub URL
        │       ↓ saves data/raw/hdb_resale_raw.csv
        │       ↓ opens warehouse/hdb_resale.duckdb
        │       ↓ creates DuckDB table: raw_hdb_resale
        │
        ├── 2. Validate             src/validate.py
        │       ↓ reads raw_hdb_resale from DuckDB (SQL queries only — no pandas load)
        │       ↓ checks: row count, columns, nulls, duplicates, numeric sanity
        │       ↓ saves reports/data_quality_report.csv
        │       ↓ prints OK / WARN / FAIL counts
        │
        ├── 3. Transform            src/transform.py
        │       ↓ runs _TRANSFORM_SQL against raw_hdb_resale in DuckDB
        │       ↓ creates DuckDB table: clean_hdb_resale
        │       ↓ exports clean_hdb_resale → data/processed/hdb_resale_clean.parquet
        │
        └── 4. Curate               src/curate.py
                ↓ runs 3 SQL GROUP BY queries against clean_hdb_resale
                ↓ creates DuckDB tables: monthly_town_prices, flat_type_summary, town_price_ranking
                ↓ exports each table → data/curated/*.parquet
```

### What gets created on a successful run

```
data/raw/hdb_resale_raw.csv              ← original download, untouched
data/processed/hdb_resale_clean.parquet  ← cleaned + feature-engineered
data/curated/monthly_town_prices.parquet ← aggregated by year/month/town
data/curated/flat_type_summary.parquet   ← aggregated by flat type
data/curated/town_price_ranking.parquet  ← towns ranked by $/sqm
warehouse/hdb_resale.duckdb              ← database with all 5 SQL tables
reports/data_quality_report.csv          ← quality check results
logs/pipeline.log                        ← timestamped run log
```

---

## Data Flow

Here is how the data moves through the project, step by step:

```
┌────────────────────────────────────────────────────────────┐
│  SOURCE                                                    │
│  GitHub raw CSV URL  (or a local CSV file)                 │
└────────────────────┬───────────────────────────────────────┘
                     │  pandas.read_csv()
                     ▼
┌────────────────────────────────────────────────────────────┐
│  RAW COPY                                                  │
│  data/raw/hdb_resale_raw.csv                               │
│  (never modified — always available for re-processing)     │
└────────────────────┬───────────────────────────────────────┘
                     │  DuckDB: CREATE TABLE ... AS SELECT * FROM df
                     ▼
┌────────────────────────────────────────────────────────────┐
│  STAGING TABLE  (DuckDB)                                   │
│  raw_hdb_resale                                            │
│  Columns: month, town, flat_type, block, street_name,      │
│           storey_range, floor_area_sqm, flat_model,        │
│           lease_commence_date, remaining_lease,            │
│           resale_price                                     │
└────────────────────┬───────────────────────────────────────┘
                     │  validate.py reads this table with SQL
                     │  (no transformation — quality checks only)
                     ▼
              reports/data_quality_report.csv
                     │
                     │  transform.py: single SQL query
                     │  (deduplicate → filter → parse → derive)
                     ▼
┌────────────────────────────────────────────────────────────┐
│  CLEAN TABLE  (DuckDB)                                     │
│  clean_hdb_resale                                          │
│  All original columns PLUS:                                │
│    sale_year, sale_month                                   │
│    storey_min, storey_max, storey_mid                      │
│    remaining_lease_months                                  │
│    flat_age                                                │
│    price_per_sqm                                           │
└────────┬───────────────────────────────────────────────────┘
         │  DuckDB: COPY ... TO ... (FORMAT PARQUET)
         ▼
data/processed/hdb_resale_clean.parquet
         │
         │  curate.py: 3 GROUP BY SQL queries
         ▼
┌────────────────────────────────────────────────────────────┐
│  CURATED TABLES  (DuckDB)                                  │
│  monthly_town_prices   → data/curated/monthly_town_...     │
│  flat_type_summary     → data/curated/flat_type_...        │
│  town_price_ranking    → data/curated/town_price_...       │
└────────────────────────────────────────────────────────────┘
```

### What columns are created in the transform step

| Raw Column | New Column(s) | How |
|---|---|---|
| `month` (e.g. `"2022-03"`) | `sale_year` = 2022, `sale_month` = 3 | `STRPTIME` in DuckDB SQL |
| `storey_range` (e.g. `"10 TO 12"`) | `storey_min` = 10, `storey_max` = 12, `storey_mid` = 11.0 | `split_part` in DuckDB SQL |
| `remaining_lease` (e.g. `"61 years 04 months"`) | `remaining_lease_months` = 736 | `regexp_extract` in DuckDB SQL |
| `lease_commence_date` | `flat_age` = `sale_year - lease_commence_date` | SQL arithmetic |
| `resale_price`, `floor_area_sqm` | `price_per_sqm` | SQL division |

---

## Tools Used

| Tool | Where it appears | What it does |
|---|---|---|
| **Python 3.11** | All `src/` files | Core language |
| **pandas** | `src/ingest.py` only | Reads the CSV file and loads it into DuckDB |
| **DuckDB** | `src/storage.py`, `ingest.py`, `validate.py`, `transform.py`, `curate.py` | In-process SQL database engine. Stores all tables and runs all transformations. No server needed. |
| **PyArrow / Parquet** | Used by DuckDB automatically via `COPY ... FORMAT PARQUET` | Columnar file format for efficient storage of large datasets |
| **PyYAML** | All `src/` files via `_load_config()` | Reads `config/config.yaml` |
| **pathlib** | `src/paths.py`, imported by all `src/` files | Cross-platform file path management |
| **Docker** | `Dockerfile`, `docker-compose.yml` | Containerised, reproducible execution |
| **Apache Airflow** | `dags/hdb_resale_pipeline_dag.py` | Optional task scheduler. Not needed for the pipeline to run. |
| **Git / GitHub** | `.gitignore`, repo at `muhammadmirza97/hdb-resale-data-engineering-pipeline` | Version control and remote storage |

### Where each technology is located in the code

**DuckDB (SQL engine):**
- Connection opened/closed: `src/storage.py` → `get_connection()`
- Raw data loaded: `src/ingest.py` line 79
- Quality check queries: `src/validate.py` lines 62–120
- Core transformation SQL: `src/transform.py` lines 47–125 (`_TRANSFORM_SQL`)
- Analytics table SQL: `src/curate.py` lines 44–94
- Parquet export: `src/storage.py` → `export_table_to_parquet()`

**Parquet files:**
- Clean dataset written: `src/transform.py` line 156
- Curated tables written: `src/curate.py` lines 58, 78, 95

**Docker:**
- Build instructions: `Dockerfile`
- Service definition + volume mounts: `docker-compose.yml`

**Airflow:**
- Task definitions and DAG wiring: `dags/hdb_resale_pipeline_dag.py`

**All project paths (pathlib):**
- Defined once: `src/paths.py`
- Imported by all other `src/` modules

---

## Files Safe to Edit

These files are designed to be changed as your project grows.
Editing them will not break the pipeline structure.

| File | What you would change it for |
|---|---|
| `config/config.yaml` | Change the data source URL, switch to a local file, rename DuckDB tables, change log level |
| `src/curate.py` | Add a new analytics table, change the columns in an existing aggregation, add a new GROUP BY query |
| `src/validate.py` | Add more quality checks (e.g. check a specific town name, add a value range check), change what counts as FAIL vs WARN |
| `README.md` | Update documentation, add new sections |
| `requirements.txt` | Add a new Python package (e.g. `matplotlib` for charts) |
| `notebooks/` | Add new exploration notebooks; the existing Week 1 notebook is just a reference |
| `scripts/setup_project.py` | Add more directories if you add new output locations |
| `dags/hdb_resale_pipeline_dag.py` | Change the schedule, add retry logic, add email notifications |

---

## Files to Be Careful With

These files are load-bearing — other files depend on them.
A mistake here can break the entire pipeline.

| File | Why to be careful | What could go wrong |
|---|---|---|
| `src/paths.py` | Every other `src/` file imports paths from here | Changing a variable name or path will cause import errors across the whole project |
| `src/storage.py` | Every step that needs DuckDB calls `get_connection()` from here | Changing the function signature or DB path logic breaks all four pipeline steps |
| `src/transform.py` | Contains the core SQL that creates `clean_hdb_resale` — the table that `curate.py` reads from | Dropping a column here will break `curate.py` if that column is used in an aggregation |
| `src/ingest.py` | The only place raw data enters the system | Changing the raw table name without updating `config.yaml` breaks validate and transform |
| `src/run_pipeline.py` | Controls the execution order | Changing the order of `ingest → validate → transform → curate` will cause failures (e.g. transform cannot run before ingest) |
| `src/__init__.py` | Empty but required | Deleting it breaks `python -m src.run_pipeline` |
| `config/config.yaml` | Table names here must match what the code expects | Renaming `raw_table` or `clean_table` here without updating `transform.py` or `curate.py` will cause SQL errors |

---

## Current Gaps or TODOs

These are things the project does not yet do, spotted during the code review.

### Functional gaps

1. **No unit tests**
   There is no `tests/` folder and no `pytest` setup.
   The transformation SQL in `transform.py` has no automated test to verify it produces correct output.

2. **Validation does not stop the pipeline on failure**
   In `validate.py`, if a FAIL-level check is found, it logs a warning but the pipeline
   continues anyway. If the data has serious problems (e.g. `resale_price` is all zeros),
   the next steps will still run and produce bad output silently.

3. **No incremental loading**
   Every run re-downloads the full CSV (200k+ rows) and replaces all DuckDB tables from scratch.
   There is no way to only process new months of data.

4. **`requests` is in `requirements.txt` but not used in the code**
   `pandas.read_csv(url)` handles the HTTP download directly.
   The `requests` package is installed but never imported in any `src/` file.

### Code observations

5. **Dead code in `validate.py` (lines 83–86)**
   The first attempt at counting duplicates using a complex window function is computed
   and stored in `dupe_count` but then immediately replaced by a simpler approach
   (lines 88–91). The first calculation is never used.
   ```python
   # This result is computed but then overwritten — it can be removed
   dupe_count = conn.execute(
       f"SELECT COUNT(*) - COUNT(*) OVER () FROM "
       f"(SELECT COUNT(*) AS n FROM {raw_table} GROUP BY ALL HAVING n > 1)"
   ).fetchone()
   ```

6. **`run_pipeline.py` says "Step 5" in the summary but runs 4 steps**
   The summary print block (line 100) references 5 outputs but the `_step()` calls
   only number up to Step 4. Not a bug — just a minor inconsistency in the messaging.

7. **`data/curated/` Parquet files are produced but never read back**
   The curated Parquet files are exported but nothing in the pipeline currently reads
   them back. They are final outputs intended for external tools (BI dashboards, pandas,
   etc.) but there is no example query or notebook demonstrating this yet.

8. **Airflow DAG has not been tested with a real Airflow installation**
   The DAG is syntactically correct and imports are guarded, but it is documented
   as a design artifact. It may need minor adjustments (e.g. `AIRFLOW_HOME` config,
   Python path) to run in an actual Airflow environment.

---

## Next Recommended Steps

In rough priority order:

### 1. Run the pipeline and verify outputs
```bash
cd hdb-resale-data-engineering-pipeline
pip install -r requirements.txt
python -m src.run_pipeline
```
Check that all 8 output files appear. Open `reports/data_quality_report.csv` and
review any WARN or FAIL entries.

### 2. Query the warehouse to verify the data looks correct
```python
import duckdb
conn = duckdb.connect("warehouse/hdb_resale.duckdb")

# See all tables
print(conn.execute("SHOW TABLES").df())

# Top 10 most expensive towns
print(conn.execute("SELECT * FROM town_price_ranking LIMIT 10").df())

# Price distribution by flat type
print(conn.execute("SELECT * FROM flat_type_summary").df())
```

### 3. Add a validation hard-stop for FAIL results
In `src/validate.py`, after the report is saved, add:
```python
if fail_count > 0:
    raise ValueError(f"{fail_count} quality FAIL(s) — pipeline stopped. Review: {QUALITY_REPORT_PATH}")
```
This prevents bad data from flowing through to transform and curate.

### 4. Remove the dead code in `validate.py`
Delete lines 83–86 (the unused `dupe_count` calculation) to clean up the file.

### 5. Remove `requests` from `requirements.txt`
It is not used. Removing it keeps the dependency list honest.

### 6. Add a `notebooks/02_explore_warehouse.ipynb` notebook
A short notebook that opens `hdb_resale.duckdb` and queries the curated tables
would make the project much more demonstrable. This is the natural "output showcase"
for interviews and reviews.

### 7. Add unit tests
Create a `tests/` folder with `pytest`. Start with:
- A test for `_parse_storey_range` logic (ensure `"10 TO 12"` → min=10, max=12, mid=11.0)
- A test for the remaining_lease SQL parsing (ensure `"61 years 04 months"` → 736 months)

### 8. Consider splitting the transform SQL into named views
The single large `_TRANSFORM_SQL` in `transform.py` works but is hard to test in
isolation. Splitting it into `CREATE VIEW deduped AS ...`, `CREATE VIEW filtered AS ...`,
etc., would make each step queryable and debuggable independently.

---

*This document was generated by reading every file in the repository.*
*It reflects the state of the code as it exists — not as it was planned or described elsewhere.*
