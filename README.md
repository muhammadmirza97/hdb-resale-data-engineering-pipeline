# HDB Resale Data Engineering Pipeline

An end-to-end **ELT batch data pipeline** for Singapore HDB resale transaction data,
built as the Week 2 SNAIC Capstone project.

> Week 1 focused on machine learning (price prediction).
> Week 2 re-engineers that work into a production-style data pipeline.

---

## Project Goal

Transform raw HDB resale CSV data — collected by Singapore's Housing Development Board —
into a clean, analytics-ready DuckDB warehouse that can serve dashboards, reports, and
downstream ML models without any manual notebook intervention.

---

## Problem Statement

The raw HDB resale dataset has several real-world data engineering challenges:

| Problem | How the pipeline solves it |
|---|---|
| Storey range stored as text (`"10 TO 12"`) | SQL `split_part` → `storey_min`, `storey_max`, `storey_mid` |
| Remaining lease stored as prose (`"61 years 04 months"`) | SQL `regexp_extract` → `remaining_lease_months` |
| No derived analytical features | SQL computes `flat_age`, `price_per_sqm` at transform time |
| No quality gate before analysis | Automated validation step saves a CSV quality report |
| Results lost when notebook closes | DuckDB warehouse and Parquet files persist every run |
| Reproducibility depends on who runs the notebook | Docker container + config file makes every run identical |

---

## Data Source

| Attribute | Value |
|---|---|
| Dataset | HDB Resale Flat Prices (Jan 2017 onwards) |
| Provider | data.gov.sg |
| Format | CSV — ~200k+ rows, 11 columns |
| Access | Public GitHub raw URL (configured in `config/config.yaml`) |

---

## ELT Pipeline Architecture

```
┌───────────────┐
│  Data Source   │  GitHub raw CSV URL
│  (data.gov.sg) │  or local file path
└──────┬────────┘
       │ Extract
       ▼
┌───────────────────────────────────┐
│  1. Ingest  (src/ingest.py)       │
│  • Download CSV                   │
│  • Save data/raw/hdb_resale_      │
│    raw.csv  (unchanged)           │
│  • Load into DuckDB staging table │
│    raw_hdb_resale                 │
└──────┬────────────────────────────┘
       │
       ▼
┌───────────────────────────────────┐
│  2. Validate  (src/validate.py)   │
│  • Schema check                   │
│  • Null / duplicate counts        │
│  • Sanity checks on numerics      │
│  • Save reports/data_quality_     │
│    report.csv                     │
└──────┬────────────────────────────┘
       │ Load → Transform (ELT)
       ▼
┌───────────────────────────────────┐
│  3. Transform  (src/transform.py) │
│  SQL inside DuckDB:               │
│  • Deduplicate (QUALIFY)          │
│  • Parse month → sale_year/month  │
│  • Parse storey_range             │
│  • Parse remaining_lease          │
│  • Derive flat_age, price_per_sqm │
│  • CREATE TABLE clean_hdb_resale  │
│  • Export Parquet →               │
│    data/processed/                │
└──────┬────────────────────────────┘
       │
       ▼
┌───────────────────────────────────┐
│  4. Curate  (src/curate.py)       │
│  DuckDB analytics tables:         │
│  • monthly_town_prices            │
│  • flat_type_summary              │
│  • town_price_ranking             │
│  + Export each to data/curated/   │
└───────────────────────────────────┘
```

**Design choice — ELT over ETL:**
Data is loaded into DuckDB first (as `raw_hdb_resale`) and transformed
entirely in SQL. This keeps Python code thin, transformations auditable,
and the raw data always available for re-processing without a new download.

---

## Folder Structure

```
hdb-resale-data-engineering-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── Dockerfile
├── docker-compose.yml
│
├── config/
│   └── config.yaml              ← All settings: URL, DB path, table names
│
├── data/
│   ├── raw/                     ← hdb_resale_raw.csv  (after ingest)
│   ├── processed/               ← hdb_resale_clean.parquet  (after transform)
│   └── curated/                 ← 3 × analytics Parquet files  (after curate)
│
├── warehouse/
│   └── hdb_resale.duckdb        ← DuckDB database  (all SQL tables live here)
│
├── reports/
│   └── data_quality_report.csv  ← Quality check results  (after validate)
│
├── logs/
│   └── pipeline.log             ← Timestamped run history
│
├── notebooks/
│   └── Capstone_1_HDB_resale_price_prediction_f.ipynb   ← original Week 1
│
├── src/
│   ├── __init__.py
│   ├── paths.py                 ← Centralised pathlib path definitions
│   ├── ingest.py                ← Step 1: Download → raw DuckDB table
│   ├── validate.py              ← Step 2: Quality checks → CSV report
│   ├── transform.py             ← Step 3: SQL ELT → clean table + Parquet
│   ├── storage.py               ← DuckDB connection helpers
│   ├── curate.py                ← Step 4: Analytics tables + curated Parquet
│   └── run_pipeline.py          ← Master runner: steps 1–4 in sequence
│
├── dags/
│   └── hdb_resale_pipeline_dag.py   ← Airflow DAG (optional)
│
└── scripts/
    └── setup_project.py             ← One-time directory setup
```

---

## Tools Used

| Tool | Version | Role |
|---|---|---|
| Python | 3.11 | Core language |
| pandas | ≥ 2.0 | Initial CSV read / write |
| DuckDB | ≥ 0.10 | In-process SQL engine — staging, transforms, analytics |
| PyArrow | ≥ 14 | Parquet read/write via DuckDB `COPY … FORMAT PARQUET` |
| PyYAML | ≥ 6.0 | Config loading |
| pathlib | stdlib | Cross-platform path management |
| Docker | any | Reproducible container execution |
| Apache Airflow | ≥ 2.8 (optional) | DAG-based scheduling |

---

## How to Run Locally

### 1 — Clone and install

```bash
git clone https://github.com/muhammadmirza97/hdb-resale-data-engineering-pipeline.git
cd hdb-resale-data-engineering-pipeline
pip install -r requirements.txt
```

### 2 — Run the full pipeline

```bash
python -m src.run_pipeline
```

### 3 — Run individual steps

```bash
python -m src.ingest      # Download CSV → raw DuckDB table
python -m src.validate    # Quality checks → reports/
python -m src.transform   # SQL transforms → clean table + Parquet
python -m src.curate      # Analytics tables → curated Parquet
```

### 4 — Query the warehouse

```python
import duckdb
conn = duckdb.connect("warehouse/hdb_resale.duckdb")

# Top 10 most expensive towns by median price per sqm
print(conn.execute("SELECT * FROM town_price_ranking LIMIT 10").df())

# Average resale prices by flat type
print(conn.execute("SELECT * FROM flat_type_summary").df())
```

---

## How to Run with Docker

### Prerequisites
- Docker Desktop installed and running

```bash
# Build and run the full pipeline
docker-compose up --build

# Run a single step inside the container
docker-compose run --rm hdb-pipeline python -m src.ingest

# Open an interactive shell
docker-compose run --rm hdb-pipeline bash
```

All outputs (`data/`, `warehouse/`, `reports/`, `logs/`) are mounted back
to your local machine via volumes in `docker-compose.yml`.

---

## How Airflow Fits In

The `dags/hdb_resale_pipeline_dag.py` file defines an Airflow DAG with four tasks:

```
ingest_data → validate_data → transform_data → create_curated_tables
```

Airflow is **not required** to run the pipeline — `python -m src.run_pipeline`
does the same work. Airflow becomes useful when you need:
- Scheduled weekly/daily runs
- Task-level retry logic
- A visual UI to monitor pipeline health

To run with Airflow locally:
```bash
pip install apache-airflow
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db migrate
airflow standalone
# Visit http://localhost:8080 and trigger dag: hdb_resale_pipeline
```

---

## Outputs Produced

| Output | Location | Contents |
|---|---|---|
| Raw CSV | `data/raw/hdb_resale_raw.csv` | Unchanged source file |
| Clean Parquet | `data/processed/hdb_resale_clean.parquet` | Cleaned + feature-engineered |
| Curated Parquet | `data/curated/monthly_town_prices.parquet` | Monthly aggregates by town |
| Curated Parquet | `data/curated/flat_type_summary.parquet` | Stats by flat type |
| Curated Parquet | `data/curated/town_price_ranking.parquet` | Towns ranked by median $/sqm |
| DuckDB database | `warehouse/hdb_resale.duckdb` | All SQL tables |
| Quality report | `reports/data_quality_report.csv` | Per-check pass/fail/warn |
| Pipeline log | `logs/pipeline.log` | Timestamped run history |

### DuckDB tables

| Table | Description |
|---|---|
| `raw_hdb_resale` | Staging — raw data as downloaded |
| `clean_hdb_resale` | Transformed — with all derived columns |
| `monthly_town_prices` | Avg/median resale price by year, month, town |
| `flat_type_summary` | Price and area stats by flat type |
| `town_price_ranking` | Towns ranked by median price per sqm |

---

## Key Feature Engineering (reused from Week 1 capstone)

| Raw Column | Derived Feature | Transformation Logic |
|---|---|---|
| `month` | `sale_year`, `sale_month` | `STRPTIME(month, '%Y-%m')` in DuckDB SQL |
| `storey_range` | `storey_min`, `storey_max`, `storey_mid` | `split_part(storey_range, ' TO ', n)` |
| `remaining_lease` | `remaining_lease_months` | `regexp_extract` for years + months |
| `lease_commence_date` | `flat_age` | `sale_year - lease_commence_date` |
| `resale_price`, `floor_area_sqm` | `price_per_sqm` | Division |

---

## Challenges

1. **ELT-style `remaining_lease` parsing** — The text format `"61 years 04 months"` cannot be
   cast directly in SQL. Solved using DuckDB's `regexp_extract` with `TRY_CAST` for null safety.

2. **Deduplication in pure SQL** — Used DuckDB's `QUALIFY ROW_NUMBER() OVER (PARTITION BY ALL …) = 1`
   pattern, which is cleaner and faster than a pandas `drop_duplicates()`.

3. **Cross-platform path resolution** — `src/paths.py` uses `find_project_base_dir()` to walk
   up the file tree to the project root, so the pipeline works from any working directory or inside Docker.

4. **DuckDB absolute path requirement** — `COPY table TO path` in DuckDB requires an absolute path.
   Solved with `Path.resolve()` before passing to SQL.

---

## Future Improvements

- **Incremental loading** — Only ingest new months rather than re-downloading the full CSV.
- **Data versioning** — Use DVC or Delta Lake to track dataset snapshots over time.
- **Unit tests** — Add `pytest` tests for each transformation function using a small fixture CSV.
- **Dashboard** — Connect Streamlit or Evidence.dev to the DuckDB warehouse for live charts.
- **Cloud storage** — Replace local Parquet with S3/GCS; replace DuckDB with BigQuery or Snowflake for team sharing.
- **ML re-integration** — Re-train the Week 1 Random Forest model on the `clean_hdb_resale` table as a pipeline step.
- **Streaming** — Replace the batch download step with a Kafka consumer if a real-time feed becomes available.

---

## Resume Bullet Points

- Designed and implemented a 4-step ELT batch pipeline in Python that ingests, validates, transforms, and serves Singapore HDB resale data (200k+ transactions) using DuckDB SQL for all transformation logic.
- Applied ELT design principles: raw data is loaded into a DuckDB staging table first, then all transformations (deduplication, feature derivation, type parsing) run as auditable SQL — keeping Python code thin and results reproducible.
- Engineered automated data quality checks (schema validation, null detection, duplicate counting, sanity checks on numerics) with structured CSV reporting, following production data-observability patterns.
- Used columnar storage (Apache Parquet) and an embedded OLAP engine (DuckDB) to export analytics-ready tables — `monthly_town_prices`, `flat_type_summary`, `town_price_ranking` — consumable by any SQL client or BI tool.
- Containerised the pipeline with Docker and docker-compose, and defined an Apache Airflow DAG for scheduled weekly execution, making the project deployment-ready.

---

## Author

Muhammad Mirza — SNAIC Week 2 Capstone, Data Engineering
Built on top of the Week 1 HDB resale price prediction project.
