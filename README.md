# HDB Resale Data Engineering & Price Prediction Platform

An end-to-end **data engineering + ML + serving platform** for Singapore HDB resale transaction data,
built as the Week 2 SNAIC Capstone project.

> Week 1 focused on machine learning (price prediction) in a Jupyter notebook.
> Week 2 re-engineers that work into a production-style data pipeline with model training,
> a REST API, an interactive dashboard, and Docker-based deployment.

---

## Project Goal

Transform raw HDB resale CSV data into a clean DuckDB warehouse, train a price prediction
model, serve predictions via FastAPI, and visualise results in a Streamlit dashboard —
all orchestrated by an Airflow DAG and packaged with Docker Compose.

---

## Problem Statement

| Problem | How the platform solves it |
|---|---|
| Storey range stored as text (`"10 TO 12"`) | SQL `split_part` → `storey_min`, `storey_max`, `storey_mid` |
| Remaining lease stored as prose (`"61 years 04 months"`) | SQL `regexp_extract` → `remaining_lease_months` |
| No derived analytical features | SQL computes `flat_age`, `price_per_sqm` at transform time |
| No quality gate before analysis | Automated validation step saves a CSV quality report |
| ML model lives only in a notebook | Trained model saved to `models/` and served via FastAPI |
| No way to compare model runs | MLflow experiment tracking (local file-based, no server needed) |
| Results lost when notebook closes | DuckDB warehouse + Parquet files persist every run |
| No anomaly detection | Alerts step flags towns with unusual price patterns |

---

## Data Source

| Attribute | Value |
|---|---|
| Dataset | HDB Resale Flat Prices (Jan 2017 onwards) |
| Provider | data.gov.sg |
| Format | CSV — ~200k+ rows, 11 columns |
| Access | Public GitHub raw URL (configured in `config/config.yaml`) |

---

## Full Platform Architecture

```
 Data Source (data.gov.sg)
         │
         ▼
 ┌──────────────────────────────────────────────┐
 │  ELT Pipeline  (src/)                         │
 │                                              │
 │  1. ingest.py      → raw_hdb_resale (DuckDB) │
 │  2. validate.py    → data_quality_report.csv │
 │  3. clean.py       → stg_hdb_resale (DuckDB) │
 │  4. transform.py   → clean_hdb_resale + Parquet│
 │  5. curate.py      → analytics tables         │
 │                                              │
 └──────────────────┬───────────────────────────┘
                    │
         ┌──────────▼──────────┐
         │  ML Pipeline (src/) │
         │                     │
         │  6. train_model.py  │ ← MLflow tracking
         │  7. predict.py      │ → predictions table
         │  8. evaluate_model.py│→ model_evaluation.csv
         │  9. alerts.py       │ → alerts.csv
         │                     │
         └──┬──────────────────┘
            │
     ┌──────┴───────────┐
     │                  │
     ▼                  ▼
 FastAPI :8011    Streamlit :8501
 /health          Price Trends
 /towns           Town Analysis
 /flat-types      Flat Type
 /summary/{town}  Predict Form
 /predict         Model Eval
                  Alerts
```

**Design choice — ELT over ETL:**
Data is loaded into DuckDB first (as `raw_hdb_resale`) and transformed entirely in SQL.
This keeps Python code thin, transformations auditable, and the raw data always available
for re-processing without a new download.

---

## Folder Structure

```
hdb-resale-data-engineering-pipeline/
├── README.md
├── requirements.txt             ← Core + ML + API + Dashboard deps
├── requirements-airflow.txt     ← Airflow (install separately)
├── .gitignore
├── Dockerfile
├── docker-compose.yml           ← hdb-pipeline, hdb-api, hdb-dashboard, optional kafka
│
├── config/
│   └── config.yaml              ← All settings: URL, tables, ML params, alerts
│
├── src/
│   ├── paths.py                 ← Centralised pathlib path definitions
│   ├── config_loader.py         ← Cached YAML loader (lru_cache)
│   ├── storage.py               ← DuckDB connection helpers
│   ├── ingest.py                ← Step 1: Download CSV → raw_hdb_resale
│   ├── validate.py              ← Step 2: Quality checks → CSV report
│   ├── clean.py                 ← Step 3: Dedup + filter → stg_hdb_resale
│   ├── transform.py             ← Step 4: SQL features → clean_hdb_resale + Parquet
│   ├── curate.py                ← Step 5: Analytics tables → curated Parquet
│   ├── train_model.py           ← Step 6: Train LinearReg + RandomForest (MLflow)
│   ├── predict.py               ← Step 7: Batch inference → predictions table
│   ├── evaluate_model.py        ← Step 8: MAE/RMSE/R2 vs actuals
│   ├── alerts.py                ← Step 9: Anomaly detection → alerts table
│   └── run_pipeline.py          ← Master runner: all 9 steps in sequence
│
├── api/
│   └── main.py                  ← FastAPI: /health /towns /flat-types /summary /predict
│
├── dashboard/
│   └── app.py                   ← Streamlit: 6 views including prediction form
│
├── dags/
│   └── hdb_resale_pipeline_dag.py  ← Airflow DAG: 8 tasks, @daily schedule
│
├── kafka/                       ← Optional: streaming simulation
│   ├── producer.py              ← Publishes fake HDB transactions to Kafka
│   └── consumer.py              ← Reads from Kafka → DuckDB raw table
│
├── spark/                       ← Optional: distributed processing demo
│   └── spark_transform.py       ← PySpark version of clean + transform steps
│
├── dbt_hdb/                     ← Optional: SQL transforms via dbt-duckdb
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/stg_hdb_resale.sql
│       └── marts/               ← mart_town_stats, mart_flat_type_stats, mart_monthly_trends
│
├── data/
│   ├── raw/                     ← hdb_resale_raw.csv
│   ├── processed/               ← hdb_resale_clean.parquet
│   ├── curated/                 ← 3 analytics Parquet files
│   └── predictions/             ← hdb_resale_predictions.parquet
│
├── warehouse/
│   └── hdb_resale.duckdb        ← All DuckDB tables live here
│
├── models/
│   └── hdb_resale_model.joblib  ← Best trained sklearn Pipeline (joblib)
│
├── mlruns/                      ← MLflow experiment tracking (local file-based)
│
├── reports/
│   ├── data_quality_report.csv
│   ├── model_evaluation.csv
│   └── alerts.csv
│
├── logs/
│   └── pipeline.log
│
├── notebooks/
│   └── Capstone_1_HDB_resale_price_prediction_f.ipynb   ← original Week 1
│
└── scripts/
    └── check_outputs.py         ← Verify all expected output files exist
```

---

## Tools Used

| Tool | Role |
|---|---|
| Python 3.11 | Core language |
| DuckDB ≥ 0.10 | In-process SQL engine — all table storage and transforms |
| pandas ≥ 2.0 | CSV ingestion; DataFrame ↔ DuckDB interchange |
| PyArrow ≥ 14 | Parquet read/write |
| scikit-learn ≥ 1.4 | ML Pipeline: StandardScaler + OneHotEncoder + RandomForest |
| MLflow ≥ 2.11 | Experiment tracking (local file-based, no server needed) |
| joblib | Model serialisation |
| FastAPI ≥ 0.110 | REST API for prediction serving |
| Streamlit ≥ 1.33 | Interactive analytics dashboard |
| Plotly | Charts inside Streamlit |
| Docker + Compose | Reproducible multi-service packaging |
| Apache Airflow ≥ 2.8 | DAG-based scheduling (optional) |
| kafka-python | Streaming simulation (optional) |
| PySpark ≥ 3.5 | Distributed processing demo (optional) |
| dbt-duckdb | SQL transform framework (optional) |

---

## How to Run

### 1 — Clone and install

```bash
git clone https://github.com/muhammadmirza97/hdb-resale-data-engineering-pipeline.git
cd hdb-resale-data-engineering-pipeline
pip install -r requirements.txt
```

### 2 — Run the full pipeline (ELT + ML)

```bash
python -m src.run_pipeline
```

ELT only (skip training):

```bash
python -m src.run_pipeline --skip-ml
```

### 3 — Run individual steps

```bash
python -m src.ingest          # Step 1: Download CSV → raw_hdb_resale
python -m src.validate        # Step 2: Quality checks
python -m src.clean           # Step 3: Dedup + filter → stg_hdb_resale
python -m src.transform       # Step 4: SQL features → clean_hdb_resale
python -m src.curate          # Step 5: Analytics tables
python -m src.train_model     # Step 6: Train + MLflow
python -m src.predict         # Step 7: Batch predictions
python -m src.evaluate_model  # Step 8: MAE / RMSE / R2
python -m src.alerts          # Step 9: Anomaly detection
```

### 4 — Start the API

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8011 --reload
# Visit http://localhost:8011/docs
```

### 5 — Start the Dashboard

```bash
streamlit run dashboard/app.py
# Visit http://localhost:8501
```

### 6 — Query the warehouse directly

```python
import duckdb
conn = duckdb.connect("warehouse/hdb_resale.duckdb")
print(conn.execute("SELECT * FROM town_price_ranking LIMIT 10").df())
print(conn.execute("SELECT * FROM predictions LIMIT 5").df())
```

---

## Docker Compose

```bash
# Build and start all services (pipeline + API + dashboard)
docker-compose up --build

# Pipeline only
docker-compose up --build hdb-pipeline

# API + dashboard (pipeline must have run first)
docker-compose up --build hdb-api hdb-dashboard

# With Kafka streaming simulation
docker-compose --profile kafka up --build

# Single step inside the container
docker-compose run --rm hdb-pipeline python -m src.ingest
```

| Service | Port | Description |
|---|---|---|
| hdb-pipeline | — | Runs `python -m src.run_pipeline` |
| hdb-api | 8011 | FastAPI prediction server |
| hdb-dashboard | 8501 | Streamlit analytics dashboard |
| kafka (optional) | 9092 | Kafka broker for streaming simulation |

---

## Airflow DAG

The `dags/hdb_resale_pipeline_dag.py` defines an 8-task DAG:

```
ingest_data
  → validate_data
  → clean_transform_data
  → create_curated_tables
  → train_model
  → generate_predictions
  → evaluate_model
  → check_alerts
```

Schedule: `@daily`. To run with Airflow locally:

```bash
pip install -r requirements-airflow.txt
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db migrate
airflow standalone
# Visit http://localhost:8080 → trigger 'hdb_resale_pipeline'
```

---

## FastAPI Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness check + model status |
| GET | `/towns` | All distinct towns |
| GET | `/flat-types` | All distinct flat types |
| GET | `/summary/town/{town}` | Price stats for a specific town |
| POST | `/predict` | Predict resale price for a flat |

Interactive docs: `http://localhost:8011/docs`

---

## MLflow Experiment Tracking

MLflow runs are stored locally in `mlruns/` — no server required.

Two models are tracked per run:
- `LinearRegression` — fast baseline
- `RandomForestRegressor` — best performer (n_estimators=100, max_depth=15)

Metrics logged: MAE, RMSE, R2, MAPE.

The best model (highest R2) is automatically saved to `models/hdb_resale_model.joblib`.

---

## Optional Components

### Kafka (streaming simulation)
```bash
# Start Kafka via Docker Compose profile
docker-compose --profile kafka up -d kafka

# In one terminal: produce 100 simulated transactions
python -m kafka.producer --count 100 --delay 0.5

# In another terminal: consume and load into DuckDB
python -m kafka.consumer
```

### PySpark (distributed processing demo)
```bash
pip install pyspark
python spark/spark_transform.py
# Reads data/raw/hdb_resale_raw.csv → writes data/processed/spark_hdb_resale_clean.parquet
```

### dbt-duckdb (SQL transform framework)
```bash
pip install dbt-core dbt-duckdb
cd dbt_hdb
dbt run          # runs staging + mart models
dbt test         # runs schema tests
dbt docs serve   # interactive lineage docs
```

---

## Outputs

| Output | Location |
|---|---|
| Raw CSV | `data/raw/hdb_resale_raw.csv` |
| Staging table | `DuckDB: stg_hdb_resale` |
| Clean Parquet | `data/processed/hdb_resale_clean.parquet` |
| Curated Parquet | `data/curated/monthly_town_prices.parquet` |
| Curated Parquet | `data/curated/flat_type_summary.parquet` |
| Curated Parquet | `data/curated/town_price_ranking.parquet` |
| Trained model | `models/hdb_resale_model.joblib` |
| Predictions Parquet | `data/predictions/hdb_resale_predictions.parquet` |
| Evaluation report | `reports/model_evaluation.csv` |
| Alerts report | `reports/alerts.csv` |
| Quality report | `reports/data_quality_report.csv` |
| MLflow experiments | `mlruns/` |
| Pipeline log | `logs/pipeline.log` |

### DuckDB tables

| Table | Description |
|---|---|
| `raw_hdb_resale` | Raw data as downloaded |
| `stg_hdb_resale` | Deduplicated + filtered staging |
| `clean_hdb_resale` | Fully featured (all derived columns) |
| `monthly_town_prices` | Aggregates by year, month, town |
| `flat_type_summary` | Price and area stats by flat type |
| `town_price_ranking` | Towns ranked by median $/sqm |
| `predictions` | Actual vs predicted resale prices |
| `model_evaluation` | MAE / RMSE / R2 per training run |
| `alerts` | Price anomaly and volume alerts |

---

## Key Feature Engineering (from Week 1 capstone)

| Raw Column | Derived Feature | Transformation |
|---|---|---|
| `month` | `sale_year`, `sale_month` | `STRPTIME(month, '%Y-%m')` |
| `storey_range` | `storey_min`, `storey_max`, `storey_mid` | `split_part(storey_range, ' TO ', n)` |
| `remaining_lease` | `remaining_lease_months` | `regexp_extract` years + months |
| `lease_commence_date` | `flat_age` | `sale_year - lease_commence_date` |
| `resale_price / floor_area_sqm` | `price_per_sqm` | Division |

---

## Author

Muhammad Mirza — SNAIC Week 2 Capstone, Data Engineering
Built on top of the Week 1 HDB resale price prediction project.
