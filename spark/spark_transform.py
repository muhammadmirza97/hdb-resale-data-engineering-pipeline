"""
Spark Transform — HDB Resale Data (Optional Demonstration)
===========================================================
Demonstrates how the clean + feature engineering steps would be
implemented using PySpark for large-scale distributed processing.

In this project, DuckDB handles all transformations efficiently
for the dataset size (~180K rows). PySpark would be the right
tool if the dataset were in the tens-of-millions of rows.

Requirements:
  pip install pyspark

Usage:
  python spark/spark_transform.py

Reads:  data/raw/hdb_resale_raw.csv
Writes: data/processed/spark_hdb_resale_clean.parquet
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

RAW_CSV = _PROJECT_ROOT / "data" / "raw" / "hdb_resale_raw.csv"
OUTPUT_PARQUET = _PROJECT_ROOT / "data" / "processed" / "spark_hdb_resale_clean.parquet"


def run_spark_transform() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, DoubleType
    except ImportError:
        print("[SPARK] pyspark not installed. Run: pip install pyspark")
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("HDB Resale Feature Engineering")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SPARK] Reading CSV: {RAW_CSV}")
    df = spark.read.csv(str(RAW_CSV), header=True, inferSchema=True)
    raw_count = df.count()
    print(f"[SPARK] Raw rows: {raw_count:,}")

    # ── Clean: dedup + filter ──────────────────────────────────────────────────
    df = df.dropDuplicates()
    df = df.filter(
        F.col("resale_price").isNotNull() & (F.col("resale_price") > 0)
        & F.col("floor_area_sqm").isNotNull() & (F.col("floor_area_sqm") > 0)
        & F.col("month").isNotNull()
        & F.col("storey_range").isNotNull()
        & F.col("remaining_lease").isNotNull()
        & F.col("lease_commence_date").isNotNull()
    )

    # ── Transform: feature engineering ────────────────────────────────────────
    # Date features
    df = df.withColumn("sale_year",  F.year(F.to_date("month", "yyyy-MM")))
    df = df.withColumn("sale_month", F.month(F.to_date("month", "yyyy-MM")))

    # Storey features: "10 TO 12" → storey_min=10, storey_max=12, storey_mid=11
    df = df.withColumn("storey_min",
        F.split(F.trim(F.col("storey_range")), " TO ")[0].cast(IntegerType()))
    df = df.withColumn("storey_max",
        F.split(F.trim(F.col("storey_range")), " TO ")[1].cast(IntegerType()))
    df = df.withColumn("storey_mid",
        ((F.col("storey_min") + F.col("storey_max")) / 2.0).cast(DoubleType()))

    # Remaining lease months: "61 years 04 months" → 732
    df = df.withColumn("lease_years",
        F.regexp_extract("remaining_lease", r"(\d+)\s+years?", 1).cast(IntegerType()))
    df = df.withColumn("lease_months_part",
        F.regexp_extract("remaining_lease", r"(\d+)\s+months?", 1).cast(IntegerType()))
    df = df.withColumn("remaining_lease_months",
        F.coalesce(F.col("lease_years"), F.lit(0)) * 12
        + F.coalesce(F.col("lease_months_part"), F.lit(0)))
    df = df.drop("lease_years", "lease_months_part")

    # Flat age
    df = df.withColumn("flat_age",
        F.col("sale_year") - F.col("lease_commence_date").cast(IntegerType()))

    # Price per sqm
    df = df.withColumn("price_per_sqm",
        F.col("resale_price") / F.col("floor_area_sqm"))

    clean_count = df.count()
    print(f"[SPARK] Clean rows: {clean_count:,}  (removed {raw_count - clean_count:,})")

    # ── Write ──────────────────────────────────────────────────────────────────
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(OUTPUT_PARQUET))
    print(f"[SPARK] Parquet saved: {OUTPUT_PARQUET}")

    spark.stop()


if __name__ == "__main__":
    run_spark_transform()
