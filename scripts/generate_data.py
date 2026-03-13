#!/usr/bin/env python3
"""
generate_data.py
────────────────
Generates synthetic customer transaction data and writes it as a
Delta Lake table at ./data/delta-lake/customer_transactions.

Run locally (requires: pyspark, delta-spark, faker):
    pip install pyspark==3.4.1 delta-spark==2.4.0 faker pandas
    python scripts/generate_data.py

Inside the Spark container:
    spark-submit \
      --packages io.delta:delta-core_2.12:2.4.0 \
      /opt/spark_jobs/../scripts/generate_data.py
"""

import os
import uuid
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
import pandas as pd

# ── Spark + Delta Lake setup ──────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType
)
import pyspark.sql.functions as F

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "./data/delta-lake")
TABLE_PATH      = f"{DELTA_LAKE_PATH}/customer_transactions"
NUM_RECORDS     = 1200   # base records (before duplicates)
NUM_CUSTOMERS   = 80
NUM_MERCHANTS   = 25
DAYS_BACK       = 60     # transactions spread over last 60 days

fake = Faker()
random.seed(42)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DataGenerator")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def random_timestamp(days_back: int = DAYS_BACK) -> datetime:
    """Random UTC timestamp within the last `days_back` days."""
    now   = datetime.now(timezone.utc)
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return now - delta


def generate_records(n: int) -> list[dict]:
    customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS + 1)]
    merchants    = [f"STORE_{i}" for i in range(1, NUM_MERCHANTS + 1)]
    # add a few realistic merchant names
    merchants   += ["ONLINE_SHOP", "GAS_STATION", "SUPERMARKET", "PHARMACY", "RESTAURANT"]

    records = []
    for _ in range(n):
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id":    random.choice(customer_ids),
            "amount":         round(random.uniform(1.0, 2000.0), 2),
            "timestamp":      random_timestamp(),
            "merchant":       random.choice(merchants),
        })
    return records


def inject_data_quality_issues(records: list[dict]) -> list[dict]:
    """
    Inject realistic data quality issues so the ETL pipeline has work to do:
      - ~5 % duplicate transaction_ids
      - ~3 % zero-amount transactions
      - ~2 % negative amounts (refunds / errors)
    """
    dirty = records.copy()

    # Duplicates: re-use existing transaction_ids
    dup_count = max(1, len(records) // 20)
    originals = random.sample(records, dup_count)
    for orig in originals:
        dup = orig.copy()
        dup["timestamp"] = random_timestamp()   # slightly different ts
        dirty.append(dup)

    # Zero amounts
    zero_count = max(1, len(records) // 33)
    for rec in random.sample(dirty, zero_count):
        rec["amount"] = 0.0

    # Negative amounts
    neg_count = max(1, len(records) // 50)
    for rec in random.sample(dirty, neg_count):
        rec["amount"] = round(random.uniform(-500.0, -0.01), 2)

    random.shuffle(dirty)
    return dirty


def main():
    print("=" * 60)
    print("  Delta Lake – Data Generator")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Generate records
    print(f"\n[1/4] Generating {NUM_RECORDS} base records ...")
    records = generate_records(NUM_RECORDS)

    # 2. Inject data quality issues
    print("[2/4] Injecting data quality issues (dupes, zero/neg amounts) ...")
    dirty_records = inject_data_quality_issues(records)
    print(f"      Total rows incl. dirty data: {len(dirty_records)}")

    # 3. Create Spark DataFrame
    print("[3/4] Creating Spark DataFrame ...")
    schema = StructType([
        StructField("transaction_id", StringType(),    False),
        StructField("customer_id",    StringType(),    False),
        StructField("amount",         FloatType(),     False),
        StructField("timestamp",      TimestampType(), False),
        StructField("merchant",       StringType(),    False),
    ])

    pdf = pd.DataFrame(dirty_records)
    pdf["timestamp"] = pd.to_datetime(pdf["timestamp"], utc=True)

    df = spark.createDataFrame(pdf, schema=schema)
    print(f"      Schema: {df.schema.simpleString()}")
    print(f"      Partitions: {df.rdd.getNumPartitions()}")

    # 4. Write as Delta Lake table
    print(f"[4/4] Writing Delta Lake table → {TABLE_PATH}")
    os.makedirs(TABLE_PATH, exist_ok=True)

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(TABLE_PATH)
    )

    # ── Verify Delta table versioning ─────────────────────────────────────────
    print("\n── Delta table verification ─────────────────────────────────")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, TABLE_PATH)

    print("\n  Table history (versions):")
    dt.history().select("version", "timestamp", "operation", "operationMetrics") \
      .show(truncate=False)

    print("\n  Row counts:")
    total = spark.read.format("delta").load(TABLE_PATH).count()
    print(f"    Total rows written : {total}")

    print("\n  Sample data (5 rows):")
    spark.read.format("delta").load(TABLE_PATH).show(5, truncate=False)

    # ── Write sample CSV for reference ───────────────────────────────────────
    sample_csv_path = "./sample_data/sample_transactions.csv"
    os.makedirs("./sample_data", exist_ok=True)
    pdf.head(20).to_csv(sample_csv_path, index=False)
    print(f"\n  Sample CSV written → {sample_csv_path}")

    print("\n✅  Data generation complete.\n")
    spark.stop()


if __name__ == "__main__":
    main()
