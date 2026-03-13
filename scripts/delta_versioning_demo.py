#!/usr/bin/env python3
"""
delta_versioning_demo.py
─────────────────────────
Demonstrates Delta Lake table creation and time-travel / versioning features.

Run:
    python scripts/delta_versioning_demo.py
"""

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

DELTA_PATH = os.getenv("DELTA_LAKE_PATH", "./data/delta-lake") + "/customer_transactions"


def build_spark():
    return (
        SparkSession.builder
        .appName("DeltaVersioningDemo")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    from delta.tables import DeltaTable

    print("\n" + "=" * 60)
    print("  Delta Lake – Versioning & Time Travel Demo")
    print("=" * 60)

    # ── 1. Show current table history ─────────────────────────────────────────
    print("\n[1] Table History (all versions):")
    dt = DeltaTable.forPath(spark, DELTA_PATH)
    dt.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)

    # ── 2. Read latest version ────────────────────────────────────────────────
    print("\n[2] Latest version row count:")
    df_latest = spark.read.format("delta").load(DELTA_PATH)
    print(f"    Rows: {df_latest.count()}")
    df_latest.show(5, truncate=False)

    # ── 3. Time travel – read version 0 ──────────────────────────────────────
    print("\n[3] Time travel → version 0:")
    df_v0 = (
        spark.read.format("delta")
             .option("versionAsOf", 0)
             .load(DELTA_PATH)
    )
    print(f"    Rows in version 0: {df_v0.count()}")
    df_v0.show(5, truncate=False)

    # ── 4. Simulate an UPDATE (creates new version) ───────────────────────────
    print("\n[4] Simulating UPDATE (multiply amounts by 1.01 for one customer) …")
    dt.update(
        condition=F.col("customer_id") == "C00001",
        set={"amount": F.round(F.col("amount") * 1.01, 2)},
    )
    print("    Update applied – new version created.")
    dt.history().select("version", "timestamp", "operation").show(5)

    # ── 5. Simulate a DELETE (creates another version) ────────────────────────
    print("\n[5] Simulating DELETE (remove negative amounts) …")
    dt.delete(condition=F.col("amount") < 0)
    print("    Delete applied – new version created.")
    dt.history().select("version", "timestamp", "operation").show(5)

    # ── 6. MERGE (upsert) example ─────────────────────────────────────────────
    print("\n[6] MERGE (upsert) example with new batch …")
    new_batch = spark.createDataFrame([
        ("UPSERT-001", "C00001", 999.99,
         __import__("datetime").datetime.utcnow(), "DEMO_STORE"),
    ], ["transaction_id", "customer_id", "amount", "timestamp", "merchant"])

    (
        dt.alias("target")
          .merge(
              new_batch.alias("source"),
              "target.transaction_id = source.transaction_id",
          )
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
    )
    print("    Merge complete – new version created.")
    dt.history().select("version", "timestamp", "operation").show(truncate=False)

    # ── 7. Vacuum (clean old files, dry run) ──────────────────────────────────
    print("\n[7] VACUUM dry-run (would delete files older than 168h) …")
    spark.sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = false")
    dt.vacuum(168)   # keep 7 days of history

    print("\n✅  Versioning demo complete.\n")
    spark.stop()


if __name__ == "__main__":
    main()
