#!/usr/bin/env python3
"""
spark_etl.py
────────────
Spark ETL pipeline:
  1. Extract   – read customer_transactions Delta Lake table
  2. Transform – deduplicate, validate, add transaction_date, aggregate
  3. Load      – upsert daily_customer_totals into ScyllaDB

Spark-submit command (from inside spark-master container):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages io.delta:delta-core_2.12:2.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.cassandra.connection.host=scylladb \
      --conf spark.cassandra.connection.port=9042 \
      /opt/spark_jobs/spark_etl.py
"""

import os
import sys
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

# ── Config (can be overridden by env vars) ────────────────────────────────────
DELTA_LAKE_PATH  = os.getenv("DELTA_LAKE_PATH", "/opt/spark/delta-lake")
TABLE_PATH       = f"{DELTA_LAKE_PATH}/customer_transactions"
SCYLLA_HOST      = os.getenv("SCYLLA_HOST",      "scylladb")
SCYLLA_PORT      = os.getenv("SCYLLA_PORT",      "9042")
SCYLLA_KEYSPACE  = os.getenv("SCYLLA_KEYSPACE",  "transactions_ks")
SCYLLA_TABLE     = "daily_customer_totals"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("spark_etl")


# ── Spark Session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CustomerTransactionsETL")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
        )
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.cassandra.connection.host", SCYLLA_HOST)
        .config("spark.cassandra.connection.port", SCYLLA_PORT)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Step 1: Extract ───────────────────────────────────────────────────────────

def extract(spark: SparkSession) -> DataFrame:
    """Read the customer_transactions Delta Lake table."""
    log.info("EXTRACT – reading Delta table from %s", TABLE_PATH)
    df = spark.read.format("delta").load(TABLE_PATH)
    log.info("  Raw row count: %d", df.count())
    df.printSchema()
    return df


# ── Step 2: Transform ─────────────────────────────────────────────────────────

def transform(df: DataFrame) -> DataFrame:
    """
    Transformations applied in order:
      a) Deduplication  – keep first occurrence of each transaction_id
      b) Data validation – drop rows where amount <= 0
      c) Date extraction – add transaction_date column (DATE from timestamp)
      d) Aggregation     – sum daily amounts per (customer_id, transaction_date)
    """

    # ── a) Deduplication ─────────────────────────────────────────────────────
    log.info("TRANSFORM a) Deduplication on transaction_id …")
    before = df.count()
    df_deduped = df.dropDuplicates(["transaction_id"])
    after_dedup = df_deduped.count()
    log.info("  Rows before: %d  |  after dedup: %d  |  removed: %d",
             before, after_dedup, before - after_dedup)

    # ── b) Data Validation ───────────────────────────────────────────────────
    log.info("TRANSFORM b) Filtering out amount <= 0 …")
    df_valid = df_deduped.filter(F.col("amount") > 0)
    after_valid = df_valid.count()
    log.info("  Rows after validation filter: %d  |  removed: %d",
             after_valid, after_dedup - after_valid)

    # ── c) Date Extraction ───────────────────────────────────────────────────
    log.info("TRANSFORM c) Extracting transaction_date from timestamp …")
    df_dated = df_valid.withColumn(
        "transaction_date",
        F.to_date(F.col("timestamp"))
    )

    # ── d) Aggregation ───────────────────────────────────────────────────────
    log.info("TRANSFORM d) Aggregating daily totals per customer …")
    df_agg = (
        df_dated
        .groupBy("customer_id", "transaction_date")
        .agg(
            F.round(F.sum(F.col("amount").cast(FloatType())), 2)
             .alias("daily_total")
        )
        .orderBy("customer_id", "transaction_date")
    )

    row_count = df_agg.count()
    log.info("  Aggregated rows: %d", row_count)

    # Quality gate: fail the job if output is suspiciously small
    if row_count == 0:
        raise ValueError("QUALITY GATE FAILED: aggregated DataFrame is empty!")
    if row_count < 10:
        log.warning("WARNING: Very few aggregated rows (%d). Check input data.", row_count)

    log.info("  Sample aggregated output:")
    df_agg.show(10, truncate=False)

    return df_agg


# ── Step 3: Load ──────────────────────────────────────────────────────────────

def load(df: DataFrame) -> None:
    """
    Upsert aggregated rows into ScyllaDB daily_customer_totals table.
    ScyllaDB / Cassandra treats INSERT as upsert (last-write-wins).
    """
    log.info(
        "LOAD – writing %d rows to ScyllaDB %s.%s …",
        df.count(), SCYLLA_KEYSPACE, SCYLLA_TABLE,
    )

    (
        df.write
          .format("org.apache.spark.sql.cassandra")
          .mode("append")           # Cassandra INSERT = upsert on PK collision
          .options(
              table=SCYLLA_TABLE,
              keyspace=SCYLLA_KEYSPACE,
          )
          .save()
    )

    log.info("LOAD – write complete ✅")


# ── Data Quality Checks ───────────────────────────────────────────────────────

def quality_check_post_load(spark: SparkSession, expected_rows: int) -> None:
    """Read back a sample from ScyllaDB and verify row count."""
    log.info("QUALITY CHECK – reading back from ScyllaDB …")
    try:
        df_check = (
            spark.read
                 .format("org.apache.spark.sql.cassandra")
                 .options(table=SCYLLA_TABLE, keyspace=SCYLLA_KEYSPACE)
                 .load()
        )
        actual = df_check.count()
        log.info("  Rows in ScyllaDB table: %d (expected >= %d)", actual, expected_rows)
        df_check.show(5, truncate=False)
        if actual < expected_rows:
            log.warning("  Row count lower than expected – possible partial write.")
    except Exception as e:
        log.error("  Quality check failed: %s", e)


# ── Delta Lake Versioning Demo ────────────────────────────────────────────────

def show_delta_history(spark: SparkSession) -> None:
    """Show Delta table version history (time-travel demo)."""
    from delta.tables import DeltaTable
    log.info("DELTA HISTORY:")
    dt = DeltaTable.forPath(spark, TABLE_PATH)
    dt.history().select("version", "timestamp", "operation").show(truncate=False)

    log.info("TIME TRAVEL – reading version 0:")
    spark.read.format("delta").option("versionAsOf", 0).load(TABLE_PATH).show(5)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Spark ETL – Customer Transactions Pipeline")
    log.info("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # E
        raw_df = extract(spark)

        # T
        aggregated_df = transform(raw_df)
        aggregated_df.cache()
        expected_rows = aggregated_df.count()

        # L
        load(aggregated_df)

        # Post-load quality check
        quality_check_post_load(spark, expected_rows)

        # Delta versioning demo
        show_delta_history(spark)

        log.info("Pipeline completed successfully ✅")

    except Exception as e:
        log.error("Pipeline FAILED: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
