"""
pipeline_dag.py
───────────────
Airflow DAG that orchestrates the full ETL pipeline:
  Task 1 (extract_delta)    – verify Delta Lake table exists & get row count
  Task 2 (spark_transform)  – submit Spark ETL job (transform + load)
  Task 3 (quality_check)    – validate rows landed in ScyllaDB
  Task 4 (notify_success)   – log summary on success

Schedule : daily at 01:00 UTC
Retries  : 2 per task, 5-minute delay
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── DAG default args ──────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── Config ────────────────────────────────────────────────────────────────────
DELTA_PATH      = "/opt/spark/delta-lake/customer_transactions"
SCYLLA_HOST     = "scylladb"
SCYLLA_KEYSPACE = "transactions_ks"
SCYLLA_TABLE    = "daily_customer_totals"
MIN_EXPECTED_ROWS = 10   # quality gate threshold


# ── Task functions ────────────────────────────────────────────────────────────

def check_delta_table(**context) -> dict:
    """
    Task 1 – Verify the Delta Lake source table exists and has data.
    Pushes row count to XCom for downstream quality gating.
    """
    import os
    from pyspark.sql import SparkSession

    log.info("Checking Delta Lake table at: %s", DELTA_PATH)

    if not os.path.isdir(DELTA_PATH):
        raise FileNotFoundError(
            f"Delta Lake table not found at {DELTA_PATH}. "
            "Run generate_data.py first."
        )

    spark = (
        SparkSession.builder
        .appName("AirflowExtractCheck")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df    = spark.read.format("delta").load(DELTA_PATH)
        count = df.count()
        log.info("Delta table row count: %d", count)

        if count == 0:
            raise ValueError("Delta Lake source table is empty!")

        # Push to XCom
        context["ti"].xcom_push(key="source_row_count", value=count)

        # Show Delta history
        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(spark, DELTA_PATH)
        log.info("Delta table history:")
        dt.history().show(truncate=False)

        return {"status": "ok", "row_count": count}
    finally:
        spark.stop()


def run_spark_etl(**context) -> None:
    """
    Task 2 – Run the Spark ETL job inline (PySpark in local mode).
    In production this would be a SparkSubmitOperator.
    """
    import subprocess, sys

    log.info("Submitting Spark ETL job …")

    result = subprocess.run(
        [
            sys.executable,
            "/opt/airflow/spark_jobs/spark_etl.py",
        ],
        capture_output=True,
        text=True,
        timeout=1800,   # 30-minute hard limit
    )

    log.info("STDOUT:\n%s", result.stdout[-4000:])   # tail to avoid huge logs

    if result.returncode != 0:
        log.error("STDERR:\n%s", result.stderr[-4000:])
        raise RuntimeError(
            f"Spark ETL job failed with return code {result.returncode}"
        )

    log.info("Spark ETL job completed ✅")


def quality_check_scylla(**context) -> str:
    """
    Task 3 – Validate that rows have arrived in ScyllaDB.
    Returns branch name: 'notify_success' or 'notify_failure'.
    """
    try:
        from cassandra.cluster import Cluster
        from cassandra.policies import RoundRobinPolicy

        cluster = Cluster(
            [SCYLLA_HOST],
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4,
        )
        session = cluster.connect(SCYLLA_KEYSPACE)

        row = session.execute(
            f"SELECT COUNT(*) FROM {SCYLLA_TABLE}"
        ).one()
        count = row[0]

        log.info("ScyllaDB row count in %s.%s: %d", SCYLLA_KEYSPACE, SCYLLA_TABLE, count)

        # Sample some rows for verification
        rows = session.execute(
            f"SELECT * FROM {SCYLLA_TABLE} LIMIT 5"
        )
        for r in rows:
            log.info("  Sample row: %s", r)

        session.shutdown()
        cluster.shutdown()

        context["ti"].xcom_push(key="scylla_row_count", value=count)

        if count < MIN_EXPECTED_ROWS:
            log.warning(
                "Quality gate FAILED: only %d rows (min %d)",
                count, MIN_EXPECTED_ROWS
            )
            return "notify_failure"

        log.info("Quality gate PASSED ✅")
        return "notify_success"

    except Exception as e:
        log.error("ScyllaDB quality check error: %s", e)
        raise


def notify_success_fn(**context) -> None:
    src_count = context["ti"].xcom_pull(
        key="source_row_count", task_ids="extract_delta"
    )
    dst_count = context["ti"].xcom_pull(
        key="scylla_row_count", task_ids="quality_check"
    )
    log.info(
        "✅  Pipeline SUCCESS | run_id=%s | source_rows=%s | scylla_rows=%s",
        context["run_id"], src_count, dst_count,
    )


def notify_failure_fn(**context) -> None:
    log.error("❌  Pipeline quality gate FAILED for run_id=%s", context["run_id"])
    raise ValueError("Pipeline quality gate failed – see logs for details.")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="customer_transactions_pipeline",
    description="ETL: Delta Lake → Spark → ScyllaDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",   # daily at 01:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "delta-lake", "spark", "scylladb"],
) as dag:

    # Task 1 – Extract / validate source
    extract_delta = PythonOperator(
        task_id="extract_delta",
        python_callable=check_delta_table,
        doc_md="""
        **Extract**
        Verifies the Delta Lake `customer_transactions` table exists and is
        non-empty. Pushes `source_row_count` to XCom.
        """,
    )

    # Task 2 – Spark ETL (transform + load)
    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=run_spark_etl,
        execution_timeout=timedelta(minutes=25),
        doc_md="""
        **Transform & Load**
        Runs spark_etl.py which:
        - Deduplicates on transaction_id
        - Filters amount > 0
        - Extracts transaction_date
        - Aggregates daily totals per customer
        - Upserts into ScyllaDB daily_customer_totals
        """,
    )

    # Task 3 – Quality check (branches)
    quality_check = BranchPythonOperator(
        task_id="quality_check",
        python_callable=quality_check_scylla,
        doc_md="Counts rows in ScyllaDB and branches on pass/fail.",
    )

    # Branch: success
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    # Branch: failure
    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Pipeline end marker
    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    (
        extract_delta
        >> spark_transform
        >> quality_check
        >> [notify_success, notify_failure]
        >> pipeline_end
    )
