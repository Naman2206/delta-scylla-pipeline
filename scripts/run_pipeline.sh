#!/usr/bin/env bash
# run_pipeline.sh – generate data, then run the ETL
# Run: chmod +x scripts/run_pipeline.sh && ./scripts/run_pipeline.sh

set -euo pipefail
GREEN='\033[0;32m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC}  $*"; }

info "Step 1/3 – Generate data into Delta Lake …"
docker exec spark-master \
  spark-submit \
    --master local[*] \
    --packages io.delta:delta-core_2.12:2.4.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /opt/spark_jobs/../scripts/generate_data.py

info "Step 2/3 – Run Spark ETL (transform + load to ScyllaDB) …"
docker exec spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:2.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.cassandra.connection.host=scylladb \
    --conf spark.cassandra.connection.port=9042 \
    /opt/spark_jobs/spark_etl.py

info "Step 3/3 – Verify ScyllaDB …"
docker exec scylladb cqlsh -e \
  "SELECT COUNT(*) FROM transactions_ks.daily_customer_totals;"

info ""
info "✅  Pipeline complete!"
info "   Open Airflow at http://localhost:8080 to trigger the DAG manually."
