#!/usr/bin/env bash
# setup.sh – one-shot local environment setup
# Run: chmod +x scripts/setup.sh && ./scripts/setup.sh

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

info "=== Data Engineering Assignment 2 – Setup ==="

# 1. Create directories
info "Creating directory structure …"
mkdir -p data/delta-lake/customer_transactions
mkdir -p sample_data
chmod -R 777 data/   # ensure containers can write

# 2. Check Docker
if ! command -v docker &>/dev/null; then
  warn "Docker not found. Please install Docker Desktop first."
  exit 1
fi
if ! command -v docker-compose &>/dev/null && ! docker compose version &>/dev/null 2>&1; then
  warn "docker-compose not found."
  exit 1
fi
info "Docker: $(docker --version)"

# 3. Pull images (optional, speeds up first start)
info "Pulling Docker images (this may take a few minutes) …"
docker pull bitnami/spark:3.4.1
docker pull apache/airflow:2.8.1-python3.10
docker pull scylladb/scylla:5.4
docker pull postgres:15

# 4. Start services
info "Starting services with docker-compose …"
docker-compose up -d postgres scylladb spark-master spark-worker

info "Waiting 30s for ScyllaDB to be ready …"
sleep 30

# 5. Init Airflow
info "Initializing Airflow …"
docker-compose up -d airflow-init
info "Waiting 60s for Airflow init to complete …"
sleep 60

# 6. Start Airflow web + scheduler
docker-compose up -d airflow-webserver airflow-scheduler

info ""
info "=== Services started ==="
info "  Airflow UI  : http://localhost:8080  (admin / admin)"
info "  Spark UI    : http://localhost:8081"
info "  ScyllaDB    : localhost:9042 (CQL)"
info ""
info "Next step: run  ./scripts/run_pipeline.sh"
