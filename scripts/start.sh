#!/bin/bash
# ==============================================================
# Customer Data Platform - Startup Script
# Starts all services and verifies health
# ==============================================================

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "  Customer Data Platform - Starting Up"
echo "============================================"
echo ""

# Step 1: Build images
echo -e "${YELLOW}[1/6] Building Docker images...${NC}"
docker compose build
echo -e "${GREEN}✅ Images built${NC}"

# Step 2: Start infrastructure
echo -e "${YELLOW}[2/6] Starting PostgreSQL and MinIO...${NC}"
docker compose up -d postgres minio
sleep 15
echo -e "${GREEN}✅ Infrastructure started${NC}"

# Step 3: Initialize MinIO buckets
echo -e "${YELLOW}[3/6] Initializing MinIO buckets...${NC}"
docker compose up minio-init
echo -e "${GREEN}✅ MinIO buckets ready${NC}"

# Step 4: Initialize Airflow
echo -e "${YELLOW}[4/6] Initializing Airflow...${NC}"
docker compose up -d airflow-init
sleep 30
docker compose up -d airflow-webserver airflow-scheduler
echo -e "${GREEN}✅ Airflow started${NC}"

# Step 5: Start Metabase
echo -e "${YELLOW}[5/6] Starting Metabase...${NC}"
docker compose up -d metabase
echo -e "${GREEN}✅ Metabase started${NC}"

# Step 6: Generate sample data
echo -e "${YELLOW}[6/6] Generating sample data...${NC}"
docker compose run --rm data-generator
echo -e "${GREEN}✅ Sample data uploaded to MinIO${NC}"

echo ""
echo "============================================"
echo "  All Services Running!"
echo "============================================"
echo ""
echo "  Airflow:   http://localhost:8080  (admin / admin_pass_2026)"
echo "  MinIO:     http://localhost:9001  (minio_admin / minio_secure_pass_2026)"
echo "  Metabase:  http://localhost:3000  (configure on first visit)"
echo "  PostgreSQL: localhost:5432        (cdp_admin / cdp_secure_pass_2026)"
echo ""
echo "  Next steps:"
echo "  1. Trigger pipeline: docker compose exec airflow-webserver airflow dags trigger sales_data_pipeline"
echo "  2. View dashboards: open http://localhost:3000"
echo "  3. Check data: make db-shell"
echo ""
