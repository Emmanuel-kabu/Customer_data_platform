# Customer Data Platform (CDP)

Production-style mini data platform for batch e-commerce analytics using Docker Compose, Airflow, PostgreSQL, MinIO, and Metabase.

## What This Project Does

- Generates daily/batch sample sales data.
- Lands raw files in MinIO (`raw-data` bucket).
- Runs ETL in Airflow into PostgreSQL staging and warehouse schemas.
- Maintains SCD Type 2 dimensions and incremental fact loading.
- Builds analytics views for BI use cases.
- Provisions Metabase collections, dashboards, and cards via code.
- Tracks data quality and pipeline health in audit schemas.

## Architecture

`Data Generator -> MinIO (raw-data) -> Airflow -> PostgreSQL (staging -> warehouse -> analytics) -> Metabase`

## Data Flow

1. `data_generation_pipeline` creates a full timestamped batch (`customers/products/sales`) and uploads to MinIO.
2. It automatically triggers `sales_data_pipeline`.
3. `sales_data_pipeline` extracts exactly one complete batch (oldest shared timestamp).
4. Pre-load schema/data validation runs before staging load.
5. Clean data loads to staging, then transforms to warehouse dimensions/facts.
6. Analytics models are applied (`ensure_analytics_models` task).
7. DQ checks run and processed raw files are archived.
8. Metabase dashboards read from `analytics` views and update on new data.

## Tech Stack

| Component | Technology | Purpose | Default Port |
|---|---|---|---|
| Orchestration | Apache Airflow 2.8 | DAG scheduling and ETL orchestration | `8080` |
| Database | PostgreSQL 15 | Staging, warehouse, analytics, audit | `5432` |
| Object Storage | MinIO | Raw/processed/archive object storage | `9000`, `9001` |
| BI | Metabase | Dashboarding and analytics exploration | `3001` |
| Runtime | Python 3.11 | ETL, validation, automation scripts | - |
| Containers | Docker Compose | Service lifecycle | - |

## Project Structure

```text
Customer_data_platform/
|-- dags/
|   |-- data_generation_dag.py
|   |-- sales_data_pipeline.py
|   `-- data_flow_validation_dag.py
|-- scripts/
|   |-- apply_analytics_models.py
|   |-- setup_metabase.py
|   |-- sql/
|   |   `-- analytics_models.sql
|   `-- metabase_automation/
|       |-- __init__.py
|       |-- settings.py
|       |-- client.py
|       |-- catalog.py
|       |-- specs.py
|       `-- provisioner.py
|-- config/
|   |-- business_questions.yml
|   |-- schema_contracts.yml
|   `-- data_quality_rules.yml
|-- docker/
|   |-- airflow/
|   |-- data-generator/
|   `-- postgres/
|-- plugins/
|-- tests/
|-- docker-compose-platform.yml
|-- Makefile
`-- .env
```

## Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Make (recommended)

### 1. Configure Environment

Update `.env` if needed. Defaults work for local runs.

### 2. Start Platform

```bash
make up
make status
```

### 3. Access Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | `http://localhost:8080` | `admin / admin_pass_2026` |
| MinIO Console | `http://localhost:9001` | `minio_admin / minio_secure_pass_2026` |
| Metabase | `http://localhost:3001` | first-run via script or setup UI |

### 4. Generate and Process Data

```bash
make trigger-generate
```

This triggers `data_generation_pipeline`, which then triggers `sales_data_pipeline`.

### 5. Apply Analytics Models and Provision Dashboards

```bash
make analytics-models
make setup-metabase
```

## Airflow DAGs

| DAG ID | Schedule | Purpose |
|---|---|---|
| `data_generation_pipeline` | Daily (`0 0 * * *`) | Generate and upload batch files to MinIO |
| `sales_data_pipeline` | Every 6 hours (`0 */6 * * *`) | ETL from raw files to warehouse + analytics + DQ |
| `data_flow_validation` | Every 12 hours (`0 */12 * * *`) | End-to-end data flow validation checks |

## Database Layers

- `staging`: raw landing tables (`customers_raw`, `products_raw`, `sales_raw`)
- `warehouse`: SCD Type 2 dimensions + sales fact
- `analytics`: BI-oriented views (executive, finance, customer, product, ops, forecasting)
- `audit`: pipeline runs, DQ runs, and pre-load rejections

## Analytics Models (Scripted)

Analytics SQL is maintained in:

- `scripts/sql/analytics_models.sql`

Applied via:

- `scripts/apply_analytics_models.py`
- `make analytics-models`

`sales_data_pipeline` also runs this automatically through `ensure_analytics_models`.

## Metabase Automation (Modular)

The Metabase provisioning system is modular and idempotent:

- `scripts/setup_metabase.py`: thin entrypoint
- `scripts/metabase_automation/settings.py`: env-based settings
- `scripts/metabase_automation/client.py`: API client/upsert operations
- `scripts/metabase_automation/specs.py`: dashboard/card definitions
- `scripts/metabase_automation/catalog.py`: business-question catalog loader
- `scripts/metabase_automation/provisioner.py`: orchestration workflow

Business question catalog:

- `config/business_questions.yml`

## Dashboard Suite

Provisioned dashboards:

1. Executive Command Center
2. Revenue and Profit Intelligence
3. Customer Growth and Retention
4. Product and Inventory Intelligence
5. Operations and Data Quality
6. Forecasting and Planning

Forecasting dashboard uses:

- `analytics.revenue_orders_forecast_30d`
- `analytics.revenue_orders_actual_vs_forecast`
- `analytics.revenue_orders_forecast_summary`

## Useful Commands

```bash
# Lifecycle
make up
make down
make down-clean
make restart
make status

# Logs
make logs
make logs-airflow
make logs-postgres
make logs-minio
make logs-metabase

# Pipelines
make trigger-generate
make trigger-pipeline
make trigger-validate

# Models and BI
make analytics-models
make setup-metabase

# Validation & quality
make schema-validate
make dq-report
make pre-load-report

# Tests
make test
make test-unit
make test-integration
```

## Testing

Run all tests:

```bash
make test
```

Run targeted suites:

```bash
make test-unit
make test-integration
make test-dq
make test-schema
```

## Troubleshooting

```bash
# Check running containers
make status

# Airflow DAG import errors
docker compose -f docker-compose-platform.yml exec airflow-webserver airflow dags list-import-errors

# Trigger DAG manually
docker compose -f docker-compose-platform.yml exec airflow-webserver airflow dags trigger sales_data_pipeline

# Check latest DAG runs
docker compose -f docker-compose-platform.yml exec airflow-webserver airflow dags list-runs -d sales_data_pipeline

# Check row counts
docker compose -f docker-compose-platform.yml exec postgres \
  psql -U cdp_admin -d customer_data_platform -c "SELECT COUNT(*) FROM warehouse.fact_sales;"
```

## License

Educational and assessment use.
