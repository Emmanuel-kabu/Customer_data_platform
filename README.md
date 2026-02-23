# Customer Data Platform (CDP)

A production-grade mini data platform built with Docker Compose that collects, processes, stores, and visualizes sales data. Implements best practices including SCD Type 2 for time travel, hash-based incremental loading, automated data quality checks, and Slack notifications.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Customer Data Platform (CDP)                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────────┐    │
│  │   Data       │    │   MinIO       │    │   PostgreSQL          │    │
│  │  Generator   │───▶│  (S3 Storage) │    │   Data Warehouse      │    │
│  │  (Python)    │    │   raw-data/   │    │                        │    │
│  └─────────────┘    │   archive/    │    │  ┌──────────────────┐  │    │
│                      └──────┬───────┘    │  │ staging schema    │  │    │
│                             │            │  │ (raw landing)     │  │    │
│                             ▼            │  ├──────────────────┤  │    │
│                      ┌──────────────┐    │  │ warehouse schema  │  │    │
│                      │   Apache      │───▶│  │ (SCD Type 2)     │  │    │
│                      │   Airflow     │    │  ├──────────────────┤  │    │
│                      │  (Scheduler)  │    │  │ analytics schema  │  │    │
│                      │              │    │  │ (views/reports)   │  │    │
│                      └──────────────┘    │  ├──────────────────┤  │    │
│                             │            │  │ audit schema      │  │    │
│                      ┌──────┴───────┐    │  │ (pipeline logs)   │  │    │
│                      │  Slack/Email  │    │  └──────────────────┘  │    │
│                      │ Notifications │    └──────────┬───────────┘    │
│                      └──────────────┘               │                │
│                                                      ▼                │
│                                              ┌──────────────┐        │
│                                              │   Metabase     │        │
│                                              │  (Dashboards)  │        │
│                                              │   port 3000    │        │
│                                              └──────────────┘        │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
CSV Generation → MinIO Upload → Airflow Extract → Staging Tables
    → SCD Type 2 Transform → Warehouse (Facts & Dimensions)
    → Analytics Views → Metabase Dashboards
```

1. **Ingestion**: Data Generator creates realistic sales, customer, and product CSV files and uploads to MinIO `raw-data` bucket
2. **Processing**: Airflow DAG extracts CSVs from MinIO, loads into PostgreSQL staging tables
3. **Transformation**: SCD Type 2 incremental loading into warehouse dimension/fact tables using hash-based change detection
4. **Quality**: Automated data quality checks (row counts, nulls, uniqueness, referential integrity)
5. **Archival**: Processed files moved to `archive-data` bucket
6. **Visualization**: Metabase connects to warehouse for dashboards and reports
7. **Monitoring**: Slack/email notifications on pipeline success, failure, or retry

## Tech Stack

| Component | Technology | Purpose | Port |
|-----------|-----------|---------|------|
| Database | PostgreSQL 15 | Data warehouse with star schema | 5432 |
| Object Storage | MinIO | S3-compatible file storage | 9000/9001 |
| Orchestration | Apache Airflow 2.8 | ETL pipeline scheduling | 8080 |
| Visualization | Metabase | Business intelligence dashboards | 3000 |
| Language | Python 3.11 | Data processing scripts | - |
| Containerization | Docker Compose | Service orchestration | - |
| CI/CD | GitHub Actions | Automated testing & deployment | - |

## Project Structure

```
Customer_data_platform/
├── .github/workflows/
│   ├── ci.yml                    # CI: lint, test, build
│   └── cd.yml                    # CD: deploy and validate data flow
├── dags/
│   ├── sales_data_pipeline.py    # Main ETL DAG (every 6 hours)
│   ├── data_generation_dag.py    # Data generation DAG (daily)
│   └── data_flow_validation_dag.py # Validation DAG (every 12 hours)
├── docker/
│   ├── airflow/Dockerfile
│   ├── data-generator/Dockerfile
│   └── postgres/
│       ├── init-multiple-dbs.sh
│       └── init-schema.sql       # Star schema with SCD Type 2
├── plugins/
│   ├── slack_notifications.py    # Slack integration
│   └── data_quality.py           # Quality check framework
├── scripts/
│   ├── generate_data.py          # Faker-based data generator
│   ├── db_helper.py              # DB utilities with incremental loading
│   ├── minio_helper.py           # MinIO client wrapper
│   └── setup_metabase.py         # Auto-configure Metabase
├── tests/
│   ├── conftest.py               # Shared test fixtures
│   ├── test_data_generation.py   # Data generation tests
│   ├── test_db_helper.py         # Database helper tests
│   ├── test_minio_helper.py      # MinIO helper tests
│   ├── test_data_quality.py      # Quality check tests
│   ├── test_slack_notifications.py # Notification tests
│   └── test_integration.py       # End-to-end integration tests
├── data/                          # Local data backup
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

## Quick Start

### Prerequisites

- Docker & Docker Compose (v2.0+)
- Git
- 8GB+ RAM recommended

### 1. Clone the Repository

```bash
git clone https://github.com/Emmanuel-kabu/Customer_data_platform.git
cd Customer_data_platform
```

### 2. Configure Environment

```bash
# Review and update .env file
cp .env .env.backup
# Edit .env with your preferred settings (defaults work out of the box)
```

### 3. Start All Services

```bash
# Build and start everything
docker compose up -d --build

# Watch the logs
docker compose logs -f
```

### 4. Verify Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin_pass_2026 |
| MinIO Console | http://localhost:9001 | minio_admin / minio_secure_pass_2026 |
| Metabase | http://localhost:3000 | Setup on first visit |
| PostgreSQL | localhost:5432 | cdp_admin / cdp_secure_pass_2026 |

### 5. Generate Sample Data

```bash
# Run the data generator
docker compose run --rm data-generator

# Or trigger via Airflow UI
# Navigate to http://localhost:8080 → data_generation_pipeline → Trigger
```

### 6. Run the ETL Pipeline

```bash
# Trigger manually via Airflow CLI
docker compose exec airflow-webserver airflow dags trigger sales_data_pipeline

# Or wait for the scheduled run (every 6 hours)
```

### 7. View Dashboards

Open http://localhost:3000 in your browser to access Metabase dashboards.

```bash
# Optional: Auto-configure Metabase with preset questions
python scripts/setup_metabase.py
```

## Best Practices Implemented

### Database (PostgreSQL)
- **Star Schema**: Fact and dimension tables for optimal query performance
- **SCD Type 2**: Slowly Changing Dimensions for historical tracking / time travel
- **Hash-based Incremental Loading**: MD5 hash comparison detects changes efficiently
- **Staging Layer**: Raw data landing zone before transformation
- **Analytics Views**: Pre-computed views for dashboard performance
- **Audit Trail**: Pipeline run logging in `audit` schema
- **Indexing**: Strategic indexes on join keys, business keys, and hashes

### Orchestration (Airflow)
- **Scheduled DAGs**: Automated runs (6-hour, 12-hour, daily schedules)
- **Retry with Exponential Backoff**: 3 retries with increasing delays
- **Email Notifications**: On failure and retry events
- **Slack Integration**: Real-time alerts to Slack channels
- **Task Dependencies**: Proper DAG structure with parallel where possible
- **XCom for Data Passing**: Inter-task communication
- **Idempotent Operations**: Safe to re-run without data duplication
- **Connection Management**: Airflow connections for PostgreSQL and MinIO

### Storage (MinIO)
- **Bucket Organization**: raw-data, processed-data, archive-data
- **File Archival**: Processed files moved to archive bucket
- **Health Checks**: Container-level health monitoring

### Visualization (Metabase)
- **Auto-Setup**: Script for automated configuration
- **Pre-built Questions**: 5 starter dashboard questions
- **Analytics Views**: Optimized database views for dashboards

### Testing
- **Unit Tests**: Individual module testing with mocks
- **Integration Tests**: End-to-end data flow validation
- **Quality Checks**: Automated data quality assertions
- **CI Pipeline**: Automated testing on every commit

### CI/CD
- **CI**: Lint → Unit Tests → Docker Build → Compose Validation → Integration Tests
- **CD**: Deploy → Health Checks → Data Flow Validation → Cleanup
- **Data Flow Validation**: Automated end-to-end checks in pipeline

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `sales_data_pipeline` | Every 6 hours | Main ETL: MinIO → Staging → Warehouse |
| `data_generation_pipeline` | Daily midnight | Generate and upload sample data |
| `data_flow_validation` | Every 12 hours | Validate end-to-end data integrity |

## Database Schema

### Staging Schema
- `staging.customers_raw` - Raw customer landing table
- `staging.sales_raw` - Raw sales landing table
- `staging.products_raw` - Raw product landing table

### Warehouse Schema (Star Schema)
- `warehouse.dim_customers` - Customer dimension (SCD Type 2)
- `warehouse.dim_products` - Product dimension (SCD Type 2)
- `warehouse.dim_date` - Date dimension (pre-populated 2020-2030)
- `warehouse.fact_sales` - Sales fact table

### Analytics Schema
- `analytics.daily_sales_summary` - Daily aggregations
- `analytics.sales_by_category` - Category breakdown
- `analytics.customer_segments` - Customer segmentation (VIP/Premium/Regular/New)
- `analytics.top_products` - Best-selling products
- `analytics.monthly_trends` - Month-over-month growth

### Audit Schema
- `audit.pipeline_runs` - Pipeline execution tracking
- `audit.data_quality_checks` - Quality check results

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=scripts --cov=plugins --cov-report=term-missing

# Run specific test file
pytest tests/test_data_generation.py -v

# Run only unit tests (exclude integration)
pytest tests/ -v -k "not integration"
```

## Troubleshooting

```bash
# Check service status
docker compose ps

# View logs for specific service
docker compose logs -f airflow-scheduler
docker compose logs -f postgres

# Restart a specific service
docker compose restart airflow-webserver

# Reset everything
docker compose down -v --remove-orphans
docker compose up -d --build

# Check PostgreSQL data
docker compose exec postgres psql -U cdp_admin -d customer_data_platform -c "SELECT COUNT(*) FROM warehouse.fact_sales;"
```

## Stopping the Platform

```bash
# Stop all services (keep data)
docker compose stop

# Stop and remove containers (keep volumes/data)
docker compose down

# Stop and remove everything including data
docker compose down -v --remove-orphans
```

## Team Contributions

| Member | Contribution |
|--------|-------------|
| Emmanuel Kabu | Full platform architecture, Docker setup, ETL pipelines, testing, CI/CD |

## License

This project is for educational/assessment purposes.
