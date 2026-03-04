# Customer Data Platform — Architecture

## High-Level Architecture Diagram

```mermaid
graph TB
    subgraph CICD["CI/CD Pipeline - GitHub Actions"]
        direction LR
        CI["CI: Lint → Unit Tests → Build Images → Validate Compose → Integration Tests"]
        CD["CD: Deploy Containers → Validate Data Flow → Cleanup"]
        CI --> CD
    end

    subgraph DataGen["1. Data Generation Layer"]
        DG["Data Generator<br/>(Python + Faker)"]
        DQE["Shift-Left DQ Engine<br/>(dq_rules_engine.py)"]
        SC["Schema Contracts<br/>(schema_contracts.yml)"]
        DQR["DQ Rules<br/>(data_quality_rules.yml)"]
        DG --> DQE
        SC --> DQE
        DQR --> DQE
    end

    subgraph Storage["2. Object Storage Layer"]
        direction TB
        MINIO["MinIO - S3-Compatible<br/>Port: 9000 / 9001"]
        RAW["raw-data bucket<br/>(customers/, products/, sales/)"]
        PROC["processed-data bucket<br/>(schema-validation reports)"]
        ARCH["archive-data bucket<br/>(processed files)"]
        QUAR["quarantine-data bucket<br/>(rejected rows)"]
        MINIO --- RAW
        MINIO --- PROC
        MINIO --- ARCH
        MINIO --- QUAR
    end

    subgraph Orchestration["3. Orchestration Layer - Apache Airflow"]
        direction TB
        AW["Airflow Webserver<br/>Port: 8080"]
        AS["Airflow Scheduler"]
        DAG1["data_generation_dag<br/>(Generate + Upload)"]
        DAG2["sales_data_pipeline<br/>(Extract → Validate → Stage → Transform → DQ → Archive)"]
        DAG3["data_flow_validation_dag<br/>(End-to-End Health Checks)"]
        AW --- AS
        AS --> DAG1
        AS --> DAG2
        AS --> DAG3
    end

    subgraph Database["4. Database Layer - PostgreSQL 15"]
        direction TB
        PG["PostgreSQL<br/>Port: 5432"]
        STG["staging schema<br/>(customers_raw, sales_raw, products_raw)"]
        WH["warehouse schema<br/>(dim_customers, dim_products,<br/>dim_date, fact_sales)<br/>SCD Type 2"]
        AN["analytics schema<br/>(executive_kpis, revenue_trends,<br/>customer_segments, product_performance)"]
        AUD["audit schema<br/>(pipeline_runs, dq_validation_runs,<br/>pre_load_rejections)"]
        PG --- STG
        PG --- WH
        PG --- AN
        PG --- AUD
        STG -->|"Transform +<br/>Incremental Load"| WH
        WH -->|"Analytics<br/>Views"| AN
    end

    subgraph BI["5. BI & Visualization Layer"]
        MB["Metabase<br/>Port: 3000"]
        DASH["Dashboards & Cards<br/>(Auto-provisioned via code)"]
        MB --- DASH
    end

    subgraph Monitoring["6. Monitoring & Alerts"]
        SLACK["Slack Notifications<br/>(Pipeline alerts)"]
        DQREP["DQ Reports<br/>(JSON in MinIO + audit tables)"]
    end

    %% Data Flow Connections
    DQE -->|"Clean CSVs"| RAW
    DQE -->|"Rejected rows"| QUAR
    DAG1 -->|"Triggers"| DAG2
    DAG2 -->|"Extract CSVs"| RAW
    DAG2 -->|"Pre-load Validate"| SC
    DAG2 -->|"Load to Staging"| STG
    DAG2 -->|"Archive processed"| ARCH
    DAG2 -->|"Save reports"| PROC
    DAG2 -->|"DQ Results"| AUD
    DAG2 -->|"Alerts"| SLACK
    AN -->|"Query"| MB
    CICD -->|"Auto Deploy"| Orchestration
```

## Data Flow Summary

```
Data Generator (Faker)
    │
    ▼
Shift-Left DQ Engine ──→ quarantine-data bucket (rejected rows)
    │
    ▼ (clean CSVs)
MinIO raw-data bucket
    │
    ▼ (Airflow sales_data_pipeline)
┌─────────────────────────────────────────────────┐
│  1. Extract CSVs from MinIO                     │
│  2. Pre-load schema & data validation           │
│  3. Load clean rows → staging schema            │
│  4. Transform → warehouse (SCD Type 2)          │
│  5. Apply analytics views                       │
│  6. Run post-load DQ checks                     │
│  7. Archive processed files → archive-data      │
│  8. Send Slack notifications                    │
└─────────────────────────────────────────────────┘
    │
    ▼
PostgreSQL (staging → warehouse → analytics)
    │
    ▼
Metabase Dashboards (auto-provisioned)
```

## Technology Stack

| Layer | Component | Technology | Port |
|-------|-----------|------------|------|
| Data Generation | Sample data + DQ validation | Python 3.11, Faker | — |
| Object Storage | Raw/processed/archive files | MinIO (S3-compatible) | 9000, 9001 |
| Orchestration | DAG scheduling & ETL | Apache Airflow 2.8 | 8080 |
| Database | Staging, warehouse, analytics, audit | PostgreSQL 15 | 5432 |
| BI & Dashboards | Visualization & exploration | Metabase | 3000 |
| Containerization | Service lifecycle | Docker Compose | — |
| CI/CD | Automated build, test, deploy | GitHub Actions | — |
| Alerts | Pipeline notifications | Slack Webhooks | — |

## Database Schemas

| Schema | Purpose | Key Tables/Views |
|--------|---------|------------------|
| `staging` | Raw data landing zone | `customers_raw`, `sales_raw`, `products_raw` |
| `warehouse` | Curated dimensions & facts (SCD Type 2) | `dim_customers`, `dim_products`, `dim_date`, `fact_sales` |
| `analytics` | Business-ready views | `executive_kpis`, `revenue_profit_trends`, `customer_segments`, `product_performance` |
| `audit` | Pipeline & DQ tracking | `pipeline_runs`, `dq_validation_runs`, `pre_load_rejections` |

## CI/CD Pipeline

### CI (Continuous Integration)
1. **Lint** — flake8, black, isort
2. **Unit Tests** — pytest with coverage
3. **Build Docker Images** — airflow, data-generator
4. **Validate Compose** — config syntax + service definitions
5. **Integration Tests** — end-to-end mocked tests

### CD (Continuous Deployment)
1. **Deploy Services** — build images, start all containers, health checks
2. **Validate Data Flow** — run data generator, trigger pipeline, verify storage, check Metabase
3. **Cleanup** — tear down containers and volumes

## Docker Services

| Service | Container Name | Image | Purpose |
|---------|---------------|-------|---------|
| postgres | cdp-postgres | postgres:15-alpine | Data warehouse |
| minio | cdp-minio | minio/minio:latest | Object storage |
| minio-init | cdp-minio-init | minio/mc:latest | Bucket initialization |
| airflow-init | cdp-airflow-init | Custom (Dockerfile) | DB init + user creation |
| airflow-webserver | cdp-airflow-webserver | Custom (Dockerfile) | Airflow UI |
| airflow-scheduler | cdp-airflow-scheduler | Custom (Dockerfile) | DAG scheduling |
| metabase | cdp-metabase | metabase/metabase:latest | BI dashboards |
| data-generator | cdp-data-generator | Custom (Dockerfile) | Sample data generation |
