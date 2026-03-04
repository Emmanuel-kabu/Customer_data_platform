.PHONY: help build up down logs test lint clean generate pipeline validate \
       up-infra up-airflow up-metabase down-clean restart status \
       test-unit test-integration test-dq test-schema format health setup-metabase \
       db-shell trigger-generate trigger-validate trigger-pipeline \
       logs-airflow logs-postgres logs-minio logs-metabase \
       dq-validate dq-report dq-quarantine \
       schema-validate schema-contracts pre-load-report analytics-models

COMPOSE = docker compose -f docker-compose-platform.yml

# ============================================================
# Help
# ============================================================

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================
# Docker Compose - Build & Lifecycle
# ============================================================

build: ## Build all Docker images
	$(COMPOSE) build --no-cache

up: ## Start all services
	$(COMPOSE) up -d --build
	@echo "Services starting... Check with: make status"

up-infra: ## Start infrastructure only (PostgreSQL, MinIO)
	$(COMPOSE) up -d postgres minio
	@echo "Waiting for infrastructure to be healthy..."
	$(COMPOSE) up minio-init

up-airflow: up-infra ## Start Airflow (webserver + scheduler)
	$(COMPOSE) up -d airflow-init
	@echo "Waiting for Airflow init..."
	sleep 30
	$(COMPOSE) up -d airflow-webserver airflow-scheduler

up-metabase: up-infra ## Start Metabase
	$(COMPOSE) up -d metabase

down: ## Stop all services
	$(COMPOSE) down

down-clean: ## Stop all services and remove volumes
	$(COMPOSE) down -v --remove-orphans

restart: ## Restart all services
	$(COMPOSE) down
	$(COMPOSE) up -d --build

status: ## Show service status
	$(COMPOSE) ps

# ============================================================
# Logs
# ============================================================

logs: ## Show all service logs
	$(COMPOSE) logs -f

logs-airflow: ## Show Airflow logs
	$(COMPOSE) logs -f airflow-webserver airflow-scheduler

logs-postgres: ## Show PostgreSQL logs
	$(COMPOSE) logs -f postgres

logs-minio: ## Show MinIO logs
	$(COMPOSE) logs -f minio

logs-metabase: ## Show Metabase logs
	$(COMPOSE) logs -f metabase

# ============================================================
# Data Generation & Pipeline Triggers
# ============================================================

generate: ## Generate sample data and upload to MinIO
	$(COMPOSE) run --rm data-generator

trigger-pipeline: ## Trigger the sales data pipeline DAG
	$(COMPOSE) exec airflow-webserver airflow dags trigger sales_data_pipeline

trigger-validate: ## Trigger the data flow validation DAG
	$(COMPOSE) exec airflow-webserver airflow dags trigger data_flow_validation

trigger-generate: ## Trigger the data generation DAG
	$(COMPOSE) exec airflow-webserver airflow dags trigger data_generation_pipeline

# ============================================================
# Testing
# ============================================================

test: ## Run all tests
	pytest tests/ -v --cov=scripts --cov=plugins --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/ -v -k "not integration" --cov=scripts --cov=plugins

test-integration: ## Run integration tests
	pytest tests/test_integration.py -v

test-dq: ## Run data quality rules engine tests
	pytest tests/test_dq_rules_engine.py -v --tb=short

test-schema: ## Run schema validator tests
	pytest tests/test_schema_validator.py -v --tb=short

# ============================================================
# Schema Validation (Pre-Load)
# ============================================================

schema-validate: ## Dry-run schema validation on latest generated data
	$(COMPOSE) exec airflow-webserver python -c "\
	import sys; sys.path.insert(0, '/opt/airflow/scripts'); \
	from schema_validator import SchemaValidator; \
	v = SchemaValidator('/opt/airflow/config/schema_contracts.yml'); \
	print('Schema contracts loaded for:', list(v.contracts.keys())); \
	print('UUID v4 enforcement: ENABLED'); \
	print('Strict mode:', v.global_settings.get('strict_mode', False))"

schema-contracts: ## Show schema contract summary
	@python -c "\
	import yaml; \
	c = yaml.safe_load(open('config/schema_contracts.yml')); \
	for entity, cfg in {k:v for k,v in c.items() if k != 'global_settings'}.items(): \
	    cols = cfg.get('columns', {}); \
	    print(f'{entity} -> {cfg[\"target_table\"]} ({len(cols)} columns)'); \
	    [print(f'  {name}: {col[\"type\"]}' + (' PK' if col.get('primary_key') else '') + (' NOT NULL' if col.get('not_null') else '')) for name, col in cols.items()]"

pre-load-report: ## Show latest pre-load validation report from MinIO
	$(COMPOSE) exec airflow-webserver python -c "\
	from scripts.minio_helper import MinIOClient; \
	import json; \
	mc = MinIOClient(); \
	reports = [o for o in mc.list_objects('processed-data', 'pre-load-reports/') if o.endswith('.json')]; \
	print(json.dumps(json.loads(mc.download_file_content('processed-data', sorted(reports)[-1])), indent=2)) if reports else print('No pre-load validation reports found')"

# ============================================================
# Data Quality (Shift-Left)
# ============================================================

dq-validate: ## Run DQ validation on generated data (dry-run, no upload)
	python -c "from scripts.dq_rules_engine import DataQualityRulesEngine; e = DataQualityRulesEngine(); print('DQ rules loaded:', len(e._parse_rules('customers')), 'customers,', len(e._parse_rules('products')), 'products,', len(e._parse_rules('sales')), 'sales')"

dq-report: ## Show latest DQ report from MinIO
	$(COMPOSE) exec airflow-webserver python -c "\
	from scripts.minio_helper import MinIOClient; \
	import json; \
	mc = MinIOClient(); \
	reports = [o for o in mc.list_objects('processed-data', 'dq-reports/') if o.endswith('.json')]; \
	print(json.dumps(json.loads(mc.download_file_content('processed-data', sorted(reports)[-1])), indent=2)) if reports else print('No DQ reports found')"

dq-quarantine: ## List quarantined files in MinIO
	$(COMPOSE) exec airflow-webserver python -c "\
	from scripts.minio_helper import MinIOClient; \
	mc = MinIOClient(); \
	files = mc.list_objects('quarantine-data'); \
	print('Quarantined files:'); \
	[print(f'  - {f}') for f in files] if files else print('  None')"

# ============================================================
# Code Quality
# ============================================================

lint: ## Run code linting (flake8, black --check, isort --check)
	flake8 scripts/ plugins/ dags/ tests/ --max-line-length=120 --ignore=E501,W503,E402
	black --check scripts/ plugins/ dags/ tests/
	isort --check-only scripts/ plugins/ dags/ tests/

format: ## Auto-format code with black and isort
	black scripts/ plugins/ dags/ tests/
	isort scripts/ plugins/ dags/ tests/

# ============================================================
# Utilities
# ============================================================

db-shell: ## Open PostgreSQL shell
	$(COMPOSE) exec postgres psql -U cdp_admin -d customer_data_platform

health: ## Run health checks on all services
	python scripts/health_check.py

setup-metabase: ## Run Metabase auto-setup (connect to warehouse, create dashboards)
	python scripts/setup_metabase.py

analytics-models: ## Apply analytics SQL models (views for dashboarding)
	$(COMPOSE) exec airflow-webserver python /opt/airflow/scripts/apply_analytics_models.py

clean: ## Clean up generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage htmlcov/ test-results/ coverage.xml
