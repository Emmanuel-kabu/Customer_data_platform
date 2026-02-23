.PHONY: help build up down logs test lint clean generate pipeline validate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all Docker images
	docker compose build --no-cache

up: ## Start all services
	docker compose up -d --build
	@echo "Services starting... Check with: docker compose ps"

down: ## Stop all services
	docker compose down

down-clean: ## Stop all services and remove volumes
	docker compose down -v --remove-orphans

logs: ## Show service logs
	docker compose logs -f

status: ## Show service status
	docker compose ps

test: ## Run all tests
	pytest tests/ -v --cov=scripts --cov=plugins --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/ -v -k "not integration" --cov=scripts --cov=plugins

test-integration: ## Run integration tests
	pytest tests/test_integration.py -v

lint: ## Run code linting
	flake8 scripts/ plugins/ dags/ tests/ --max-line-length=120 --ignore=E501,W503,E402

generate: ## Generate sample data and upload to MinIO
	docker compose run --rm data-generator

pipeline: ## Trigger the sales data pipeline
	docker compose exec airflow-webserver airflow dags trigger sales_data_pipeline

validate: ## Run data flow validation
	docker compose exec airflow-webserver airflow dags trigger data_flow_validation

db-shell: ## Open PostgreSQL shell
	docker compose exec postgres psql -U cdp_admin -d customer_data_platform

clean: ## Clean up generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage htmlcov/ test-results/ coverage.xml
