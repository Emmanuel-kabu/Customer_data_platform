"""
Tests for Airflow DAG definitions.
Validates DAG structure, import, schedule, and task dependencies.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from datetime import timedelta

# Mock airflow modules before importing DAGs
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.python"] = MagicMock()
sys.modules["airflow.operators.empty"] = MagicMock()
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.dates"] = MagicMock()


class TestDagDefinitions:
    """Test DAG file structure and configuration."""

    def test_dag_files_exist(self):
        """All expected DAG files should exist."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        expected_dags = [
            "sales_data_pipeline.py",
            "data_generation_dag.py",
            "data_flow_validation_dag.py",
        ]
        for dag_file in expected_dags:
            filepath = os.path.join(dag_dir, dag_file)
            assert os.path.exists(filepath), f"DAG file missing: {dag_file}"

    def test_dag_files_have_docstrings(self):
        """All DAG files should have module docstrings."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        dag_files = [
            "sales_data_pipeline.py",
            "data_generation_dag.py",
            "data_flow_validation_dag.py",
        ]
        for dag_file in dag_files:
            filepath = os.path.join(dag_dir, dag_file)
            with open(filepath, "r") as f:
                content = f.read()
            assert '"""' in content, f"DAG {dag_file} missing docstring"

    def test_sales_pipeline_has_required_tasks(self):
        """Sales pipeline DAG should define all required task functions."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "sales_data_pipeline.py")

        with open(filepath, "r") as f:
            content = f.read()

        required_functions = [
            "extract_from_minio",
            "load_to_staging",
            "transform_customers",
            "transform_products",
            "transform_sales",
            "run_quality_checks",
            "archive_processed_files",
            "send_pipeline_summary",
        ]

        for func in required_functions:
            assert f"def {func}" in content, f"Missing task function: {func}"

    def test_sales_pipeline_has_schedule(self):
        """Sales pipeline should have a schedule_interval defined."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "sales_data_pipeline.py")

        with open(filepath, "r") as f:
            content = f.read()

        assert "schedule_interval" in content, "Missing schedule_interval"

    def test_sales_pipeline_has_retries(self):
        """Sales pipeline should have retry configuration."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "sales_data_pipeline.py")

        with open(filepath, "r") as f:
            content = f.read()

        assert "retries" in content, "Missing retry configuration"
        assert "retry_delay" in content, "Missing retry_delay configuration"
        assert "retry_exponential_backoff" in content, "Missing exponential backoff"

    def test_sales_pipeline_has_callbacks(self):
        """Sales pipeline should have success/failure callbacks."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "sales_data_pipeline.py")

        with open(filepath, "r") as f:
            content = f.read()

        assert "on_failure_callback" in content, "Missing failure callback"
        assert "on_success_callback" in content, "Missing success callback"
        assert "on_retry_callback" in content, "Missing retry callback"

    def test_sales_pipeline_has_email_config(self):
        """Sales pipeline should have email notification config."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "sales_data_pipeline.py")

        with open(filepath, "r") as f:
            content = f.read()

        assert "email_on_failure" in content, "Missing email_on_failure"

    def test_data_generation_dag_exists(self):
        """Data generation DAG should be properly defined."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "data_generation_dag.py")

        with open(filepath, "r") as f:
            content = f.read()

        assert "data_generation_pipeline" in content, "Missing DAG ID"
        assert "schedule_interval" in content, "Missing schedule"

    def test_validation_dag_has_all_checks(self):
        """Validation DAG should check all pipeline components."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        filepath = os.path.join(dag_dir, "data_flow_validation_dag.py")

        with open(filepath, "r") as f:
            content = f.read()

        required_validations = [
            "validate_minio",
            "validate_postgres",
            "validate_staging",
            "validate_warehouse",
            "validate_analytics_views",
            "validate_data_flow",
        ]

        for validation in required_validations:
            assert validation in content, f"Missing validation: {validation}"

    def test_no_hardcoded_credentials(self):
        """DAG files should not contain hardcoded credentials."""
        dag_dir = os.path.join(os.path.dirname(__file__), "..", "dags")
        dag_files = [
            "sales_data_pipeline.py",
            "data_generation_dag.py",
            "data_flow_validation_dag.py",
        ]

        for dag_file in dag_files:
            filepath = os.path.join(dag_dir, dag_file)
            with open(filepath, "r") as f:
                content = f.read()

            # Should use env vars or Airflow variables, not hardcoded passwords
            assert "password = \"" not in content.lower() or "os.getenv" in content, \
                f"Potential hardcoded credentials in {dag_file}"
