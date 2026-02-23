"""
Tests for database helper module.
Tests: connection, hashing, staging load, incremental loading, audit logging.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from db_helper import (
    compute_row_hash,
    get_conn_params,
)


class TestComputeRowHash:
    """Tests for hash-based change detection."""

    def test_hash_with_specified_columns(self):
        """Should hash only specified columns."""
        row = {"name": "John", "age": 30, "email": "john@test.com"}
        columns = ["name", "email"]
        result = compute_row_hash(row, columns)
        assert isinstance(result, str)
        assert len(result) == 32

    def test_same_data_same_hash(self):
        """Identical data should produce identical hash."""
        row = {"name": "John", "age": 30}
        columns = ["name", "age"]
        hash1 = compute_row_hash(row, columns)
        hash2 = compute_row_hash(row, columns)
        assert hash1 == hash2

    def test_different_data_different_hash(self):
        """Different data should produce different hash."""
        row1 = {"name": "John", "age": 30}
        row2 = {"name": "Jane", "age": 30}
        columns = ["name", "age"]
        hash1 = compute_row_hash(row1, columns)
        hash2 = compute_row_hash(row2, columns)
        assert hash1 != hash2

    def test_column_order_independent(self):
        """Hash should be consistent regardless of column order in list."""
        row = {"b": "2", "a": "1", "c": "3"}
        cols1 = ["a", "b", "c"]
        cols2 = ["c", "a", "b"]
        assert compute_row_hash(row, cols1) == compute_row_hash(row, cols2)

    def test_missing_column_handled(self):
        """Missing columns should be handled gracefully."""
        row = {"name": "John"}
        columns = ["name", "missing_col"]
        result = compute_row_hash(row, columns)
        assert isinstance(result, str)
        assert len(result) == 32

    def test_empty_columns_list(self):
        """Empty columns list should return valid hash."""
        row = {"name": "John"}
        result = compute_row_hash(row, [])
        assert isinstance(result, str)
        assert len(result) == 32

    def test_hash_detects_value_change(self):
        """Hash should change when a tracked value changes."""
        row_v1 = {"name": "John", "city": "NYC"}
        row_v2 = {"name": "John", "city": "LA"}
        columns = ["name", "city"]
        assert compute_row_hash(row_v1, columns) != compute_row_hash(row_v2, columns)

    def test_hash_ignores_non_tracked_columns(self):
        """Hash should not change when non-tracked columns change."""
        row_v1 = {"name": "John", "city": "NYC", "age": 30}
        row_v2 = {"name": "John", "city": "NYC", "age": 31}
        columns = ["name", "city"]
        assert compute_row_hash(row_v1, columns) == compute_row_hash(row_v2, columns)


class TestGetConnParams:
    """Tests for database connection parameters."""

    def test_returns_dict(self, mock_env_vars):
        """Should return a dictionary."""
        params = get_conn_params()
        assert isinstance(params, dict)

    def test_has_required_keys(self, mock_env_vars):
        """Should have all required connection keys."""
        params = get_conn_params()
        required = ["host", "port", "dbname", "user", "password"]
        for key in required:
            assert key in params

    def test_uses_env_vars(self, mock_env_vars):
        """Should use environment variables."""
        params = get_conn_params()
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["dbname"] == "test_db"
        assert params["user"] == "test_user"

    def test_port_is_integer(self, mock_env_vars):
        """Port should be an integer."""
        params = get_conn_params()
        assert isinstance(params["port"], int)


class TestBulkInsertStaging:
    """Tests for staging table bulk insert."""

    @patch("db_helper.get_db_connection")
    def test_empty_data_returns_zero(self, mock_conn):
        """Empty data should return 0 inserted rows."""
        from db_helper import bulk_insert_staging
        conn = MagicMock()
        result = bulk_insert_staging(conn, "staging.test", [], "test.csv")
        assert result == 0

    @patch("db_helper.get_db_connection")
    def test_inserts_data_with_metadata(self, mock_conn):
        """Should add source_file and loaded_at to each row."""
        from db_helper import bulk_insert_staging
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        data = [{"col1": "val1", "col2": "val2"}]
        result = bulk_insert_staging(conn, "staging.test", data, "source.csv")

        assert result == 1
        assert data[0]["source_file"] == "source.csv"
        assert "loaded_at" in data[0]


class TestLogPipelineRun:
    """Tests for pipeline audit logging."""

    @patch("db_helper.get_db_connection")
    def test_logs_successful_run(self, mock_get_conn):
        """Should log pipeline run and return run_id."""
        from db_helper import log_pipeline_run
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchone.return_value = (42,)

        run_id = log_pipeline_run(
            conn, "test_pipeline", "test_dag", "success",
            records_processed=100, records_inserted=95
        )

        assert run_id == 42
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("db_helper.get_db_connection")
    def test_logs_failed_run(self, mock_get_conn):
        """Should log failed pipeline run with error message."""
        from db_helper import log_pipeline_run
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchone.return_value = (99,)

        run_id = log_pipeline_run(
            conn, "test_pipeline", "test_dag", "failed",
            error_message="Something went wrong"
        )

        assert run_id == 99
