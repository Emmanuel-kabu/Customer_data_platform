"""
Tests for data quality checks module.
Tests: quality checks, row counts, null validation, referential integrity.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))


class TestDataQualityChecker:
    """Tests for the DataQualityChecker class."""

    @patch("data_quality.psycopg2.connect")
    def test_check_row_count_passes(self, mock_connect):
        """Should pass when table has enough rows."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("PASS",)

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.check_row_count("warehouse.dim_customers", min_rows=1)

        assert result is True

    @patch("data_quality.psycopg2.connect")
    def test_check_row_count_fails(self, mock_connect):
        """Should fail when table doesn't have enough rows."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("FAIL",)

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.check_row_count("warehouse.dim_customers", min_rows=1000000)

        assert result is False

    @patch("data_quality.psycopg2.connect")
    def test_check_null_values_passes(self, mock_connect):
        """Should pass when no NULL values found."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("PASS",)

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.check_null_values("warehouse.dim_customers", "customer_id")

        assert result is True

    @patch("data_quality.psycopg2.connect")
    def test_check_unique_values_passes(self, mock_connect):
        """Should pass when all values are unique."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("PASS",)

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.check_unique_values("warehouse.dim_customers", "customer_key")

        assert result is True

    @patch("data_quality.psycopg2.connect")
    def test_check_value_range_passes(self, mock_connect):
        """Should pass when values are within range."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("PASS",)

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.check_value_range("warehouse.dim_products", "unit_price", 0, 100000)

        assert result is True

    def test_get_summary_empty(self):
        """Should return empty summary when no checks run."""
        from data_quality import DataQualityChecker
        checker = DataQualityChecker({"host": "localhost"})
        summary = checker.get_summary()

        assert summary["total_checks"] == 0
        assert summary["passed"] == 0
        assert summary["failed"] == 0
        assert summary["pass_rate"] == 0

    @patch("data_quality.psycopg2.connect")
    def test_get_summary_with_results(self, mock_connect):
        """Should correctly summarize check results."""
        from data_quality import DataQualityChecker

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        checker = DataQualityChecker({"host": "localhost"})

        # Manually add results
        checker.results = [
            {"check_name": "test1", "table_name": "t1", "is_passed": True, "expected": "PASS", "actual": "PASS"},
            {"check_name": "test2", "table_name": "t2", "is_passed": True, "expected": "PASS", "actual": "PASS"},
            {"check_name": "test3", "table_name": "t3", "is_passed": False, "expected": "PASS", "actual": "FAIL"},
        ]

        summary = checker.get_summary()
        assert summary["total_checks"] == 3
        assert summary["passed"] == 2
        assert summary["failed"] == 1
        assert summary["pass_rate"] == 66.67
        assert len(summary["failed_checks"]) == 1

    @patch("data_quality.psycopg2.connect")
    def test_check_handles_db_error(self, mock_connect):
        """Should handle database errors gracefully."""
        from data_quality import DataQualityChecker

        mock_connect.side_effect = Exception("Connection refused")

        checker = DataQualityChecker({"host": "localhost"})
        result = checker.run_check("test", "table", "type", "SELECT 1", "1")

        assert result is False
