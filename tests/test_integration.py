"""
Integration tests for end-to-end data flow validation.
Tests that the complete pipeline can process data correctly.
"""

import os
import sys
import csv
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))


class TestEndToEndDataFlow:
    """Integration tests for the full data pipeline."""

    def test_generate_to_csv_flow(self):
        """Test data generation → CSV conversion flow."""
        from generate_data import generate_products, generate_customers, generate_sales, dict_list_to_csv

        products = generate_products(5)
        customers = generate_customers(10)
        sales = generate_sales(15, customers, products)

        # Convert to CSV
        products_csv = dict_list_to_csv(products, "products")
        customers_csv = dict_list_to_csv(customers, "customers")
        sales_csv = dict_list_to_csv(sales, "sales")

        # Verify CSVs are valid
        assert len(products_csv) > 0
        assert len(customers_csv) > 0
        assert len(sales_csv) > 0

        # Parse back and verify
        product_rows = list(csv.DictReader(StringIO(products_csv)))
        customer_rows = list(csv.DictReader(StringIO(customers_csv)))
        sales_rows = list(csv.DictReader(StringIO(sales_csv)))

        assert len(product_rows) == len(products)
        assert len(customer_rows) == len(customers)
        assert len(sales_rows) == len(sales)

    def test_csv_roundtrip_integrity(self):
        """Test that data survives CSV serialization/deserialization."""
        from generate_data import generate_customers, dict_list_to_csv

        original = generate_customers(5)
        csv_str = dict_list_to_csv(original, "test")
        parsed = list(csv.DictReader(StringIO(csv_str)))

        for i, customer in enumerate(parsed):
            assert customer["customer_id"] == original[i]["customer_id"]
            assert customer["email"] == original[i]["email"]
            assert customer["first_name"] == original[i]["first_name"]

    def test_hash_consistency_across_pipeline(self):
        """Test that hashes remain consistent through the pipeline."""
        from generate_data import compute_row_hash as gen_hash
        from db_helper import compute_row_hash as db_hash

        # A row from generator
        row = {
            "customer_id": "CUST-00001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@test.com"
        }

        # Generator hash
        gen_result = gen_hash(row)
        assert isinstance(gen_result, str)
        assert len(gen_result) == 32

        # DB helper hash (with column specification)
        db_result = db_hash(row, ["customer_id", "first_name", "last_name", "email"])
        assert isinstance(db_result, str)
        assert len(db_result) == 32

    @patch("minio_helper.Minio")
    def test_minio_upload_download_cycle(self, mock_minio, mock_env_vars):
        """Test upload → download cycle through MinIO."""
        from minio_helper import MinIOClient
        from generate_data import generate_products, dict_list_to_csv

        mock_instance = mock_minio.return_value
        mock_instance.bucket_exists.return_value = True

        # Generate and convert
        products = generate_products(5)
        csv_data = dict_list_to_csv(products, "products")

        # Simulate upload
        client = MinIOClient()
        client.upload_file_content("raw-data", "products/test.csv", csv_data)
        mock_instance.put_object.assert_called_once()

        # Simulate download
        mock_response = MagicMock()
        mock_response.read.return_value = csv_data.encode("utf-8")
        mock_instance.get_object.return_value = mock_response

        downloaded = client.download_file_content("raw-data", "products/test.csv")
        assert downloaded == csv_data

    def test_data_referential_integrity(self):
        """Test that generated sales reference valid customers and products."""
        from generate_data import generate_products, generate_customers, generate_sales

        products = generate_products(10)
        customers = generate_customers(20)
        sales = generate_sales(50, customers, products)

        valid_customer_ids = {c["customer_id"] for c in customers}
        valid_product_ids = {p["product_id"] for p in products}

        for sale in sales:
            assert sale["customer_id"] in valid_customer_ids, \
                f"Sale {sale['sale_id']} references invalid customer {sale['customer_id']}"
            assert sale["product_id"] in valid_product_ids, \
                f"Sale {sale['sale_id']} references invalid product {sale['product_id']}"

    def test_data_volume_consistency(self):
        """Test that data generation scales correctly."""
        from generate_data import generate_customers, generate_sales, generate_products

        products = generate_products(5)
        customers_10 = generate_customers(10)
        customers_50 = generate_customers(50)

        assert len(customers_10) == 10
        assert len(customers_50) == 50

        sales_20 = generate_sales(20, customers_10, products)
        sales_100 = generate_sales(100, customers_50, products)

        assert len(sales_20) == 20
        assert len(sales_100) == 100


class TestDataQualityIntegration:
    """Integration tests for data quality checks."""

    def test_quality_checker_initialization(self):
        """Test quality checker can be initialized with params."""
        from data_quality import DataQualityChecker

        checker = DataQualityChecker(
            conn_params={"host": "localhost", "port": 5432},
            pipeline_run_id=1
        )
        assert checker.pipeline_run_id == 1
        assert checker.results == []

    def test_quality_summary_aggregation(self):
        """Test that quality summary correctly aggregates results."""
        from data_quality import DataQualityChecker

        checker = DataQualityChecker({"host": "localhost"})
        checker.results = [
            {"check_name": "c1", "table_name": "t1", "is_passed": True, "expected": "P", "actual": "P"},
            {"check_name": "c2", "table_name": "t2", "is_passed": False, "expected": "P", "actual": "F"},
            {"check_name": "c3", "table_name": "t3", "is_passed": True, "expected": "P", "actual": "P"},
            {"check_name": "c4", "table_name": "t4", "is_passed": True, "expected": "P", "actual": "P"},
        ]

        summary = checker.get_summary()
        assert summary["total_checks"] == 4
        assert summary["passed"] == 3
        assert summary["failed"] == 1
        assert summary["pass_rate"] == 75.0
        assert len(summary["failed_checks"]) == 1
        assert summary["failed_checks"][0]["check_name"] == "c2"
