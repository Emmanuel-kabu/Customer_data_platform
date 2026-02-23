"""
Tests for data generation module.
Tests: data generation, CSV conversion, hashing, MinIO upload.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO
import csv

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from generate_data import (
    compute_row_hash,
    generate_products,
    generate_customers,
    generate_sales,
    dict_list_to_csv,
)


class TestComputeRowHash:
    """Tests for the row hash computation function."""

    def test_hash_returns_string(self):
        """Hash function should return a string."""
        row = {"col1": "value1", "col2": "value2"}
        result = compute_row_hash(row)
        assert isinstance(result, str)

    def test_hash_is_consistent(self):
        """Same input should always produce the same hash."""
        row = {"name": "John", "email": "john@test.com"}
        hash1 = compute_row_hash(row)
        hash2 = compute_row_hash(row)
        assert hash1 == hash2

    def test_hash_is_md5_length(self):
        """MD5 hash should be 32 characters."""
        row = {"key": "value"}
        result = compute_row_hash(row)
        assert len(result) == 32

    def test_different_data_produces_different_hash(self):
        """Different data should produce different hashes."""
        row1 = {"name": "John"}
        row2 = {"name": "Jane"}
        assert compute_row_hash(row1) != compute_row_hash(row2)

    def test_empty_dict_hash(self):
        """Empty dict should still produce a valid hash."""
        result = compute_row_hash({})
        assert isinstance(result, str)
        assert len(result) == 32


class TestGenerateProducts:
    """Tests for product data generation."""

    def test_generates_correct_count(self):
        """Should generate approximately the requested number of products."""
        products = generate_products(10)
        assert len(products) > 0
        assert len(products) <= 15  # May be slightly more due to rounding

    def test_product_has_required_fields(self):
        """Each product should have all required fields."""
        products = generate_products(5)
        required_fields = [
            "product_id", "product_name", "category", "sub_category",
            "brand", "unit_price", "cost_price", "supplier",
            "stock_quantity", "row_hash"
        ]
        for product in products:
            for field in required_fields:
                assert field in product, f"Missing field: {field}"

    def test_product_id_format(self):
        """Product IDs should follow PROD-XXXX format."""
        products = generate_products(5)
        for product in products:
            assert product["product_id"].startswith("PROD-")

    def test_price_is_positive(self):
        """Prices should be positive values."""
        products = generate_products(10)
        for product in products:
            assert float(product["unit_price"]) > 0
            assert float(product["cost_price"]) > 0

    def test_cost_less_than_price(self):
        """Cost price should be less than unit price."""
        products = generate_products(10)
        for product in products:
            assert float(product["cost_price"]) < float(product["unit_price"])

    def test_valid_categories(self):
        """Categories should be from the predefined list."""
        valid_categories = ["Electronics", "Clothing", "Home & Garden", "Books & Media", "Health & Beauty"]
        products = generate_products(25)
        for product in products:
            assert product["category"] in valid_categories


class TestGenerateCustomers:
    """Tests for customer data generation."""

    def test_generates_correct_count(self):
        """Should generate exact number of customers."""
        customers = generate_customers(10)
        assert len(customers) == 10

    def test_customer_has_required_fields(self):
        """Each customer should have all required fields."""
        customers = generate_customers(3)
        required_fields = [
            "customer_id", "first_name", "last_name", "email",
            "phone", "city", "state", "country", "row_hash"
        ]
        for customer in customers:
            for field in required_fields:
                assert field in customer, f"Missing field: {field}"

    def test_customer_id_format(self):
        """Customer IDs should follow CUST-XXXXX format."""
        customers = generate_customers(5)
        for customer in customers:
            assert customer["customer_id"].startswith("CUST-")

    def test_unique_emails(self):
        """All customers should have unique emails."""
        customers = generate_customers(20)
        emails = [c["email"] for c in customers]
        assert len(emails) == len(set(emails))

    def test_country_is_us(self):
        """All generated customers should be from US."""
        customers = generate_customers(5)
        for customer in customers:
            assert customer["country"] == "US"


class TestGenerateSales:
    """Tests for sales data generation."""

    def test_generates_correct_count(self):
        """Should generate exact number of sales records."""
        products = generate_products(5)
        customers = generate_customers(5)
        sales = generate_sales(10, customers, products)
        assert len(sales) == 10

    def test_sale_has_required_fields(self):
        """Each sale should have all required fields."""
        products = generate_products(5)
        customers = generate_customers(5)
        sales = generate_sales(3, customers, products)
        required_fields = [
            "sale_id", "customer_id", "product_id", "quantity",
            "unit_price", "total_amount", "payment_method", "row_hash"
        ]
        for sale in sales:
            for field in required_fields:
                assert field in sale, f"Missing field: {field}"

    def test_sale_id_format(self):
        """Sale IDs should follow SALE-XXXXXX format."""
        products = generate_products(5)
        customers = generate_customers(5)
        sales = generate_sales(5, customers, products)
        for sale in sales:
            assert sale["sale_id"].startswith("SALE-")

    def test_valid_customer_references(self):
        """Sales should reference valid customer IDs."""
        products = generate_products(5)
        customers = generate_customers(5)
        customer_ids = {c["customer_id"] for c in customers}
        sales = generate_sales(10, customers, products)
        for sale in sales:
            assert sale["customer_id"] in customer_ids

    def test_valid_product_references(self):
        """Sales should reference valid product IDs."""
        products = generate_products(5)
        customers = generate_customers(5)
        product_ids = {p["product_id"] for p in products}
        sales = generate_sales(10, customers, products)
        for sale in sales:
            assert sale["product_id"] in product_ids

    def test_quantity_positive(self):
        """Quantities should be positive."""
        products = generate_products(5)
        customers = generate_customers(5)
        sales = generate_sales(10, customers, products)
        for sale in sales:
            assert int(sale["quantity"]) > 0

    def test_valid_payment_methods(self):
        """Payment methods should be from predefined list."""
        valid_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash", "Crypto"]
        products = generate_products(5)
        customers = generate_customers(5)
        sales = generate_sales(20, customers, products)
        for sale in sales:
            assert sale["payment_method"] in valid_methods


class TestDictListToCsv:
    """Tests for CSV conversion function."""

    def test_converts_to_csv_string(self):
        """Should return a valid CSV string."""
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        result = dict_list_to_csv(data, "test")
        assert isinstance(result, str)
        assert "name" in result
        assert "John" in result

    def test_csv_has_header(self):
        """CSV output should include header row."""
        data = [{"col1": "val1", "col2": "val2"}]
        result = dict_list_to_csv(data, "test")
        lines = result.strip().split("\n")
        assert len(lines) == 2  # header + 1 data row
        assert "col1" in lines[0]

    def test_empty_list_returns_empty(self):
        """Empty list should return empty string."""
        result = dict_list_to_csv([], "test")
        assert result == ""

    def test_csv_is_parseable(self):
        """Generated CSV should be parseable back."""
        data = [{"x": "1", "y": "2"}, {"x": "3", "y": "4"}]
        csv_str = dict_list_to_csv(data, "test")
        reader = csv.DictReader(StringIO(csv_str))
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["x"] == "1"
