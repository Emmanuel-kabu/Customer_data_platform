"""
conftest.py - Shared fixtures for all test modules.
"""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set environment variables for testing."""
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "test_user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
    monkeypatch.setenv("NUM_CUSTOMERS", "10")
    monkeypatch.setenv("NUM_SALES_RECORDS", "20")
    monkeypatch.setenv("NUM_PRODUCTS", "5")


@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing."""
    return [
        {
            "customer_id": "CUST-00001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "phone": "555-0101",
            "address": "123 Main St",
            "city": "New York",
            "state": "NY",
            "country": "US",
            "zip_code": "10001",
            "date_of_birth": "1990-01-15",
            "registration_date": "2024-01-01T00:00:00",
            "row_hash": "abc123"
        },
        {
            "customer_id": "CUST-00002",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@example.com",
            "phone": "555-0102",
            "address": "456 Oak Ave",
            "city": "Los Angeles",
            "state": "CA",
            "country": "US",
            "zip_code": "90001",
            "date_of_birth": "1985-06-20",
            "registration_date": "2024-02-15T00:00:00",
            "row_hash": "def456"
        }
    ]


@pytest.fixture
def sample_product_data():
    """Sample product data for testing."""
    return [
        {
            "product_id": "PROD-0001",
            "product_name": "TechPro Smartphone X100",
            "category": "Electronics",
            "sub_category": "Smartphones",
            "brand": "TechPro",
            "unit_price": 799.99,
            "cost_price": 439.99,
            "supplier": "Tech Corp",
            "stock_quantity": 100,
            "row_hash": "prod_hash_1"
        },
        {
            "product_id": "PROD-0002",
            "product_name": "UrbanStyle Jacket Premium",
            "category": "Clothing",
            "sub_category": "Men's Wear",
            "brand": "UrbanStyle",
            "unit_price": 149.99,
            "cost_price": 59.99,
            "supplier": "Fashion Inc",
            "stock_quantity": 250,
            "row_hash": "prod_hash_2"
        }
    ]


@pytest.fixture
def sample_sales_data():
    """Sample sales data for testing."""
    return [
        {
            "sale_id": "SALE-000001",
            "customer_id": "CUST-00001",
            "product_id": "PROD-0001",
            "product_name": "TechPro Smartphone X100",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 799.99,
            "total_amount": 1599.98,
            "discount_percent": 10,
            "sale_date": "2025-06-15T14:30:00",
            "payment_method": "Credit Card",
            "store_location": "New York, NY",
            "row_hash": "sale_hash_1"
        },
        {
            "sale_id": "SALE-000002",
            "customer_id": "CUST-00002",
            "product_id": "PROD-0002",
            "product_name": "UrbanStyle Jacket Premium",
            "category": "Clothing",
            "quantity": 1,
            "unit_price": 149.99,
            "total_amount": 149.99,
            "discount_percent": 0,
            "sale_date": "2025-07-20T10:00:00",
            "payment_method": "PayPal",
            "store_location": "Online Store",
            "row_hash": "sale_hash_2"
        }
    ]


@pytest.fixture
def mock_minio_client():
    """Mocked MinIO client."""
    client = MagicMock()
    client.bucket_exists.return_value = True
    client.list_objects.return_value = []
    return client


@pytest.fixture
def mock_db_connection():
    """Mocked database connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchone.return_value = (0,)
    cursor.fetchall.return_value = []
    return conn, cursor
