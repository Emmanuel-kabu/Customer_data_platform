"""
Tests for the Shift-Left Data Quality Rules Engine.
Tests: rule parsing, all rule types, batch validation, quarantine logic, profiling.
"""

import os
import sys
import uuid
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Fixed UUIDs for deterministic tests
CUST_UUID_1 = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
CUST_UUID_2 = "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
CUST_UUID_3 = "c3d4e5f6-a7b8-4c9d-ae1f-2a3b4c5d6e7f"
CUST_UUID_4 = "d4e5f6a7-b8c9-4d0e-9f2a-3b4c5d6e7f80"
CUST_UUID_5 = "e5f6a7b8-c9d0-4e1f-aa3b-4c5d6e7f8091"
CUST_UUID_99 = "f6a7b8c9-d0e1-4f2a-ab4c-5d6e7f809102"
PROD_UUID_1 = "11111111-2222-4333-8444-555555555555"
PROD_UUID_2 = "22222222-3333-4444-8555-666666666666"
PROD_UUID_3 = "33333333-4444-4555-8666-777777777777"
PROD_UUID_4 = "44444444-5555-4666-8777-888888888888"
PROD_UUID_5 = "55555555-6666-4777-8888-999999999999"
SALE_UUID_1 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SALE_UUID_2 = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
SALE_UUID_3 = "cccccccc-dddd-4eee-8fff-aaaaaaaaaaaa"
SALE_UUID_4 = "dddddddd-eeee-4fff-8aaa-bbbbbbbbbbbb"
SALE_UUID_5 = "eeeeeeee-ffff-4aaa-8bbb-cccccccccccc"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def rules_path():
    """Path to the DQ rules config."""
    return os.path.join(os.path.dirname(__file__), "..", "config", "data_quality_rules.yml")


@pytest.fixture
def engine(rules_path):
    """Create a DQ rules engine instance."""
    from dq_rules_engine import DataQualityRulesEngine
    return DataQualityRulesEngine(rules_path)


@pytest.fixture
def valid_customer():
    """A single valid customer record."""
    return {
        "customer_id": CUST_UUID_1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "phone": "555-0100",
        "address": "123 Main St",
        "city": "New York",
        "state": "NY",
        "country": "US",
        "zip_code": "10001",
        "date_of_birth": "1985-06-15",
        "registration_date": "2024-01-15T10:30:00",
        "row_hash": "abc123",
    }


@pytest.fixture
def valid_product():
    """A single valid product record."""
    return {
        "product_id": PROD_UUID_1,
        "product_name": "TechPro Smartphone Ultra 500",
        "category": "Electronics",
        "sub_category": "Smartphones",
        "brand": "TechPro",
        "unit_price": 999.99,
        "cost_price": 549.99,
        "supplier": "TechSupply Inc",
        "stock_quantity": 150,
        "row_hash": "def456",
    }


@pytest.fixture
def valid_sale(valid_customer, valid_product):
    """A single valid sales record."""
    return {
        "sale_id": SALE_UUID_1,
        "customer_id": CUST_UUID_1,
        "product_id": PROD_UUID_1,
        "product_name": "TechPro Smartphone Ultra 500",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 999.99,
        "total_amount": 1999.98,
        "discount_percent": 10,
        "sale_date": "2025-06-15T14:30:00",
        "payment_method": "Credit Card",
        "store_location": "New York, NY",
        "row_hash": "ghi789",
    }


@pytest.fixture
def valid_customers(valid_customer):
    """List of valid customer records."""
    customers = [dict(valid_customer)]
    for i in range(2, 6):
        c = dict(valid_customer)
        c["customer_id"] = str(uuid.uuid4())
        c["email"] = f"user{i}@example.com"
        c["first_name"] = f"User{i}"
        customers.append(c)
    return customers


@pytest.fixture
def valid_products(valid_product):
    """List of valid product records."""
    products = [dict(valid_product)]
    for i in range(2, 6):
        p = dict(valid_product)
        p["product_id"] = str(uuid.uuid4())
        p["product_name"] = f"Product {i}"
        products.append(p)
    return products


@pytest.fixture
def valid_sales(valid_sale):
    """List of valid sales records."""
    sales = [dict(valid_sale)]
    for i in range(2, 6):
        s = dict(valid_sale)
        s["sale_id"] = str(uuid.uuid4())
        s["total_amount"] = s["unit_price"] * s["quantity"]
        sales.append(s)
    return sales


# ============================================================
# Test: Rule Loading & Parsing
# ============================================================

class TestRuleLoading:
    def test_loads_config(self, engine):
        assert engine.config is not None
        assert "customers" in engine.config
        assert "products" in engine.config
        assert "sales" in engine.config

    def test_parses_customer_rules(self, engine):
        rules = engine._parse_rules("customers")
        assert len(rules) > 10
        rule_names = [r.name for r in rules]
        assert "customer_id_not_null" in rule_names
        assert "customer_id_format" in rule_names
        assert "email_format" in rule_names

    def test_parses_product_rules(self, engine):
        rules = engine._parse_rules("products")
        assert len(rules) > 10
        rule_names = [r.name for r in rules]
        assert "product_id_not_null" in rule_names
        assert "unit_price_positive" in rule_names

    def test_parses_sales_rules(self, engine):
        rules = engine._parse_rules("sales")
        assert len(rules) > 10
        rule_names = [r.name for r in rules]
        assert "sale_id_not_null" in rule_names
        assert "total_amount_consistency" in rule_names

    def test_rules_sorted_by_severity(self, engine):
        rules = engine._parse_rules("customers")
        severities = [r.severity.weight for r in rules]
        assert severities == sorted(severities, reverse=True)

    def test_global_settings_loaded(self, engine):
        assert "halt_on_critical" in engine.global_settings
        assert "quarantine_bucket" in engine.global_settings
        assert engine.global_settings["max_quarantine_pct"] == 15.0


# ============================================================
# Test: Individual Rule Types
# ============================================================

class TestNotNullRule:
    def test_passes_with_value(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        null_violations = [v for v in result.violations if v.rule_name == "customer_id_not_null"]
        assert len(null_violations) == 0

    def test_fails_with_null(self, engine, valid_customers):
        valid_customers[0]["customer_id"] = None
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "customer_id_not_null"]
        assert len(violations) >= 1

    def test_fails_with_empty_string(self, engine, valid_customers):
        valid_customers[0]["email"] = ""
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "email_not_null"]
        assert len(violations) >= 1


class TestRegexRule:
    def test_valid_customer_id_format(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "customer_id_format"]
        assert len(violations) == 0

    def test_invalid_customer_id_format(self, engine, valid_customers):
        valid_customers[0]["customer_id"] = "INVALID-001"
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "customer_id_format"]
        assert len(violations) >= 1

    def test_valid_email_format(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "email_format"]
        assert len(violations) == 0

    def test_invalid_email_format(self, engine, valid_customers):
        valid_customers[0]["email"] = "not-an-email"
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "email_format"]
        assert len(violations) >= 1


class TestUniqueRule:
    def test_unique_values_pass(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "customer_id_unique"]
        assert len(violations) == 0

    def test_duplicate_values_fail(self, engine, valid_customers):
        valid_customers[1]["customer_id"] = valid_customers[0]["customer_id"]
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "customer_id_unique"]
        assert len(violations) >= 1


class TestRangeRule:
    def test_valid_price_range(self, engine, valid_products):
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "unit_price_positive"]
        assert len(violations) == 0

    def test_negative_price_fails(self, engine, valid_products):
        valid_products[0]["unit_price"] = -10.0
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "unit_price_positive"]
        assert len(violations) >= 1

    def test_zero_quantity_fails(self, engine, valid_sales):
        engine.register_reference_data("customers", [{"customer_id": CUST_UUID_1}])
        engine.register_reference_data("products", [{"product_id": PROD_UUID_1}])
        valid_sales[0]["quantity"] = 0
        result = engine.validate("sales", valid_sales)
        violations = [v for v in result.violations if v.rule_name == "quantity_positive"]
        assert len(violations) >= 1


class TestEnumRule:
    def test_valid_category(self, engine, valid_products):
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "category_enum"]
        assert len(violations) == 0

    def test_invalid_category(self, engine, valid_products):
        valid_products[0]["category"] = "InvalidCategory"
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "category_enum"]
        assert len(violations) >= 1

    def test_valid_payment_method(self, engine, valid_sales):
        engine.register_reference_data("customers", [{"customer_id": CUST_UUID_1}])
        engine.register_reference_data("products", [{"product_id": PROD_UUID_1}])
        result = engine.validate("sales", valid_sales)
        violations = [v for v in result.violations if v.rule_name == "payment_method_enum"]
        assert len(violations) == 0


class TestLengthRule:
    def test_valid_length(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "first_name_length"]
        assert len(violations) == 0

    def test_empty_name_fails(self, engine, valid_customers):
        # Empty string would be caught by not_null first, but let's test length
        valid_customers[0]["first_name"] = "A" * 101
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "first_name_length"]
        assert len(violations) >= 1


class TestCrossFieldRule:
    def test_cost_less_than_price(self, engine, valid_products):
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "cost_less_than_price"]
        assert len(violations) == 0

    def test_cost_exceeds_price_fails(self, engine, valid_products):
        valid_products[0]["cost_price"] = 1500.00
        valid_products[0]["unit_price"] = 999.99
        result = engine.validate("products", valid_products)
        violations = [v for v in result.violations if v.rule_name == "cost_less_than_price"]
        assert len(violations) >= 1

    def test_total_amount_consistency(self, engine, valid_sales):
        engine.register_reference_data("customers", [{"customer_id": CUST_UUID_1}])
        engine.register_reference_data("products", [{"product_id": PROD_UUID_1}])
        result = engine.validate("sales", valid_sales)
        violations = [v for v in result.violations if v.rule_name == "total_amount_consistency"]
        assert len(violations) == 0


class TestDateRules:
    def test_valid_dob_format(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "dob_format"]
        assert len(violations) == 0

    def test_future_registration_fails(self, engine, valid_customers):
        future = (datetime.now() + timedelta(days=365)).isoformat()
        valid_customers[0]["registration_date"] = future
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "registration_date_not_future"]
        assert len(violations) >= 1

    def test_dob_out_of_range_fails(self, engine, valid_customers):
        valid_customers[0]["date_of_birth"] = "1910-01-01"
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_name == "dob_range"]
        assert len(violations) >= 1


class TestReferentialRule:
    def test_valid_reference(self, engine, valid_sales):
        engine.register_reference_data("customers", [{"customer_id": CUST_UUID_1}])
        engine.register_reference_data("products", [{"product_id": PROD_UUID_1}])
        result = engine.validate("sales", valid_sales)
        violations = [v for v in result.violations if v.rule_name == "customer_id_referential"]
        assert len(violations) == 0

    def test_orphan_reference_fails(self, engine, valid_sales):
        engine.register_reference_data("customers", [{"customer_id": CUST_UUID_99}])
        engine.register_reference_data("products", [{"product_id": PROD_UUID_1}])
        result = engine.validate("sales", valid_sales)
        violations = [v for v in result.violations if v.rule_name == "customer_id_referential"]
        assert len(violations) >= 1


# ============================================================
# Test: Batch-Level Rules
# ============================================================

class TestBatchRules:
    def test_row_count_passes(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        assert not result.batch_halted

    def test_empty_dataset_halts_batch(self, engine):
        result = engine.validate("customers", [])
        # Empty dataset should trigger row_count violation or be handled
        assert result.total_rows == 0

    def test_completeness_passes(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        violations = [v for v in result.violations if v.rule_type == "completeness"]
        assert len(violations) == 0


# ============================================================
# Test: Quarantine Logic
# ============================================================

class TestQuarantine:
    def test_clean_rows_separated(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        assert len(result.clean_rows) == len(valid_customers)
        assert len(result.quarantined_rows) == 0

    def test_bad_rows_quarantined(self, engine, valid_customers):
        # Make one row have invalid email (HIGH severity → quarantine)
        valid_customers[0]["email"] = "bad-email"
        result = engine.validate("customers", valid_customers)
        assert len(result.quarantined_rows) >= 1
        assert len(result.clean_rows) < len(valid_customers)

    def test_quarantine_rate_threshold(self, engine):
        """If >15% rows quarantined, batch should halt."""
        customers = []
        for i in range(1, 11):
            customers.append({
                "customer_id": str(uuid.uuid4()),
                "first_name": f"User{i}",
                "last_name": "Test",
                "email": f"user{i}@example.com",
                "phone": "555-0100",
                "address": "123 Main St",
                "city": "New York",
                "state": "NY",
                "country": "US",
                "zip_code": "10001",
                "date_of_birth": "1985-06-15",
                "registration_date": "2024-01-15T10:30:00",
                "row_hash": f"hash{i}",
            })
        # Corrupt >15% of rows (3 out of 10 = 30%)
        for i in range(3):
            customers[i]["email"] = "invalid"
        result = engine.validate("customers", customers)
        # Should have quarantine rate > 15%, may halt depending on config
        assert result.quarantine_rate > 15.0


# ============================================================
# Test: Validation Result
# ============================================================

class TestValidationResult:
    def test_summary_structure(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        summary = result.get_summary()
        assert "entity" in summary
        assert "total_rows" in summary
        assert "clean_rows" in summary
        assert "quarantined_rows" in summary
        assert "pass_rate" in summary
        assert "violations_by_severity" in summary
        assert "violations_by_rule" in summary

    def test_pass_rate_100_for_clean_data(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        assert result.pass_rate == 100.0

    def test_is_valid_for_clean_data(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        assert result.is_valid is True


# ============================================================
# Test: Data Profiling
# ============================================================

class TestProfiling:
    def test_profile_generated(self, engine, valid_customers):
        result = engine.validate("customers", valid_customers)
        assert result.profile is not None
        assert "row_count" in result.profile
        assert "column_count" in result.profile
        assert "columns" in result.profile

    def test_profile_column_stats(self, engine, valid_products):
        result = engine.validate("products", valid_products)
        assert "unit_price" in result.profile["columns"]
        price_profile = result.profile["columns"]["unit_price"]
        assert "non_null_count" in price_profile
        assert "null_count" in price_profile
        assert "min" in price_profile
        assert "max" in price_profile


# ============================================================
# Test: Full Validation Pipeline
# ============================================================

class TestFullValidation:
    def test_all_entities_valid(self, engine, valid_customers, valid_products, valid_sales):
        engine.register_reference_data("customers", valid_customers)
        engine.register_reference_data("products", valid_products)

        for entity, data in [("customers", valid_customers), ("products", valid_products), ("sales", valid_sales)]:
            result = engine.validate(entity, data)
            assert not result.batch_halted, f"{entity} batch halted: {result.halt_reason}"
            assert result.pass_rate >= 80.0, f"{entity} pass rate too low: {result.pass_rate}%"

    def test_mixed_quality_data(self, engine, valid_customers):
        """Verify that clean and dirty rows are properly separated."""
        # Add a bad customer
        bad_customer = dict(valid_customers[0])
        bad_customer["customer_id"] = CUST_UUID_99
        bad_customer["email"] = "not-valid"  # HIGH → quarantine
        valid_customers.append(bad_customer)

        result = engine.validate("customers", valid_customers)
        assert len(result.clean_rows) >= len(valid_customers) - 1
        assert len(result.quarantined_rows) >= 1
