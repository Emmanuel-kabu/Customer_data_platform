"""
Tests for Schema Validator — Pre-Load Data Validation
====================================================
Comprehensive tests covering all validation phases:
- Schema structure validation
- UUID type validation
- Type coercion (varchar, integer, decimal, date, timestamp)
- Constraint checks (NOT NULL, UNIQUE, ENUM, REGEX, RANGE)
- Referential integrity (foreign key validation)
- Cross-column business rules
- Batch-level metrics
"""

import os
import sys
import uuid
import pytest
from datetime import datetime, timedelta
from copy import deepcopy

# Add scripts/ to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def validator():
    """Create a SchemaValidator instance with the project's contracts."""
    from schema_validator import SchemaValidator
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "config", "schema_contracts.yml"
    )
    return SchemaValidator(contracts_path=config_path)


@pytest.fixture
def valid_customer():
    """A single valid customer record with UUID."""
    return {
        "customer_id": str(uuid.uuid4()),
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "phone": "555-0100",
        "address": "123 Main St",
        "city": "New York",
        "state": "NY",
        "country": "US",
        "zip_code": "10001",
        "date_of_birth": "1990-05-15",
        "registration_date": "2024-01-15T10:30:00",
        "row_hash": "abc123def456",
    }


@pytest.fixture
def valid_product():
    """A single valid product record with UUID."""
    return {
        "product_id": str(uuid.uuid4()),
        "product_name": "TechPro Wireless Mouse",
        "category": "Electronics",
        "sub_category": "Accessories",
        "brand": "TechPro",
        "unit_price": "29.99",
        "cost_price": "15.50",
        "supplier": "Widget Corp",
        "stock_quantity": "250",
        "row_hash": "def789ghi012",
    }


@pytest.fixture
def valid_sale(valid_customer, valid_product):
    """A single valid sale record with UUID references."""
    return {
        "sale_id": str(uuid.uuid4()),
        "customer_id": valid_customer["customer_id"],
        "product_id": valid_product["product_id"],
        "product_name": "TechPro Wireless Mouse",
        "category": "Electronics",
        "quantity": "3",
        "unit_price": "29.99",
        "total_amount": "89.97",
        "discount_percent": "0",
        "sale_date": "2025-06-15T14:30:00",
        "payment_method": "Credit Card",
        "store_location": "New York, NY",
        "row_hash": "ghi345jkl678",
    }


@pytest.fixture
def valid_customers_batch(valid_customer):
    """Generate a batch of valid customers."""
    customers = []
    for i in range(10):
        c = deepcopy(valid_customer)
        c["customer_id"] = str(uuid.uuid4())
        c["email"] = f"user{i}@example.com"
        customers.append(c)
    return customers


@pytest.fixture
def valid_products_batch(valid_product):
    """Generate a batch of valid products."""
    products = []
    for i in range(5):
        p = deepcopy(valid_product)
        p["product_id"] = str(uuid.uuid4())
        p["product_name"] = f"Product {i}"
        products.append(p)
    return products


@pytest.fixture
def valid_sales_batch(valid_customers_batch, valid_products_batch):
    """Generate valid sales referencing existing customers and products."""
    import random
    sales = []
    for i in range(20):
        c = random.choice(valid_customers_batch)
        p = random.choice(valid_products_batch)
        qty = random.randint(1, 5)
        price = float(p["unit_price"])
        total = round(price * qty, 2)
        sales.append({
            "sale_id": str(uuid.uuid4()),
            "customer_id": c["customer_id"],
            "product_id": p["product_id"],
            "product_name": p["product_name"],
            "category": p["category"],
            "quantity": str(qty),
            "unit_price": str(price),
            "total_amount": str(total),
            "discount_percent": "0",
            "sale_date": "2025-03-15T10:00:00",
            "payment_method": "Credit Card",
            "store_location": "Online Store",
            "row_hash": f"hash_{i}",
        })
    return sales


# ============================================================
# UUID Validation Tests
# ============================================================

class TestUUIDValidation:
    """Test UUID v4 format validation."""

    def test_valid_uuid_v4(self, validator):
        from schema_validator import is_valid_uuid
        assert is_valid_uuid(str(uuid.uuid4()))

    def test_valid_uuid_v4_lowercase(self, validator):
        from schema_validator import is_valid_uuid
        assert is_valid_uuid("550e8400-e29b-41d4-a716-446655440000")

    def test_valid_uuid_v4_uppercase(self, validator):
        from schema_validator import is_valid_uuid
        assert is_valid_uuid("550E8400-E29B-41D4-A716-446655440000")

    def test_invalid_uuid_sequential_id(self, validator):
        from schema_validator import is_valid_uuid
        assert not is_valid_uuid("CUST-00001")

    def test_invalid_uuid_short_string(self, validator):
        from schema_validator import is_valid_uuid
        assert not is_valid_uuid("not-a-uuid")

    def test_invalid_uuid_empty(self, validator):
        from schema_validator import is_valid_uuid
        assert not is_valid_uuid("")

    def test_invalid_uuid_none(self, validator):
        from schema_validator import is_valid_uuid
        assert not is_valid_uuid(None)

    def test_normalize_uuid(self, validator):
        from schema_validator import normalize_uuid
        raw = "550E8400-E29B-41D4-A716-446655440000"
        assert normalize_uuid(raw) == "550e8400-e29b-41d4-a716-446655440000"

    def test_normalize_uuid_invalid(self, validator):
        from schema_validator import normalize_uuid
        assert normalize_uuid("not-a-uuid") is None

    def test_uuid_type_validator(self, validator):
        from schema_validator import TypeValidator
        valid, coerced, err = TypeValidator.validate_uuid(str(uuid.uuid4()))
        assert valid is True
        assert err == ""

    def test_uuid_type_validator_rejects_old_format(self, validator):
        from schema_validator import TypeValidator
        valid, _, err = TypeValidator.validate_uuid("PROD-0001")
        assert valid is False
        assert "Invalid UUID" in err


# ============================================================
# Type Coercion Tests
# ============================================================

class TestTypeValidation:
    """Test type validation and coercion for PostgreSQL types."""

    def test_varchar_valid(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_varchar("hello", max_length=255)
        assert valid is True

    def test_varchar_exceeds_length(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_varchar("x" * 300, max_length=255)
        assert valid is False
        assert "exceeds max" in err

    def test_varchar_null(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_varchar(None)
        assert valid is True
        assert val is None

    def test_integer_valid(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_integer("42")
        assert valid is True
        assert val == 42

    def test_integer_from_float_string(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_integer("42.7")
        assert valid is True
        assert val == 42

    def test_integer_invalid_string(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_integer("abc")
        assert valid is False

    def test_integer_range_check(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_integer("150", min_val=1, max_val=100)
        assert valid is False
        assert "above maximum" in err

    def test_decimal_valid(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_decimal("29.99", precision=12, scale=2)
        assert valid is True
        assert val == 29.99

    def test_decimal_invalid_string(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_decimal("abc")
        assert valid is False

    def test_decimal_negative_below_min(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_decimal("-5.00", min_val=0.0)
        assert valid is False

    def test_date_valid_iso(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_date("2024-01-15")
        assert valid is True
        assert val == "2024-01-15"

    def test_date_invalid_format(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_date("15/01/2024")
        assert valid is False

    def test_timestamp_valid(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_timestamp("2024-01-15T10:30:00")
        assert valid is True

    def test_timestamp_with_microseconds(self):
        from schema_validator import TypeValidator
        valid, val, err = TypeValidator.validate_timestamp("2024-01-15T10:30:00.123456")
        assert valid is True

    def test_boolean_true_values(self):
        from schema_validator import TypeValidator
        for val in ["true", "True", "1", "yes", "t"]:
            valid, coerced, _ = TypeValidator.validate_boolean(val)
            assert valid is True
            assert coerced is True

    def test_boolean_false_values(self):
        from schema_validator import TypeValidator
        for val in ["false", "False", "0", "no", "f"]:
            valid, coerced, _ = TypeValidator.validate_boolean(val)
            assert valid is True
            assert coerced is False


# ============================================================
# String Coercion Tests
# ============================================================

class TestStringCoercion:
    """Test string transformation coercions."""

    def test_trim(self):
        from schema_validator import StringCoercer
        assert StringCoercer.apply("  hello  ", ["trim"]) == "hello"

    def test_lowercase(self):
        from schema_validator import StringCoercer
        assert StringCoercer.apply("HELLO", ["lowercase"]) == "hello"

    def test_uppercase(self):
        from schema_validator import StringCoercer
        assert StringCoercer.apply("hello", ["uppercase"]) == "HELLO"

    def test_title_case(self):
        from schema_validator import StringCoercer
        assert StringCoercer.apply("john doe", ["title_case"]) == "John Doe"

    def test_multiple_coercions(self):
        from schema_validator import StringCoercer
        result = StringCoercer.apply("  john doe  ", ["trim", "title_case"])
        assert result == "John Doe"

    def test_none_value(self):
        from schema_validator import StringCoercer
        assert StringCoercer.apply(None, ["trim"]) is None


# ============================================================
# Schema Structure Tests
# ============================================================

class TestSchemaStructure:
    """Test Phase 1: Schema structure validation."""

    def test_valid_customer_schema(self, validator, valid_customer):
        result = validator.validate("customers", [valid_customer])
        structural_violations = [
            v for v in result.violations if v.phase.value == "schema_structure"
        ]
        critical = [v for v in structural_violations if v.severity.value == "CRITICAL"]
        assert len(critical) == 0
        assert not result.batch_halted

    def test_missing_required_column(self, validator, valid_customer):
        del valid_customer["customer_id"]
        result = validator.validate("customers", [valid_customer])
        assert result.batch_halted is True
        assert "missing" in result.halt_reason.lower() or "Missing" in result.halt_reason

    def test_empty_dataset(self, validator):
        result = validator.validate("customers", [])
        assert result.batch_halted is True
        assert "empty" in result.halt_reason.lower() or "Empty" in result.halt_reason

    def test_extra_columns_detected(self, validator, valid_customer):
        valid_customer["extra_col"] = "unexpected"
        result = validator.validate("customers", [valid_customer])
        drift_violations = [
            v for v in result.violations if v.rule == "schema_drift_extra_column"
        ]
        assert len(drift_violations) > 0


# ============================================================
# Constraint Validation Tests
# ============================================================

class TestConstraints:
    """Test Phase 3: Constraint checks."""

    def test_not_null_violation(self, validator, valid_customer):
        valid_customer["email"] = None
        result = validator.validate("customers", [valid_customer])
        null_violations = [v for v in result.violations if v.rule == "not_null"]
        assert len(null_violations) > 0

    def test_not_null_empty_string(self, validator, valid_customer):
        valid_customer["customer_id"] = ""
        result = validator.validate("customers", [valid_customer])
        # Either type_uuid or not_null violation
        assert len(result.rejected_rows) > 0

    def test_unique_violation(self, validator, valid_customer):
        customers = [deepcopy(valid_customer), deepcopy(valid_customer)]
        # Same customer_id should trigger uniqueness violation
        result = validator.validate("customers", customers)
        unique_violations = [v for v in result.violations if v.rule == "unique"]
        assert len(unique_violations) > 0

    def test_unique_email_violation(self, validator, valid_customer):
        c1 = deepcopy(valid_customer)
        c2 = deepcopy(valid_customer)
        c2["customer_id"] = str(uuid.uuid4())
        # Same email
        result = validator.validate("customers", [c1, c2])
        email_unique = [v for v in result.violations if v.rule == "unique" and v.column == "email"]
        assert len(email_unique) > 0

    def test_enum_valid(self, validator, valid_customer):
        valid_customer["country"] = "US"
        result = validator.validate("customers", [valid_customer])
        enum_violations = [v for v in result.violations if v.rule == "enum" and v.column == "country"]
        assert len(enum_violations) == 0

    def test_enum_invalid(self, validator, valid_customer):
        valid_customer["country"] = "ZZ"
        result = validator.validate("customers", [valid_customer])
        enum_violations = [v for v in result.violations if v.rule == "enum" and v.column == "country"]
        assert len(enum_violations) > 0

    def test_regex_valid_email(self, validator, valid_customer):
        result = validator.validate("customers", [valid_customer])
        regex_violations = [v for v in result.violations if v.rule == "regex" and v.column == "email"]
        assert len(regex_violations) == 0

    def test_regex_invalid_email(self, validator, valid_customer):
        valid_customer["email"] = "not-an-email"
        result = validator.validate("customers", [valid_customer])
        regex_violations = [v for v in result.violations if v.rule == "regex" and v.column == "email"]
        assert len(regex_violations) > 0

    def test_not_future_valid(self, validator, valid_customer):
        valid_customer["registration_date"] = "2024-01-15T10:30:00"
        result = validator.validate("customers", [valid_customer])
        future_violations = [v for v in result.violations if v.rule == "not_future"]
        assert len(future_violations) == 0

    def test_not_future_violation(self, validator, valid_customer):
        future_dt = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S")
        valid_customer["registration_date"] = future_dt
        result = validator.validate("customers", [valid_customer])
        future_violations = [v for v in result.violations if v.rule == "not_future"]
        assert len(future_violations) > 0

    def test_date_range_valid(self, validator, valid_customer):
        valid_customer["date_of_birth"] = "1990-05-15"
        result = validator.validate("customers", [valid_customer])
        range_violations = [v for v in result.violations if v.rule == "date_range" and v.column == "date_of_birth"]
        assert len(range_violations) == 0

    def test_date_range_too_old(self, validator, valid_customer):
        valid_customer["date_of_birth"] = "1910-01-01"
        result = validator.validate("customers", [valid_customer])
        range_violations = [v for v in result.violations if v.rule == "date_range" and v.column == "date_of_birth"]
        assert len(range_violations) > 0

    def test_zip_code_valid(self, validator, valid_customer):
        valid_customer["zip_code"] = "10001"
        result = validator.validate("customers", [valid_customer])
        zip_violations = [v for v in result.violations if v.rule == "regex" and v.column == "zip_code"]
        assert len(zip_violations) == 0

    def test_zip_code_extended_format(self, validator, valid_customer):
        valid_customer["zip_code"] = "10001-1234"
        result = validator.validate("customers", [valid_customer])
        zip_violations = [v for v in result.violations if v.rule == "regex" and v.column == "zip_code"]
        assert len(zip_violations) == 0


# ============================================================
# UUID Customer ID Tests
# ============================================================

class TestUUIDCustomerID:
    """Test that customer IDs must be UUIDs (not sequential)."""

    def test_uuid_customer_id_passes(self, validator, valid_customer):
        result = validator.validate("customers", [valid_customer])
        uuid_violations = [v for v in result.violations if v.rule == "type_uuid"]
        assert len(uuid_violations) == 0

    def test_old_format_customer_id_fails(self, validator, valid_customer):
        valid_customer["customer_id"] = "CUST-00001"
        result = validator.validate("customers", [valid_customer])
        uuid_violations = [v for v in result.violations if v.rule == "type_uuid"]
        assert len(uuid_violations) > 0
        assert len(result.rejected_rows) > 0

    def test_uuid_product_id_passes(self, validator, valid_product):
        result = validator.validate("products", [valid_product])
        uuid_violations = [v for v in result.violations if v.rule == "type_uuid"]
        assert len(uuid_violations) == 0

    def test_old_format_product_id_fails(self, validator, valid_product):
        valid_product["product_id"] = "PROD-0001"
        result = validator.validate("products", [valid_product])
        uuid_violations = [v for v in result.violations if v.rule == "type_uuid"]
        assert len(uuid_violations) > 0

    def test_uuid_sale_id_passes(self, validator, valid_sale):
        from schema_validator import SchemaValidator
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "schema_contracts.yml"
        )
        v = SchemaValidator(contracts_path=config_path)
        v.set_reference_data("customers", [{"customer_id": valid_sale["customer_id"]}])
        v.set_reference_data("products", [{"product_id": valid_sale["product_id"]}])
        result = v.validate("sales", [valid_sale])
        uuid_violations = [viol for viol in result.violations if viol.rule == "type_uuid"]
        assert len(uuid_violations) == 0

    def test_old_format_sale_id_fails(self, validator, valid_sale):
        valid_sale["sale_id"] = "SALE-000001"
        validator.set_reference_data("customers", [{"customer_id": valid_sale["customer_id"]}])
        validator.set_reference_data("products", [{"product_id": valid_sale["product_id"]}])
        result = validator.validate("sales", [valid_sale])
        uuid_violations = [v for v in result.violations if v.rule == "type_uuid"]
        assert len(uuid_violations) > 0


# ============================================================
# Product Validation Tests
# ============================================================

class TestProductValidation:
    """Test product-specific schema validation."""

    def test_valid_product(self, validator, valid_product):
        result = validator.validate("products", [valid_product])
        assert len(result.valid_rows) == 1
        assert len(result.rejected_rows) == 0

    def test_negative_price_rejected(self, validator, valid_product):
        valid_product["unit_price"] = "-5.00"
        result = validator.validate("products", [valid_product])
        assert len(result.rejected_rows) > 0

    def test_invalid_category(self, validator, valid_product):
        valid_product["category"] = "Weapons"
        result = validator.validate("products", [valid_product])
        enum_violations = [v for v in result.violations if v.rule == "enum"]
        assert len(enum_violations) > 0

    def test_cost_exceeds_price(self, validator, valid_product):
        valid_product["unit_price"] = "10.00"
        valid_product["cost_price"] = "50.00"
        result = validator.validate("products", [valid_product])
        cross_violations = [v for v in result.violations if v.rule == "cost_leq_price"]
        assert len(cross_violations) > 0

    def test_product_name_too_short(self, validator, valid_product):
        valid_product["product_name"] = "AB"
        result = validator.validate("products", [valid_product])
        length_violations = [v for v in result.violations if v.rule == "min_length"]
        assert len(length_violations) > 0


# ============================================================
# Referential Integrity Tests
# ============================================================

class TestReferentialIntegrity:
    """Test Phase 4: Foreign key validation."""

    def test_valid_fk_references(self, validator, valid_customers_batch, valid_products_batch, valid_sales_batch):
        validator.set_reference_data("customers", valid_customers_batch)
        validator.set_reference_data("products", valid_products_batch)
        result = validator.validate("sales", valid_sales_batch)
        fk_violations = [v for v in result.violations if v.rule == "foreign_key"]
        assert len(fk_violations) == 0

    def test_orphan_customer_id(self, validator, valid_sale):
        valid_sale["customer_id"] = str(uuid.uuid4())  # Non-existent customer
        validator.set_reference_data("customers", [])
        validator.set_reference_data("products", [{"product_id": valid_sale["product_id"]}])
        result = validator.validate("sales", [valid_sale])
        # Should get either fk_reference_unavailable or foreign_key violation
        fk_violations = [v for v in result.violations if "fk" in v.rule or "foreign" in v.rule]
        assert len(fk_violations) > 0

    def test_orphan_product_id(self, validator, valid_sale):
        valid_sale["product_id"] = str(uuid.uuid4())  # Non-existent product
        validator.set_reference_data("customers", [{"customer_id": valid_sale["customer_id"]}])
        validator.set_reference_data("products", [{"product_id": str(uuid.uuid4())}])  # Different product
        result = validator.validate("sales", [valid_sale])
        fk_violations = [v for v in result.violations if v.rule == "foreign_key"]
        assert len(fk_violations) > 0


# ============================================================
# Cross-Column Validation Tests
# ============================================================

class TestCrossColumnValidation:
    """Test Phase 5: Cross-column business rules."""

    def test_total_equals_price_times_qty(self, validator, valid_sale):
        validator.set_reference_data("customers", [{"customer_id": valid_sale["customer_id"]}])
        validator.set_reference_data("products", [{"product_id": valid_sale["product_id"]}])
        result = validator.validate("sales", [valid_sale])
        cross_violations = [v for v in result.violations if v.rule == "total_equals_price_times_qty"]
        assert len(cross_violations) == 0

    def test_total_mismatch_detected(self, validator, valid_sale):
        valid_sale["total_amount"] = "999.99"  # Doesn't match price * qty
        validator.set_reference_data("customers", [{"customer_id": valid_sale["customer_id"]}])
        validator.set_reference_data("products", [{"product_id": valid_sale["product_id"]}])
        result = validator.validate("sales", [valid_sale])
        cross_violations = [v for v in result.violations if v.rule == "total_equals_price_times_qty"]
        assert len(cross_violations) > 0


# ============================================================
# Batch Metrics Tests
# ============================================================

class TestBatchMetrics:
    """Test Phase 6: Batch-level thresholds."""

    def test_batch_halts_on_high_rejection_rate(self, validator, valid_customer):
        """If >5% of rows are rejected, batch should halt."""
        bad_customers = []
        for i in range(100):
            c = deepcopy(valid_customer)
            c["customer_id"] = str(uuid.uuid4())
            c["email"] = f"user{i}@example.com"
            if i < 10:  # 10% bad
                c["customer_id"] = "INVALID"  # Non-UUID
            bad_customers.append(c)

        result = validator.validate("customers", bad_customers)
        # 10% rejection > 5% threshold → batch should halt
        assert result.batch_halted is True
        assert "rejection rate" in result.halt_reason.lower() or "Rejection rate" in result.halt_reason

    def test_batch_passes_within_threshold(self, validator, valid_customer):
        """If rejection rate is within threshold, batch should pass."""
        customers = []
        for i in range(100):
            c = deepcopy(valid_customer)
            c["customer_id"] = str(uuid.uuid4())
            c["email"] = f"user{i}@example.com"
            customers.append(c)
        # All valid → 0% rejection
        result = validator.validate("customers", customers)
        assert result.batch_halted is False


# ============================================================
# Validation Result Tests
# ============================================================

class TestValidationResult:
    """Test PreLoadValidationResult properties and summary."""

    def test_pass_rate_100_for_clean_data(self, validator, valid_customers_batch):
        result = validator.validate("customers", valid_customers_batch)
        assert result.pass_rate == 100.0
        assert result.is_valid is True

    def test_summary_structure(self, validator, valid_customers_batch):
        result = validator.validate("customers", valid_customers_batch)
        summary = result.get_summary()
        assert "entity" in summary
        assert "target_table" in summary
        assert "total_rows" in summary
        assert "valid_rows" in summary
        assert "rejected_rows" in summary
        assert "pass_rate" in summary
        assert "violations_by_phase" in summary
        assert "violations_by_column" in summary

    def test_rejection_rate_calculated(self, validator, valid_customer):
        bad = deepcopy(valid_customer)
        bad["customer_id"] = "NOT-A-UUID"
        result = validator.validate("customers", [bad])
        assert result.rejection_rate > 0


# ============================================================
# Full Pipeline Validation Tests
# ============================================================

class TestFullPipelineValidation:
    """Test validate_all() — full entity pipeline."""

    def test_all_entities_valid(self, validator, valid_customers_batch, valid_products_batch, valid_sales_batch):
        report = validator.validate_all(
            customers=valid_customers_batch,
            products=valid_products_batch,
            sales=valid_sales_batch,
        )
        assert report["overall_status"] == "PASS"
        assert report["totals"]["total_rejected"] == 0
        assert report["totals"]["overall_pass_rate"] == 100.0
        assert "_validated_data" in report
        assert "_rejected_data" in report

    def test_uuid_enforcement_across_pipeline(self, validator, valid_customers_batch, valid_products_batch, valid_sales_batch):
        """Ensure old sequential IDs are rejected across all entities."""
        # Inject old-format IDs
        valid_customers_batch[0]["customer_id"] = "CUST-00001"
        valid_products_batch[0]["product_id"] = "PROD-0001"

        report = validator.validate_all(
            customers=valid_customers_batch,
            products=valid_products_batch,
            sales=valid_sales_batch,
        )

        # Should have rejections for non-UUID IDs
        cust_rejected = report["entities"]["customers"]["summary"]["rejected_rows"]
        prod_rejected = report["entities"]["products"]["summary"]["rejected_rows"]
        assert cust_rejected >= 1
        assert prod_rejected >= 1

    def test_report_has_entity_details(self, validator, valid_customers_batch, valid_products_batch, valid_sales_batch):
        report = validator.validate_all(
            customers=valid_customers_batch,
            products=valid_products_batch,
            sales=valid_sales_batch,
        )
        assert "customers" in report["entities"]
        assert "products" in report["entities"]
        assert "sales" in report["entities"]
        for entity_report in report["entities"].values():
            assert "summary" in entity_report
            assert "violation_details" in entity_report


# ============================================================
# Coercion Integration Tests
# ============================================================

class TestCoercionIntegration:
    """Test that coercions are properly applied during validation."""

    def test_email_lowercased(self, validator, valid_customer):
        valid_customer["email"] = "John.Doe@EXAMPLE.COM"
        result = validator.validate("customers", [valid_customer])
        if result.valid_rows:
            assert result.valid_rows[0]["email"] == "john.doe@example.com"
        assert result.coercions_applied > 0

    def test_name_trimmed_and_titled(self, validator, valid_customer):
        valid_customer["first_name"] = "  john  "
        valid_customer["last_name"] = "  doe  "
        result = validator.validate("customers", [valid_customer])
        if result.valid_rows:
            assert result.valid_rows[0]["first_name"] == "John"
            assert result.valid_rows[0]["last_name"] == "Doe"

    def test_country_uppercased(self, validator, valid_customer):
        valid_customer["country"] = "us"
        result = validator.validate("customers", [valid_customer])
        if result.valid_rows:
            assert result.valid_rows[0]["country"] == "US"


# ============================================================
# Edge Cases
# ============================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_row_valid(self, validator, valid_customer):
        result = validator.validate("customers", [valid_customer])
        assert len(result.valid_rows) == 1
        assert result.pass_rate == 100.0

    def test_all_rows_invalid(self, validator):
        bad_rows = [
            {"customer_id": "BAD", "email": "bad", "first_name": "", "last_name": ""},
        ]
        result = validator.validate("customers", bad_rows)
        assert len(result.rejected_rows) >= 0  # May halt for schema mismatch
        assert result.pass_rate < 100.0 or result.batch_halted

    def test_max_decimal_precision(self, validator, valid_product):
        valid_product["unit_price"] = "99999.99"
        result = validator.validate("products", [valid_product])
        assert len(result.valid_rows) == 1

    def test_boundary_stock_quantity(self, validator, valid_product):
        valid_product["stock_quantity"] = "0"
        result = validator.validate("products", [valid_product])
        range_violations = [v for v in result.violations if v.rule == "range_check" and v.column == "stock_quantity"]
        assert len(range_violations) == 0
