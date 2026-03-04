"""
Schema Validator — Pre-Load Data Validation for PostgreSQL
==========================================================
Enterprise-grade validation layer that sits between data extraction
(MinIO CSV) and PostgreSQL staging load. Ensures EVERY row conforms
to the declared schema contract BEFORE touching the database.

Architecture:
    MinIO CSV → Extract → SCHEMA VALIDATOR → Load to Staging
                              │
                              ├── PASS: Data proceeds to staging
                              ├── REJECT: Row logged to audit.pre_load_rejections
                              └── HALT: Pipeline stops on critical schema mismatch

Validation phases (executed in order):
    Phase 1 — Schema Structure: Column presence, drift detection
    Phase 2 — Type Coercion: UUID, decimal, integer, date, timestamp
    Phase 3 — Constraint Checks: NOT NULL, UNIQUE, ENUM, REGEX, RANGE
    Phase 4 — Referential Integrity: Foreign key validation across entities
    Phase 5 — Cross-Column Logic: Multi-column business rules
    Phase 6 — Batch-Level Metrics: Rejection rates, completeness thresholds

Author: CDP Engineering
"""

import os
import re
import logging
import uuid as uuid_module
from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

import yaml

logger = logging.getLogger(__name__)


# ============================================================
# Enums & Data Classes
# ============================================================

class ValidationSeverity(Enum):
    """Severity of validation failures."""
    CRITICAL = "CRITICAL"   # Halt batch — schema mismatch, all IDs corrupted
    HIGH = "HIGH"           # Reject row — type cast failure, PK violation
    MEDIUM = "MEDIUM"       # Warn — constraint violation, data quality issue
    LOW = "LOW"             # Log only — optional field missing


class ValidationPhase(Enum):
    """Validation phase for tracking."""
    SCHEMA_STRUCTURE = "schema_structure"
    TYPE_COERCION = "type_coercion"
    CONSTRAINT_CHECK = "constraint_check"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    CROSS_COLUMN = "cross_column"
    BATCH_METRICS = "batch_metrics"


@dataclass
class ColumnContract:
    """Schema contract for a single column."""
    name: str
    col_type: str
    target_column: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    format: Optional[str] = None
    nullable: bool = False
    constraints: List[Any] = field(default_factory=list)
    coerce: List[str] = field(default_factory=list)
    description: str = ""


@dataclass
class SchemaViolation:
    """A single schema validation failure."""
    entity: str
    phase: ValidationPhase
    severity: ValidationSeverity
    column: Optional[str]
    row_index: Optional[int]
    rule: str
    message: str
    actual_value: Any = None
    expected: str = ""
    row_data: Optional[Dict] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class PreLoadValidationResult:
    """Complete result of pre-load schema validation for one entity."""
    entity: str
    target_table: str
    total_rows: int
    valid_rows: List[Dict] = field(default_factory=list)
    rejected_rows: List[Dict] = field(default_factory=list)
    violations: List[SchemaViolation] = field(default_factory=list)
    coercions_applied: int = 0
    batch_halted: bool = False
    halt_reason: str = ""
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return not self.batch_halted and len(self.rejected_rows) == 0

    @property
    def pass_rate(self) -> float:
        if self.total_rows == 0:
            return 100.0
        return round(len(self.valid_rows) / self.total_rows * 100, 2)

    @property
    def rejection_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round(len(self.rejected_rows) / self.total_rows * 100, 2)

    def get_summary(self) -> Dict:
        violations_by_phase = {}
        for v in self.violations:
            key = v.phase.value
            violations_by_phase[key] = violations_by_phase.get(key, 0) + 1

        violations_by_column = {}
        for v in self.violations:
            if v.column:
                violations_by_column[v.column] = violations_by_column.get(v.column, 0) + 1

        return {
            "entity": self.entity,
            "target_table": self.target_table,
            "total_rows": self.total_rows,
            "valid_rows": len(self.valid_rows),
            "rejected_rows": len(self.rejected_rows),
            "pass_rate": self.pass_rate,
            "rejection_rate": self.rejection_rate,
            "total_violations": len(self.violations),
            "coercions_applied": self.coercions_applied,
            "batch_halted": self.batch_halted,
            "halt_reason": self.halt_reason,
            "violations_by_phase": violations_by_phase,
            "violations_by_column": violations_by_column,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


# ============================================================
# UUID Validator
# ============================================================

# RFC 4122 UUID v4 pattern
UUID_V4_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)

# General UUID pattern (any version)
UUID_GENERAL_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE
)


def is_valid_uuid(value: str, version: int = 4) -> bool:
    """Validate UUID format. Supports v4 strict or general UUID."""
    if not isinstance(value, str):
        return False
    if version == 4:
        return bool(UUID_V4_PATTERN.match(value))
    return bool(UUID_GENERAL_PATTERN.match(value))


def normalize_uuid(value: str) -> Optional[str]:
    """Normalize UUID to lowercase with dashes."""
    try:
        return str(uuid_module.UUID(value))
    except (ValueError, AttributeError):
        return None


# ============================================================
# Type Validators & Coercers
# ============================================================

class TypeValidator:
    """Validates and coerces values to PostgreSQL-compatible types."""

    @staticmethod
    def validate_uuid(value: Any, strict_v4: bool = True) -> Tuple[bool, Any, str]:
        """
        Validate UUID format and normalize.
        Returns: (is_valid, coerced_value, error_message)
        """
        if value is None or str(value).strip() == "":
            return False, None, "UUID value is empty or null"

        val_str = str(value).strip().lower()
        normalized = normalize_uuid(val_str)

        if normalized is None:
            return False, value, f"Invalid UUID format: '{value}'"

        if strict_v4 and not is_valid_uuid(normalized, version=4):
            return False, value, f"Not a valid UUID v4: '{value}'"

        return True, normalized, ""

    @staticmethod
    def validate_varchar(value: Any, max_length: int = None) -> Tuple[bool, Any, str]:
        """Validate and coerce string value with length check."""
        if value is None:
            return True, None, ""

        val_str = str(value)

        if max_length and len(val_str) > max_length:
            return False, val_str, f"String length {len(val_str)} exceeds max {max_length}"

        return True, val_str, ""

    @staticmethod
    def validate_integer(value: Any, min_val: int = None, max_val: int = None) -> Tuple[bool, Any, str]:
        """Validate and coerce integer value."""
        if value is None or str(value).strip() == "":
            return True, None, ""

        try:
            int_val = int(float(str(value).strip()))
        except (ValueError, TypeError):
            return False, value, f"Cannot cast '{value}' to integer"

        if min_val is not None and int_val < min_val:
            return False, int_val, f"Integer {int_val} below minimum {min_val}"
        if max_val is not None and int_val > max_val:
            return False, int_val, f"Integer {int_val} above maximum {max_val}"

        return True, int_val, ""

    @staticmethod
    def validate_decimal(
        value: Any,
        precision: int = 12,
        scale: int = 2,
        min_val: float = None,
        max_val: float = None
    ) -> Tuple[bool, Any, str]:
        """Validate and coerce decimal value with precision/scale check."""
        if value is None or str(value).strip() == "":
            return True, None, ""

        try:
            dec_val = Decimal(str(value).strip())
        except (InvalidOperation, ValueError, TypeError):
            return False, value, f"Cannot cast '{value}' to decimal"

        # Check if value fits within precision/scale
        quantize_str = "0." + "0" * scale if scale > 0 else "0"
        try:
            dec_val = dec_val.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return False, value, f"Decimal overflow: '{value}' exceeds precision({precision},{scale})"

        # Max digits check
        sign, digits, exponent = dec_val.as_tuple()
        total_digits = len(digits)
        if total_digits > precision:
            return False, float(dec_val), f"Decimal '{dec_val}' exceeds precision {precision}"

        float_val = float(dec_val)
        if min_val is not None and float_val < min_val:
            return False, float_val, f"Decimal {float_val} below minimum {min_val}"
        if max_val is not None and float_val > max_val:
            return False, float_val, f"Decimal {float_val} above maximum {max_val}"

        return True, float_val, ""

    @staticmethod
    def validate_date(value: Any, fmt: str = "%Y-%m-%d") -> Tuple[bool, Any, str]:
        """Validate date format and parse."""
        if value is None or str(value).strip() == "":
            return True, None, ""

        val_str = str(value).strip()

        # Try the specified format first
        try:
            parsed = datetime.strptime(val_str, fmt).date()
            return True, parsed.isoformat(), ""
        except ValueError:
            pass

        # Try ISO format fallback
        try:
            parsed = datetime.fromisoformat(val_str).date()
            return True, parsed.isoformat(), ""
        except ValueError:
            pass

        return False, value, f"Cannot parse '{value}' as date (expected {fmt})"

    @staticmethod
    def validate_timestamp(value: Any, fmt: str = "%Y-%m-%dT%H:%M:%S") -> Tuple[bool, Any, str]:
        """Validate timestamp format and parse."""
        if value is None or str(value).strip() == "":
            return True, None, ""

        val_str = str(value).strip()

        try:
            parsed = datetime.strptime(val_str, fmt)
            return True, parsed.isoformat(), ""
        except ValueError:
            pass

        # Try ISO format fallback
        try:
            parsed = datetime.fromisoformat(val_str)
            return True, parsed.isoformat(), ""
        except ValueError:
            pass

        # Try with microseconds
        try:
            parsed = datetime.strptime(val_str, fmt + ".%f")
            return True, parsed.isoformat(), ""
        except ValueError:
            pass

        return False, value, f"Cannot parse '{value}' as timestamp (expected {fmt})"

    @staticmethod
    def validate_boolean(value: Any) -> Tuple[bool, Any, str]:
        """Validate and coerce boolean value."""
        if value is None or str(value).strip() == "":
            return True, None, ""

        val_str = str(value).strip().lower()
        if val_str in ("true", "1", "yes", "t", "y"):
            return True, True, ""
        if val_str in ("false", "0", "no", "f", "n"):
            return True, False, ""

        return False, value, f"Cannot cast '{value}' to boolean"


# ============================================================
# String Coercers
# ============================================================

class StringCoercer:
    """Apply declared string transformations."""

    @staticmethod
    def apply(value: Any, coercions: List[str]) -> Any:
        if value is None:
            return None

        val_str = str(value)
        for coercion in coercions:
            if coercion == "trim":
                val_str = val_str.strip()
            elif coercion == "lowercase":
                val_str = val_str.lower()
            elif coercion == "uppercase":
                val_str = val_str.upper()
            elif coercion == "title_case":
                val_str = val_str.title()

        return val_str


# ============================================================
# Schema Validator Engine
# ============================================================

class SchemaValidator:
    """
    Pre-load schema validation engine.

    Validates CSV data against declared schema contracts before
    loading into PostgreSQL staging tables. Acts as a firewall
    between raw data and the database.
    """

    def __init__(self, contracts_path: Optional[str] = None):
        self.contracts_path = contracts_path or self._find_contracts_file()
        self.config = self._load_config()
        self.global_settings = self.config.get("global_settings", {})
        self._reference_data: Dict[str, List[Dict]] = {}
        self._type_validator = TypeValidator()

    def _find_contracts_file(self) -> str:
        """Auto-detect schema contracts file location."""
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "config", "schema_contracts.yml"),
            os.path.join(os.path.dirname(__file__), "config", "schema_contracts.yml"),
            "/opt/airflow/config/schema_contracts.yml",
            "/app/config/schema_contracts.yml",
        ]
        for path in candidates:
            abspath = os.path.abspath(path)
            if os.path.isfile(abspath):
                return abspath
        raise FileNotFoundError(
            f"Schema contracts file not found. Searched: {candidates}"
        )

    def _load_config(self) -> Dict:
        """Load and parse the YAML contracts file."""
        with open(self.contracts_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded schema contracts from {self.contracts_path}")
        return config

    def _parse_column_contract(self, col_name: str, col_config: Dict) -> ColumnContract:
        """Parse a column definition from YAML into a ColumnContract."""
        return ColumnContract(
            name=col_name,
            col_type=col_config.get("type", "varchar"),
            target_column=col_config.get("target_column"),
            max_length=col_config.get("max_length"),
            precision=col_config.get("precision"),
            scale=col_config.get("scale"),
            format=col_config.get("format"),
            nullable=col_config.get("nullable", False),
            constraints=col_config.get("constraints", []),
            coerce=col_config.get("coerce", []),
            description=col_config.get("description", ""),
        )

    def get_entity_contract(self, entity: str) -> Dict:
        """Get the contract definition for an entity."""
        contract = self.config.get(entity)
        if not contract:
            raise ValueError(f"No schema contract found for entity: {entity}")
        return contract

    def set_reference_data(self, entity: str, data: List[Dict]):
        """Register reference data for FK validation."""
        self._reference_data[entity] = data

    # ================================================================
    # Main Validation Entry Point
    # ================================================================

    def validate(
        self,
        entity: str,
        data: List[Dict],
        reference_data: Optional[Dict[str, List[Dict]]] = None,
    ) -> PreLoadValidationResult:
        """
        Validate an entire dataset against its schema contract.

        Args:
            entity: Entity name (customers, products, sales)
            data: List of row dicts from CSV
            reference_data: Dict of {entity_name: rows} for FK checks

        Returns:
            PreLoadValidationResult with valid/rejected rows and violations
        """
        contract = self.get_entity_contract(entity)
        target_table = contract.get("target_table", f"staging.{entity}_raw")
        columns_config = contract.get("columns", {})
        cross_validations = contract.get("cross_validations", [])

        # Register reference data
        if reference_data:
            for ref_entity, ref_rows in reference_data.items():
                self._reference_data[ref_entity] = ref_rows

        # Parse column contracts
        column_contracts: Dict[str, ColumnContract] = {}
        for col_name, col_config in columns_config.items():
            column_contracts[col_name] = self._parse_column_contract(col_name, col_config)

        result = PreLoadValidationResult(
            entity=entity,
            target_table=target_table,
            total_rows=len(data),
        )

        if not data:
            result.batch_halted = True
            result.halt_reason = "Empty dataset — nothing to load"
            result.violations.append(SchemaViolation(
                entity=entity,
                phase=ValidationPhase.BATCH_METRICS,
                severity=ValidationSeverity.CRITICAL,
                column=None,
                row_index=None,
                rule="empty_dataset",
                message="Dataset is empty — cannot proceed with load",
            ))
            result.completed_at = datetime.now().isoformat()
            return result

        # ── Phase 1: Schema Structure ──
        self._validate_schema_structure(entity, data[0], column_contracts, result)
        if result.batch_halted:
            result.completed_at = datetime.now().isoformat()
            return result

        # ── Phase 2-5: Row-level validation ──
        unique_trackers: Dict[str, Set] = {}
        for col_name, col_contract in column_contracts.items():
            if "unique" in col_contract.constraints or "primary_key" in col_contract.constraints:
                unique_trackers[col_name] = set()

        for row_idx, row in enumerate(data):
            row_violations = []
            coerced_row = dict(row)

            # Phase 2: Type coercion & validation
            for col_name, col_contract in column_contracts.items():
                raw_value = row.get(col_name)

                # Apply string coercions first
                if col_contract.coerce and raw_value is not None:
                    coerced_val = StringCoercer.apply(raw_value, col_contract.coerce)
                    if coerced_val != raw_value:
                        result.coercions_applied += 1
                    coerced_row[col_name] = coerced_val
                    raw_value = coerced_val

                # Type validation
                type_valid, coerced_val, err_msg = self._validate_type(
                    col_contract, raw_value
                )
                if not type_valid:
                    row_violations.append(SchemaViolation(
                        entity=entity,
                        phase=ValidationPhase.TYPE_COERCION,
                        severity=ValidationSeverity.HIGH,
                        column=col_name,
                        row_index=row_idx,
                        rule=f"type_{col_contract.col_type}",
                        message=err_msg,
                        actual_value=raw_value,
                        expected=col_contract.col_type,
                    ))
                else:
                    coerced_row[col_name] = coerced_val

            # Phase 3: Constraint checks
            for col_name, col_contract in column_contracts.items():
                value = coerced_row.get(col_name)
                constraint_violations = self._validate_constraints(
                    entity, col_contract, value, row_idx, unique_trackers
                )
                row_violations.extend(constraint_violations)

            # Phase 4: Referential integrity
            for col_name, col_contract in column_contracts.items():
                value = coerced_row.get(col_name)
                fk_violations = self._validate_foreign_keys(
                    entity, col_contract, value, row_idx
                )
                row_violations.extend(fk_violations)

            # Phase 5: Cross-column validation
            cross_violations = self._validate_cross_columns(
                entity, coerced_row, cross_validations, row_idx
            )
            row_violations.extend(cross_violations)

            # Classify row
            result.violations.extend(row_violations)
            has_critical = any(
                v.severity in (ValidationSeverity.CRITICAL, ValidationSeverity.HIGH)
                for v in row_violations
            )

            if has_critical:
                rejection_reasons = [v.message for v in row_violations
                                     if v.severity in (ValidationSeverity.CRITICAL, ValidationSeverity.HIGH)]
                coerced_row["_rejection_reasons"] = rejection_reasons
                coerced_row["_violated_rules"] = [v.rule for v in row_violations]
                result.rejected_rows.append(coerced_row)
            else:
                result.valid_rows.append(coerced_row)

        # ── Phase 6: Batch-level metrics ──
        self._validate_batch_metrics(entity, result)

        result.completed_at = datetime.now().isoformat()

        # Log summary
        logger.info(
            f"Schema validation [{entity}]: "
            f"{len(result.valid_rows)}/{result.total_rows} valid "
            f"({result.pass_rate}%), "
            f"{len(result.rejected_rows)} rejected, "
            f"{result.coercions_applied} coercions, "
            f"{len(result.violations)} violations"
        )

        return result

    # ================================================================
    # Phase 1: Schema Structure Validation
    # ================================================================

    def _validate_schema_structure(
        self,
        entity: str,
        sample_row: Dict,
        column_contracts: Dict[str, ColumnContract],
        result: PreLoadValidationResult,
    ):
        """Validate CSV columns match the declared schema contract."""
        csv_columns = set(sample_row.keys())
        contract_columns = set(column_contracts.keys())

        # Required columns (not nullable, no default)
        required_columns = {
            name for name, cc in column_contracts.items()
            if not cc.nullable and ("not_null" in cc.constraints or "primary_key" in cc.constraints)
        }

        # Check for missing required columns
        missing_required = required_columns - csv_columns
        if missing_required:
            result.batch_halted = True
            result.halt_reason = f"Missing required columns: {sorted(missing_required)}"
            result.violations.append(SchemaViolation(
                entity=entity,
                phase=ValidationPhase.SCHEMA_STRUCTURE,
                severity=ValidationSeverity.CRITICAL,
                column=None,
                row_index=None,
                rule="missing_required_columns",
                message=f"Required columns missing from CSV: {sorted(missing_required)}",
                expected=str(sorted(required_columns)),
            ))
            return

        # Check for missing optional columns
        missing_optional = contract_columns - csv_columns - missing_required
        if missing_optional and not self.global_settings.get("allow_missing_optional", True):
            for col in missing_optional:
                result.violations.append(SchemaViolation(
                    entity=entity,
                    phase=ValidationPhase.SCHEMA_STRUCTURE,
                    severity=ValidationSeverity.LOW,
                    column=col,
                    row_index=None,
                    rule="missing_optional_column",
                    message=f"Optional column '{col}' not found in CSV",
                ))

        # Check for extra columns (schema drift)
        extra_columns = csv_columns - contract_columns
        if extra_columns:
            strict = self.global_settings.get("strict_mode", True)
            allow_extra = self.global_settings.get("allow_extra_columns", False)

            if strict and not allow_extra:
                # Warn but don't halt — extra columns are ignored
                for col in extra_columns:
                    result.violations.append(SchemaViolation(
                        entity=entity,
                        phase=ValidationPhase.SCHEMA_STRUCTURE,
                        severity=ValidationSeverity.LOW,
                        column=col,
                        row_index=None,
                        rule="schema_drift_extra_column",
                        message=f"Column '{col}' in CSV not declared in schema contract (will be ignored)",
                    ))

        logger.info(
            f"Schema structure [{entity}]: "
            f"{len(csv_columns)} CSV cols, "
            f"{len(contract_columns)} contract cols, "
            f"{len(missing_required)} missing required, "
            f"{len(extra_columns)} extra"
        )

    # ================================================================
    # Phase 2: Type Validation
    # ================================================================

    def _validate_type(
        self, col_contract: ColumnContract, value: Any
    ) -> Tuple[bool, Any, str]:
        """Validate and coerce a value to the declared type."""
        # None/empty handling
        if value is None or str(value).strip() == "":
            if col_contract.nullable:
                return True, None, ""
            # not_null check happens in constraint phase
            return True, None, ""

        col_type = col_contract.col_type

        if col_type == "uuid":
            return self._type_validator.validate_uuid(
                value, strict_v4=self.global_settings.get("uuid_version", 4) == 4
            )
        elif col_type == "varchar" or col_type == "text":
            return self._type_validator.validate_varchar(value, col_contract.max_length)
        elif col_type == "integer":
            min_val = max_val = None
            for c in col_contract.constraints:
                if isinstance(c, dict) and "range" in c:
                    min_val = c["range"].get("min")
                    max_val = c["range"].get("max")
            return self._type_validator.validate_integer(value, min_val, max_val)
        elif col_type == "decimal":
            min_val = max_val = None
            for c in col_contract.constraints:
                if isinstance(c, dict) and "range" in c:
                    min_val = c["range"].get("min")
                    max_val = c["range"].get("max")
            return self._type_validator.validate_decimal(
                value, col_contract.precision or 12, col_contract.scale or 2,
                min_val, max_val
            )
        elif col_type == "date":
            fmt = col_contract.format or self.global_settings.get("date_format", "%Y-%m-%d")
            return self._type_validator.validate_date(value, fmt)
        elif col_type == "timestamp":
            fmt = col_contract.format or self.global_settings.get("timestamp_format", "%Y-%m-%dT%H:%M:%S")
            return self._type_validator.validate_timestamp(value, fmt)
        elif col_type == "boolean":
            return self._type_validator.validate_boolean(value)
        else:
            # Unknown type — pass through as string
            return True, str(value), ""

    # ================================================================
    # Phase 3: Constraint Validation
    # ================================================================

    def _validate_constraints(
        self,
        entity: str,
        col_contract: ColumnContract,
        value: Any,
        row_idx: int,
        unique_trackers: Dict[str, Set],
    ) -> List[SchemaViolation]:
        """Validate all declared constraints for a column value."""
        violations = []

        for constraint in col_contract.constraints:
            # Simple string constraints
            if isinstance(constraint, str):
                if constraint == "not_null" or constraint == "primary_key":
                    if value is None or (isinstance(value, str) and value.strip() == ""):
                        violations.append(SchemaViolation(
                            entity=entity,
                            phase=ValidationPhase.CONSTRAINT_CHECK,
                            severity=ValidationSeverity.HIGH,
                            column=col_contract.name,
                            row_index=row_idx,
                            rule="not_null",
                            message=f"Column '{col_contract.name}' violates NOT NULL constraint",
                            actual_value=value,
                            expected="non-null value",
                        ))

                if constraint == "unique" or constraint == "primary_key":
                    if col_contract.name in unique_trackers and value is not None:
                        if value in unique_trackers[col_contract.name]:
                            violations.append(SchemaViolation(
                                entity=entity,
                                phase=ValidationPhase.CONSTRAINT_CHECK,
                                severity=ValidationSeverity.HIGH,
                                column=col_contract.name,
                                row_index=row_idx,
                                rule="unique",
                                message=f"Duplicate value '{value}' in column '{col_contract.name}'",
                                actual_value=value,
                                expected="unique value",
                            ))
                        else:
                            unique_trackers[col_contract.name].add(value)

                if constraint == "not_future":
                    if value is not None:
                        try:
                            if isinstance(value, str):
                                dt = datetime.fromisoformat(value)
                            elif isinstance(value, (date, datetime)):
                                dt = value if isinstance(value, datetime) else datetime.combine(value, datetime.min.time())
                            else:
                                continue

                            if dt > datetime.now():
                                violations.append(SchemaViolation(
                                    entity=entity,
                                    phase=ValidationPhase.CONSTRAINT_CHECK,
                                    severity=ValidationSeverity.HIGH,
                                    column=col_contract.name,
                                    row_index=row_idx,
                                    rule="not_future",
                                    message=f"Value '{value}' is in the future for '{col_contract.name}'",
                                    actual_value=value,
                                    expected="date/time not in the future",
                                ))
                        except (ValueError, TypeError):
                            pass

            # Dict constraints (with parameters)
            elif isinstance(constraint, dict):
                if "regex" in constraint:
                    pattern = constraint["regex"]
                    if value is not None and isinstance(value, str):
                        if not re.match(pattern, value):
                            violations.append(SchemaViolation(
                                entity=entity,
                                phase=ValidationPhase.CONSTRAINT_CHECK,
                                severity=ValidationSeverity.MEDIUM,
                                column=col_contract.name,
                                row_index=row_idx,
                                rule="regex",
                                message=f"Value '{value}' does not match pattern '{pattern}'",
                                actual_value=value,
                                expected=f"match {pattern}",
                            ))

                if "enum" in constraint:
                    allowed = constraint["enum"]
                    if value is not None and value not in allowed:
                        violations.append(SchemaViolation(
                            entity=entity,
                            phase=ValidationPhase.CONSTRAINT_CHECK,
                            severity=ValidationSeverity.MEDIUM,
                            column=col_contract.name,
                            row_index=row_idx,
                            rule="enum",
                            message=f"Value '{value}' not in allowed values: {allowed}",
                            actual_value=value,
                            expected=f"one of {allowed}",
                        ))

                if "range" in constraint:
                    range_spec = constraint["range"]
                    if value is not None:
                        # For date ranges
                        if col_contract.col_type in ("date", "timestamp"):
                            try:
                                if isinstance(value, str):
                                    val_date = datetime.fromisoformat(value)
                                elif isinstance(value, (date, datetime)):
                                    val_date = value if isinstance(value, datetime) else datetime.combine(value, datetime.min.time())
                                else:
                                    continue

                                min_date_str = range_spec.get("min")
                                max_date_str = range_spec.get("max")

                                if min_date_str:
                                    min_dt = datetime.strptime(min_date_str, "%Y-%m-%d")
                                    if val_date < min_dt:
                                        violations.append(SchemaViolation(
                                            entity=entity,
                                            phase=ValidationPhase.CONSTRAINT_CHECK,
                                            severity=ValidationSeverity.MEDIUM,
                                            column=col_contract.name,
                                            row_index=row_idx,
                                            rule="date_range",
                                            message=f"Date '{value}' before minimum '{min_date_str}'",
                                            actual_value=value,
                                            expected=f">= {min_date_str}",
                                        ))
                                if max_date_str:
                                    max_dt = datetime.strptime(max_date_str, "%Y-%m-%d")
                                    if val_date > max_dt:
                                        violations.append(SchemaViolation(
                                            entity=entity,
                                            phase=ValidationPhase.CONSTRAINT_CHECK,
                                            severity=ValidationSeverity.MEDIUM,
                                            column=col_contract.name,
                                            row_index=row_idx,
                                            rule="date_range",
                                            message=f"Date '{value}' after maximum '{max_date_str}'",
                                            actual_value=value,
                                            expected=f"<= {max_date_str}",
                                        ))
                            except (ValueError, TypeError):
                                pass
                        # Numeric ranges handled in type validation
                        # Add explicit range check for already-coerced numeric values
                        elif col_contract.col_type in ("integer", "decimal"):
                            try:
                                num_val = float(value) if value is not None else None
                                if num_val is not None:
                                    min_v = range_spec.get("min")
                                    max_v = range_spec.get("max")
                                    if min_v is not None and num_val < float(min_v):
                                        violations.append(SchemaViolation(
                                            entity=entity,
                                            phase=ValidationPhase.CONSTRAINT_CHECK,
                                            severity=ValidationSeverity.HIGH,
                                            column=col_contract.name,
                                            row_index=row_idx,
                                            rule="range_check",
                                            message=f"Value {num_val} below minimum {min_v}",
                                            actual_value=num_val,
                                            expected=f">= {min_v}",
                                        ))
                                    if max_v is not None and num_val > float(max_v):
                                        violations.append(SchemaViolation(
                                            entity=entity,
                                            phase=ValidationPhase.CONSTRAINT_CHECK,
                                            severity=ValidationSeverity.HIGH,
                                            column=col_contract.name,
                                            row_index=row_idx,
                                            rule="range_check",
                                            message=f"Value {num_val} above maximum {max_v}",
                                            actual_value=num_val,
                                            expected=f"<= {max_v}",
                                        ))
                            except (ValueError, TypeError):
                                pass

                if "length" in constraint:
                    length_spec = constraint["length"]
                    if value is not None and isinstance(value, str):
                        min_len = length_spec.get("min", 0)
                        max_len = length_spec.get("max")
                        if len(value) < min_len:
                            violations.append(SchemaViolation(
                                entity=entity,
                                phase=ValidationPhase.CONSTRAINT_CHECK,
                                severity=ValidationSeverity.MEDIUM,
                                column=col_contract.name,
                                row_index=row_idx,
                                rule="min_length",
                                message=f"String length {len(value)} below minimum {min_len}",
                                actual_value=value,
                                expected=f">= {min_len} chars",
                            ))
                        if max_len and len(value) > max_len:
                            violations.append(SchemaViolation(
                                entity=entity,
                                phase=ValidationPhase.CONSTRAINT_CHECK,
                                severity=ValidationSeverity.MEDIUM,
                                column=col_contract.name,
                                row_index=row_idx,
                                rule="max_length",
                                message=f"String length {len(value)} exceeds maximum {max_len}",
                                actual_value=value,
                                expected=f"<= {max_len} chars",
                            ))

                if "date_range" in constraint:
                    dr = constraint["date_range"]
                    if value is not None:
                        try:
                            if isinstance(value, str):
                                val_date = datetime.fromisoformat(value)
                            elif isinstance(value, (date, datetime)):
                                val_date = value if isinstance(value, datetime) else datetime.combine(value, datetime.min.time())
                            else:
                                continue

                            min_d = dr.get("min")
                            max_d = dr.get("max")
                            if min_d:
                                min_dt = datetime.strptime(min_d, "%Y-%m-%d")
                                if val_date < min_dt:
                                    violations.append(SchemaViolation(
                                        entity=entity,
                                        phase=ValidationPhase.CONSTRAINT_CHECK,
                                        severity=ValidationSeverity.MEDIUM,
                                        column=col_contract.name,
                                        row_index=row_idx,
                                        rule="date_range",
                                        message=f"Date '{value}' before min '{min_d}'",
                                        actual_value=value,
                                        expected=f">= {min_d}",
                                    ))
                            if max_d:
                                max_dt = datetime.strptime(max_d, "%Y-%m-%d")
                                if val_date > max_dt:
                                    violations.append(SchemaViolation(
                                        entity=entity,
                                        phase=ValidationPhase.CONSTRAINT_CHECK,
                                        severity=ValidationSeverity.MEDIUM,
                                        column=col_contract.name,
                                        row_index=row_idx,
                                        rule="date_range",
                                        message=f"Date '{value}' after max '{max_d}'",
                                        actual_value=value,
                                        expected=f"<= {max_d}",
                                    ))
                        except (ValueError, TypeError):
                            pass

                # Foreign key placeholder (handled in phase 4)
                # Handled separately in _validate_foreign_keys

        return violations

    # ================================================================
    # Phase 4: Referential Integrity
    # ================================================================

    def _validate_foreign_keys(
        self,
        entity: str,
        col_contract: ColumnContract,
        value: Any,
        row_idx: int,
    ) -> List[SchemaViolation]:
        """Validate foreign key references against loaded reference data."""
        violations = []

        for constraint in col_contract.constraints:
            if isinstance(constraint, dict) and "foreign_key" in constraint:
                fk_spec = constraint["foreign_key"]
                ref_entity = fk_spec.get("entity")
                ref_column = fk_spec.get("column")

                if value is None:
                    continue  # NULL FK handled by not_null constraint

                # Get reference data
                ref_data = self._reference_data.get(ref_entity, [])
                if not ref_data:
                    violations.append(SchemaViolation(
                        entity=entity,
                        phase=ValidationPhase.REFERENTIAL_INTEGRITY,
                        severity=ValidationSeverity.MEDIUM,
                        column=col_contract.name,
                        row_index=row_idx,
                        rule="fk_reference_unavailable",
                        message=f"Reference entity '{ref_entity}' not available for FK check on '{col_contract.name}'",
                        actual_value=value,
                    ))
                    continue

                # Check if value exists in reference entity
                ref_values = {str(row.get(ref_column, "")) for row in ref_data}
                if str(value) not in ref_values:
                    violations.append(SchemaViolation(
                        entity=entity,
                        phase=ValidationPhase.REFERENTIAL_INTEGRITY,
                        severity=ValidationSeverity.HIGH,
                        column=col_contract.name,
                        row_index=row_idx,
                        rule="foreign_key",
                        message=f"Orphan reference: '{value}' not found in {ref_entity}.{ref_column}",
                        actual_value=value,
                        expected=f"valid {ref_entity}.{ref_column}",
                    ))

        return violations

    # ================================================================
    # Phase 5: Cross-Column Validation
    # ================================================================

    def _validate_cross_columns(
        self,
        entity: str,
        row: Dict,
        cross_validations: List[Dict],
        row_idx: int,
    ) -> List[SchemaViolation]:
        """Evaluate multi-column business rules."""
        violations = []

        for rule in cross_validations:
            name = rule.get("name", "cross_column_rule")
            expression = rule.get("expression", "")
            severity_str = rule.get("severity", "HIGH")
            description = rule.get("description", expression)

            try:
                # Build safe evaluation context
                eval_context = {}
                for key, value in row.items():
                    if key.startswith("_"):
                        continue
                    if value is None:
                        eval_context[key] = None
                        continue

                    # Try numeric conversion
                    try:
                        eval_context[key] = float(value)
                    except (ValueError, TypeError):
                        # Try datetime
                        try:
                            eval_context[key] = datetime.fromisoformat(str(value))
                        except (ValueError, TypeError):
                            eval_context[key] = value

                # Add builtins for expressions
                eval_context["abs"] = abs
                eval_context["min"] = min
                eval_context["max"] = max

                # Skip if any referenced column is None
                skip = False
                for key, val in eval_context.items():
                    if val is None and key in expression:
                        skip = True
                        break

                if skip:
                    continue

                result = eval(expression, {"__builtins__": {}}, eval_context)

                if not result:
                    severity = ValidationSeverity[severity_str] if severity_str in ValidationSeverity.__members__ else ValidationSeverity.MEDIUM
                    violations.append(SchemaViolation(
                        entity=entity,
                        phase=ValidationPhase.CROSS_COLUMN,
                        severity=severity,
                        column=None,
                        row_index=row_idx,
                        rule=name,
                        message=f"Cross-column check failed: {description}",
                        expected=expression,
                    ))

            except Exception as e:
                logger.debug(f"Cross-column rule '{name}' evaluation error: {e}")

        return violations

    # ================================================================
    # Phase 6: Batch-Level Metrics
    # ================================================================

    def _validate_batch_metrics(self, entity: str, result: PreLoadValidationResult):
        """Evaluate batch-level thresholds after all rows are processed."""
        max_reject_pct = self.global_settings.get("max_type_cast_errors_pct", 5.0)

        if result.rejection_rate > max_reject_pct:
            result.batch_halted = True
            result.halt_reason = (
                f"Rejection rate {result.rejection_rate}% exceeds "
                f"threshold {max_reject_pct}%"
            )
            result.violations.append(SchemaViolation(
                entity=entity,
                phase=ValidationPhase.BATCH_METRICS,
                severity=ValidationSeverity.CRITICAL,
                column=None,
                row_index=None,
                rule="max_rejection_rate",
                message=result.halt_reason,
                actual_value=result.rejection_rate,
                expected=f"<= {max_reject_pct}%",
            ))

    # ================================================================
    # Full Pipeline Validation
    # ================================================================

    def validate_all(
        self,
        customers: List[Dict],
        products: List[Dict],
        sales: List[Dict],
    ) -> Dict[str, Any]:
        """
        Validate all entities in dependency order:
        1. Customers (no FK dependencies)
        2. Products (no FK dependencies)
        3. Sales (FK → customers, products)

        Returns comprehensive validation report.
        """
        report = {
            "report_id": f"schema_val_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "validation_type": "pre_load_schema",
            "started_at": datetime.now().isoformat(),
            "entities": {},
            "overall_status": "PASS",
        }

        # 1. Validate customers
        logger.info("Pre-load validation: customers...")
        cust_result = self.validate("customers", customers)
        report["entities"]["customers"] = {
            "summary": cust_result.get_summary(),
            "violation_details": [
                {
                    "rule": v.rule,
                    "phase": v.phase.value,
                    "severity": v.severity.value,
                    "column": v.column,
                    "message": v.message,
                    "count": 1,
                }
                for v in cust_result.violations[:50]  # Cap detail output
            ],
        }

        # 2. Validate products
        logger.info("Pre-load validation: products...")
        prod_result = self.validate("products", products)
        report["entities"]["products"] = {
            "summary": prod_result.get_summary(),
            "violation_details": [
                {
                    "rule": v.rule,
                    "phase": v.phase.value,
                    "severity": v.severity.value,
                    "column": v.column,
                    "message": v.message,
                    "count": 1,
                }
                for v in prod_result.violations[:50]
            ],
        }

        # 3. Validate sales (with FK references)
        logger.info("Pre-load validation: sales...")
        self.set_reference_data("customers", customers)
        self.set_reference_data("products", products)
        sales_result = self.validate(
            "sales", sales,
            reference_data={"customers": customers, "products": products}
        )
        report["entities"]["sales"] = {
            "summary": sales_result.get_summary(),
            "violation_details": [
                {
                    "rule": v.rule,
                    "phase": v.phase.value,
                    "severity": v.severity.value,
                    "column": v.column,
                    "message": v.message,
                    "count": 1,
                }
                for v in sales_result.violations[:50]
            ],
        }

        # Overall status
        any_halted = any(
            r["summary"]["batch_halted"]
            for r in report["entities"].values()
        )
        any_rejected = any(
            r["summary"]["rejected_rows"] > 0
            for r in report["entities"].values()
        )

        if any_halted:
            report["overall_status"] = "HALT"
        elif any_rejected:
            report["overall_status"] = "WARN"
        else:
            report["overall_status"] = "PASS"

        # Totals
        total_rows = sum(r["summary"]["total_rows"] for r in report["entities"].values())
        total_valid = sum(r["summary"]["valid_rows"] for r in report["entities"].values())
        total_rejected = sum(r["summary"]["rejected_rows"] for r in report["entities"].values())
        total_violations = sum(r["summary"]["total_violations"] for r in report["entities"].values())
        total_coercions = sum(r["summary"]["coercions_applied"] for r in report["entities"].values())

        report["totals"] = {
            "total_rows": total_rows,
            "total_valid": total_valid,
            "total_rejected": total_rejected,
            "total_violations": total_violations,
            "total_coercions": total_coercions,
            "overall_pass_rate": round(total_valid / total_rows * 100, 2) if total_rows > 0 else 100.0,
        }

        report["completed_at"] = datetime.now().isoformat()

        # Collect validated data for downstream use
        report["_validated_data"] = {
            "customers": cust_result.valid_rows,
            "products": prod_result.valid_rows,
            "sales": sales_result.valid_rows,
        }
        report["_rejected_data"] = {
            "customers": cust_result.rejected_rows,
            "products": prod_result.rejected_rows,
            "sales": sales_result.rejected_rows,
        }

        logger.info(
            f"Pre-load schema validation complete: "
            f"status={report['overall_status']}, "
            f"valid={total_valid}/{total_rows} ({report['totals']['overall_pass_rate']}%), "
            f"rejected={total_rejected}, coercions={total_coercions}"
        )

        return report
