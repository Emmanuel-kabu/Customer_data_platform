"""
Data Quality Rules Engine
=========================
Shift-left data quality: validates data IN-MEMORY before it reaches storage.

This engine evaluates declarative YAML rules against pandas-free Python dicts,
ensuring zero-dependency validation at the earliest possible point.

Architecture:
    generate_data.py  ──→  dq_rules_engine.py  ──→  MinIO upload
                              │                        │
                              ├── PASS → raw-data      │
                              └── FAIL → quarantine    │

Rule Evaluation Order:
    1. CRITICAL rules (batch-level: halt entire batch)
    2. HIGH rules    (row-level: quarantine failing rows)
    3. MEDIUM rules  (row-level: flag warnings)
    4. LOW rules     (row-level: informational logging)
"""

import os
import re
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import yaml

logger = logging.getLogger(__name__)


# ============================================================
# Enums & Data Classes
# ============================================================

class Severity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    @property
    def weight(self) -> int:
        return {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}[self.value]


class ViolationAction(Enum):
    HALT_BATCH = "halt_batch"
    QUARANTINE_ROW = "quarantine_row"
    FLAG_WARNING = "flag_warning"
    LOG_ONLY = "log_only"


SEVERITY_ACTION_MAP = {
    Severity.CRITICAL: ViolationAction.HALT_BATCH,
    Severity.HIGH: ViolationAction.QUARANTINE_ROW,
    Severity.MEDIUM: ViolationAction.FLAG_WARNING,
    Severity.LOW: ViolationAction.LOG_ONLY,
}


@dataclass
class RuleDefinition:
    """A single data quality rule parsed from YAML."""
    name: str
    rule_type: str
    severity: Severity
    description: str = ""
    column: Optional[str] = None
    # Type-specific parameters
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[List[str]] = None
    format: Optional[str] = None
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    expression: Optional[str] = None
    reference_entity: Optional[str] = None
    reference_column: Optional[str] = None
    min_rows: Optional[int] = None
    columns: Optional[List[str]] = None
    min_completeness_pct: Optional[float] = None


@dataclass
class Violation:
    """A single rule violation on a specific row."""
    rule_name: str
    rule_type: str
    severity: Severity
    action: ViolationAction
    description: str
    column: Optional[str] = None
    row_index: Optional[int] = None
    row_data: Optional[Dict] = None
    actual_value: Any = None
    expected: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ValidationResult:
    """Complete result of validating a dataset against all rules."""
    entity: str
    total_rows: int
    total_rules: int
    rules_evaluated: int = 0
    violations: List[Violation] = field(default_factory=list)
    clean_rows: List[Dict] = field(default_factory=list)
    quarantined_rows: List[Dict] = field(default_factory=list)
    warnings: List[Dict] = field(default_factory=list)
    batch_halted: bool = False
    halt_reason: str = ""
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    profile: Optional[Dict] = None

    @property
    def is_valid(self) -> bool:
        return not self.batch_halted and len(self.quarantined_rows) == 0

    @property
    def pass_rate(self) -> float:
        if self.total_rows == 0:
            return 100.0
        return round((len(self.clean_rows) / self.total_rows) * 100, 2)

    @property
    def quarantine_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round((len(self.quarantined_rows) / self.total_rows) * 100, 2)

    def get_summary(self) -> Dict:
        violations_by_severity = {}
        for v in self.violations:
            key = v.severity.value
            violations_by_severity[key] = violations_by_severity.get(key, 0) + 1

        violations_by_rule = {}
        for v in self.violations:
            violations_by_rule[v.rule_name] = violations_by_rule.get(v.rule_name, 0) + 1

        return {
            "entity": self.entity,
            "total_rows": self.total_rows,
            "total_rules": self.total_rules,
            "rules_evaluated": self.rules_evaluated,
            "clean_rows": len(self.clean_rows),
            "quarantined_rows": len(self.quarantined_rows),
            "warnings_count": len(self.warnings),
            "pass_rate": self.pass_rate,
            "quarantine_rate": self.quarantine_rate,
            "batch_halted": self.batch_halted,
            "halt_reason": self.halt_reason,
            "violations_by_severity": violations_by_severity,
            "violations_by_rule": violations_by_rule,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


# ============================================================
# Rules Engine
# ============================================================

class DataQualityRulesEngine:
    """
    Core rules engine that evaluates declarative DQ rules against in-memory data.

    Follows shift-left principle: validate BEFORE data reaches storage.
    """

    def __init__(self, rules_path: Optional[str] = None):
        """
        Initialize engine with rules from YAML config.

        Args:
            rules_path: Path to data_quality_rules.yml. Auto-detected if None.
        """
        self.rules_path = rules_path or self._find_rules_file()
        self.config = self._load_config()
        self.global_settings = self.config.get("global_settings", {})
        self._reference_data: Dict[str, List[Dict]] = {}

    def _find_rules_file(self) -> str:
        """Auto-detect rules file location."""
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "config", "data_quality_rules.yml"),
            os.path.join(os.path.dirname(__file__), "config", "data_quality_rules.yml"),
            "/opt/airflow/config/data_quality_rules.yml",
            "/app/config/data_quality_rules.yml",
        ]
        for path in candidates:
            normalized = os.path.normpath(path)
            if os.path.exists(normalized):
                return normalized
        raise FileNotFoundError(
            "data_quality_rules.yml not found. Searched: " + ", ".join(candidates)
        )

    def _load_config(self) -> Dict:
        """Load and parse YAML rules configuration."""
        with open(self.rules_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded DQ rules from {self.rules_path}")
        return config

    def register_reference_data(self, entity: str, data: List[Dict]):
        """
        Register reference data for referential integrity checks.
        Must be called before validate() if rules use 'referential' type.
        """
        self._reference_data[entity] = data
        logger.debug(f"Registered reference data: {entity} ({len(data)} rows)")

    def _parse_rules(self, entity: str) -> List[RuleDefinition]:
        """Parse rules for a specific entity from config."""
        entity_config = self.config.get(entity, {})
        raw_rules = entity_config.get("rules", [])
        rules = []

        for r in raw_rules:
            rule = RuleDefinition(
                name=r["name"],
                rule_type=r["type"],
                severity=Severity(r.get("severity", "MEDIUM")),
                description=r.get("description", ""),
                column=r.get("column"),
                pattern=r.get("pattern"),
                min_value=r.get("min_value"),
                max_value=r.get("max_value"),
                min_length=r.get("min_length"),
                max_length=r.get("max_length"),
                allowed_values=r.get("allowed_values"),
                format=r.get("format"),
                min_date=r.get("min_date"),
                max_date=r.get("max_date"),
                expression=r.get("expression"),
                reference_entity=r.get("reference_entity"),
                reference_column=r.get("reference_column"),
                min_rows=r.get("min_rows"),
                columns=r.get("columns"),
                min_completeness_pct=r.get("min_completeness_pct"),
            )
            rules.append(rule)

        # Sort by severity (CRITICAL first)
        rules.sort(key=lambda x: x.severity.weight, reverse=True)
        return rules

    # --------------------------------------------------------
    # Validate entry point
    # --------------------------------------------------------

    def validate(self, entity: str, data: List[Dict]) -> ValidationResult:
        """
        Validate a dataset against all rules for the given entity.

        Args:
            entity: Entity name (customers, products, sales)
            data: List of row dictionaries to validate

        Returns:
            ValidationResult with clean rows, quarantined rows, and violations
        """
        rules = self._parse_rules(entity)
        result = ValidationResult(
            entity=entity,
            total_rows=len(data),
            total_rules=len(rules),
        )

        if not data:
            logger.warning(f"[DQ] {entity}: Empty dataset provided")
            result.completed_at = datetime.now().isoformat()
            return result

        # Profile the data if enabled
        if self.global_settings.get("enable_profiling", False):
            result.profile = self._profile_data(entity, data)

        # Phase 1: Batch-level rules (row_count, completeness)
        batch_rules = [r for r in rules if r.rule_type in ("row_count", "completeness")]
        for rule in batch_rules:
            violations = self._evaluate_batch_rule(rule, data)
            result.violations.extend(violations)
            result.rules_evaluated += 1

            if violations and rule.severity == Severity.CRITICAL:
                halt = self.global_settings.get("halt_on_critical", True)
                if halt:
                    result.batch_halted = True
                    result.halt_reason = (
                        f"CRITICAL batch rule failed: {rule.name} — {rule.description}"
                    )
                    logger.error(f"[DQ] BATCH HALTED: {result.halt_reason}")
                    result.completed_at = datetime.now().isoformat()
                    return result

        # Phase 2: Row-level rules
        row_rules = [r for r in rules if r.rule_type not in ("row_count", "completeness")]
        quarantined_indices = set()
        row_warnings: Dict[int, List[str]] = {}

        for idx, row in enumerate(data):
            row_violations = []

            for rule in row_rules:
                violation = self._evaluate_row_rule(rule, row, idx, data)
                if violation:
                    row_violations.append(violation)
                    result.violations.append(violation)

                    if violation.action == ViolationAction.HALT_BATCH:
                        result.batch_halted = True
                        result.halt_reason = (
                            f"CRITICAL row rule failed: {rule.name} at row {idx} — {rule.description}"
                        )
                        logger.error(f"[DQ] BATCH HALTED: {result.halt_reason}")
                        result.completed_at = datetime.now().isoformat()
                        return result

                    elif violation.action == ViolationAction.QUARANTINE_ROW:
                        quarantined_indices.add(idx)

                    elif violation.action == ViolationAction.FLAG_WARNING:
                        row_warnings.setdefault(idx, []).append(rule.name)

            result.rules_evaluated += len(row_rules)

        # Separate clean vs quarantined rows
        for idx, row in enumerate(data):
            if idx in quarantined_indices:
                row_copy = dict(row)
                row_copy["_quarantine_reasons"] = [
                    v.rule_name for v in result.violations
                    if v.row_index == idx and v.action == ViolationAction.QUARANTINE_ROW
                ]
                result.quarantined_rows.append(row_copy)
            else:
                clean = dict(row)
                if idx in row_warnings:
                    clean["_dq_warnings"] = row_warnings[idx]
                result.clean_rows.append(clean)

        # Phase 3: Check quarantine threshold
        max_q_pct = self.global_settings.get("max_quarantine_pct", 15.0)
        if result.quarantine_rate > max_q_pct:
            result.batch_halted = True
            result.halt_reason = (
                f"Quarantine rate {result.quarantine_rate}% exceeds threshold {max_q_pct}%"
            )
            logger.error(f"[DQ] BATCH HALTED: {result.halt_reason}")

        result.completed_at = datetime.now().isoformat()

        # Log summary
        summary = result.get_summary()
        logger.info(
            f"[DQ] {entity}: {summary['clean_rows']}/{summary['total_rows']} clean, "
            f"{summary['quarantined_rows']} quarantined, "
            f"{summary['warnings_count']} warnings, "
            f"pass_rate={summary['pass_rate']}%"
        )

        return result

    # --------------------------------------------------------
    # Batch-level rule evaluators
    # --------------------------------------------------------

    def _evaluate_batch_rule(self, rule: RuleDefinition, data: List[Dict]) -> List[Violation]:
        """Evaluate a batch-level rule against the entire dataset."""
        if rule.rule_type == "row_count":
            return self._check_row_count(rule, data)
        elif rule.rule_type == "completeness":
            return self._check_completeness(rule, data)
        return []

    def _check_row_count(self, rule: RuleDefinition, data: List[Dict]) -> List[Violation]:
        min_rows = rule.min_rows or 1
        if len(data) < min_rows:
            return [Violation(
                rule_name=rule.name,
                rule_type=rule.rule_type,
                severity=rule.severity,
                action=SEVERITY_ACTION_MAP[rule.severity],
                description=rule.description,
                actual_value=len(data),
                expected=f">= {min_rows} rows",
            )]
        return []

    def _check_completeness(self, rule: RuleDefinition, data: List[Dict]) -> List[Violation]:
        columns = rule.columns or []
        threshold = rule.min_completeness_pct or 95.0
        violations = []

        for col in columns:
            total = len(data)
            non_null = sum(1 for row in data if row.get(col) not in (None, "", "None", "null"))
            pct = (non_null / total * 100) if total > 0 else 0

            if pct < threshold:
                violations.append(Violation(
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    severity=rule.severity,
                    action=SEVERITY_ACTION_MAP[rule.severity],
                    description=f"{col}: {pct:.1f}% complete (need {threshold}%)",
                    column=col,
                    actual_value=round(pct, 2),
                    expected=f">= {threshold}%",
                ))

        return violations

    # --------------------------------------------------------
    # Row-level rule evaluators
    # --------------------------------------------------------

    def _evaluate_row_rule(
        self, rule: RuleDefinition, row: Dict, idx: int, all_data: List[Dict]
    ) -> Optional[Violation]:
        """Evaluate a single rule against a single row."""
        evaluators = {
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "regex": self._check_regex,
            "range": self._check_range,
            "enum": self._check_enum,
            "length": self._check_length,
            "date_format": self._check_date_format,
            "date_range": self._check_date_range,
            "date_not_future": self._check_date_not_future,
            "cross_field": self._check_cross_field,
            "referential": self._check_referential,
        }

        evaluator = evaluators.get(rule.rule_type)
        if not evaluator:
            logger.warning(f"Unknown rule type: {rule.rule_type}")
            return None

        return evaluator(rule, row, idx, all_data)

    def _make_violation(
        self, rule: RuleDefinition, idx: int, row: Dict,
        actual: Any, expected: str
    ) -> Violation:
        """Helper to create a Violation instance."""
        return Violation(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            severity=rule.severity,
            action=SEVERITY_ACTION_MAP[rule.severity],
            description=rule.description,
            column=rule.column,
            row_index=idx,
            row_data=dict(row) if self.global_settings.get("log_all_violations", False) else None,
            actual_value=actual,
            expected=expected,
        )

    def _check_not_null(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None or str(val).strip() in ("", "None", "null", "NaN"):
            return self._make_violation(rule, idx, row, val, "NOT NULL")
        return None

    def _check_unique(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None  # nulls handled by not_null rule
        count = sum(1 for r in all_data if r.get(rule.column) == val)
        if count > 1:
            return self._make_violation(rule, idx, row, f"{val} (appears {count}x)", "UNIQUE")
        return None

    def _check_regex(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        if not re.match(rule.pattern, str(val)):
            return self._make_violation(rule, idx, row, val, f"matches /{rule.pattern}/")
        return None

    def _check_range(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        try:
            num_val = float(val)
        except (ValueError, TypeError):
            return self._make_violation(rule, idx, row, val, "numeric value")

        if rule.min_value is not None and num_val < rule.min_value:
            return self._make_violation(
                rule, idx, row, num_val, f">= {rule.min_value}"
            )
        if rule.max_value is not None and num_val > rule.max_value:
            return self._make_violation(
                rule, idx, row, num_val, f"<= {rule.max_value}"
            )
        return None

    def _check_enum(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        if str(val) not in (rule.allowed_values or []):
            return self._make_violation(
                rule, idx, row, val, f"one of {rule.allowed_values}"
            )
        return None

    def _check_length(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        length = len(str(val))
        if rule.min_length is not None and length < rule.min_length:
            return self._make_violation(
                rule, idx, row, f"len={length}", f">= {rule.min_length} chars"
            )
        if rule.max_length is not None and length > rule.max_length:
            return self._make_violation(
                rule, idx, row, f"len={length}", f"<= {rule.max_length} chars"
            )
        return None

    def _check_date_format(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        fmt = rule.format or self.global_settings.get("default_date_format", "%Y-%m-%d")
        try:
            datetime.strptime(str(val)[:len(datetime.now().strftime(fmt))], fmt)
        except ValueError:
            return self._make_violation(rule, idx, row, val, f"date format {fmt}")
        return None

    def _check_date_range(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        try:
            parsed = self._parse_date(str(val))
        except ValueError:
            return self._make_violation(rule, idx, row, val, "valid date")

        if rule.min_date:
            min_d = datetime.strptime(rule.min_date, "%Y-%m-%d").date()
            if parsed < min_d:
                return self._make_violation(
                    rule, idx, row, str(parsed), f">= {rule.min_date}"
                )
        if rule.max_date:
            max_d = datetime.strptime(rule.max_date, "%Y-%m-%d").date()
            if parsed > max_d:
                return self._make_violation(
                    rule, idx, row, str(parsed), f"<= {rule.max_date}"
                )
        return None

    def _check_date_not_future(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None
        try:
            parsed = self._parse_datetime(str(val))
        except ValueError:
            return self._make_violation(rule, idx, row, val, "valid datetime")

        if parsed > datetime.now():
            return self._make_violation(rule, idx, row, str(parsed), "not in the future")
        return None

    def _check_cross_field(self, rule, row, idx, all_data) -> Optional[Violation]:
        """
        Evaluate a Python expression across multiple columns in a row.
        Expression has access to all row fields as local variables.
        """
        expr = rule.expression
        if not expr:
            return None

        try:
            # Build evaluation context from row values
            ctx = {}
            for key, val in row.items():
                if key.startswith("_"):
                    continue
                ctx[key] = self._coerce_value(val)

            result = eval(expr, {"__builtins__": {"abs": abs, "len": len, "str": str, "float": float, "int": int}}, ctx)

            if not result:
                return self._make_violation(
                    rule, idx, row,
                    f"expression={expr} evaluated to False",
                    f"{expr} == True",
                )
        except Exception as e:
            logger.debug(f"Cross-field check '{rule.name}' eval error on row {idx}: {e}")
            return self._make_violation(
                rule, idx, row, f"eval error: {e}", f"{expr} evaluable"
            )
        return None

    def _check_referential(self, rule, row, idx, all_data) -> Optional[Violation]:
        val = row.get(rule.column)
        if val is None:
            return None

        ref_entity = rule.reference_entity
        ref_column = rule.reference_column

        ref_data = self._reference_data.get(ref_entity, [])
        if not ref_data:
            logger.debug(f"No reference data for '{ref_entity}', skipping referential check")
            return None

        ref_values = {r.get(ref_column) for r in ref_data}
        if val not in ref_values:
            return self._make_violation(
                rule, idx, row, val,
                f"exists in {ref_entity}.{ref_column}",
            )
        return None

    # --------------------------------------------------------
    # Data Profiling
    # --------------------------------------------------------

    def _profile_data(self, entity: str, data: List[Dict]) -> Dict:
        """Generate statistical profile of the dataset."""
        if not data:
            return {}

        columns = list(data[0].keys())
        profile = {
            "row_count": len(data),
            "column_count": len(columns),
            "columns": {},
        }

        for col in columns:
            if col.startswith("_"):
                continue
            values = [row.get(col) for row in data]
            non_null = [v for v in values if v is not None and str(v) not in ("", "None")]
            unique_vals = set(str(v) for v in non_null)

            col_profile = {
                "non_null_count": len(non_null),
                "null_count": len(values) - len(non_null),
                "null_pct": round((len(values) - len(non_null)) / len(values) * 100, 2),
                "unique_count": len(unique_vals),
                "uniqueness_pct": round(len(unique_vals) / len(values) * 100, 2) if values else 0,
            }

            # Numeric stats
            numeric_vals = []
            for v in non_null:
                try:
                    numeric_vals.append(float(v))
                except (ValueError, TypeError):
                    pass

            if numeric_vals:
                col_profile["min"] = min(numeric_vals)
                col_profile["max"] = max(numeric_vals)
                col_profile["mean"] = round(sum(numeric_vals) / len(numeric_vals), 4)

            # String length stats
            if non_null and not numeric_vals:
                lengths = [len(str(v)) for v in non_null]
                col_profile["min_length"] = min(lengths)
                col_profile["max_length"] = max(lengths)
                col_profile["avg_length"] = round(sum(lengths) / len(lengths), 1)

            profile["columns"][col] = col_profile

        return profile

    # --------------------------------------------------------
    # Utility methods
    # --------------------------------------------------------

    @staticmethod
    def _parse_date(val: str) -> date:
        """Parse a date string, trying multiple common formats."""
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                    "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(val[:len(datetime.now().strftime(fmt))], fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date: {val}")

    @staticmethod
    def _parse_datetime(val: str) -> datetime:
        """Parse a datetime string, trying multiple common formats."""
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val[:len(datetime.now().strftime(fmt))], fmt)
            except ValueError:
                continue
        raise ValueError(f"Unable to parse datetime: {val}")

    @staticmethod
    def _coerce_value(val: Any) -> Any:
        """Coerce string values to native Python types for expression evaluation."""
        if val is None:
            return None
        s = str(val)

        # Boolean
        if s.lower() in ("true", "false"):
            return s.lower() == "true"

        # Integer
        try:
            return int(s)
        except ValueError:
            pass

        # Float
        try:
            return float(s)
        except ValueError:
            pass

        # Date/datetime
        try:
            return datetime.fromisoformat(s)
        except (ValueError, TypeError):
            pass

        return s
