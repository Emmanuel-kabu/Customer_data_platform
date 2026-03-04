"""
Data Quality Checks - Validates data at each pipeline stage.
Implements comprehensive quality validation for the CDP.

Two-tier architecture:
    Tier 1 (Shift-Left): Pre-ingestion validation via dq_rules_engine
        → Applied at data generation BEFORE MinIO upload
    Tier 2 (In-Pipeline): Post-staging SQL-based validation
        → Applied after data lands in PostgreSQL staging/warehouse
"""

import json
import logging
from datetime import datetime
from typing import Optional, List, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Runs data quality checks and logs results to the audit schema."""

    def __init__(self, conn_params: dict, pipeline_run_id: Optional[int] = None):
        self.conn_params = conn_params
        self.pipeline_run_id = pipeline_run_id
        self.results = []

    def _get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def run_check(self, check_name: str, table_name: str, check_type: str,
                  check_query: str, expected_result: str) -> bool:
        """Run a single data quality check and log the result."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(check_query)
            result = cursor.fetchone()
            actual_result = str(result[0]) if result else "NULL"
            is_passed = actual_result == expected_result

            # Log to audit table
            cursor.execute("""
                INSERT INTO audit.data_quality_checks
                (check_name, table_name, check_type, check_query, expected_result,
                 actual_result, is_passed, pipeline_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (check_name, table_name, check_type, check_query,
                  expected_result, actual_result, is_passed, self.pipeline_run_id))

            conn.commit()
            cursor.close()
            conn.close()

            status = "PASSED" if is_passed else "FAILED"
            logger.info(f"Quality Check [{status}] {check_name}: expected={expected_result}, actual={actual_result}")

            self.results.append({
                "check_name": check_name,
                "table_name": table_name,
                "is_passed": is_passed,
                "expected": expected_result,
                "actual": actual_result
            })

            return is_passed

        except Exception as e:
            logger.error(f"Quality check '{check_name}' failed with error: {e}")
            return False

    def check_row_count(self, table_name: str, min_rows: int = 1) -> bool:
        """Check that a table has at least min_rows records."""
        return self.run_check(
            check_name=f"row_count_{table_name}",
            table_name=table_name,
            check_type="row_count",
            check_query=f"SELECT CASE WHEN COUNT(*) >= {min_rows} THEN 'PASS' ELSE 'FAIL' END FROM {table_name}",
            expected_result="PASS"
        )

    def check_null_values(self, table_name: str, column_name: str) -> bool:
        """Check that a column has no NULL values."""
        return self.run_check(
            check_name=f"null_check_{table_name}_{column_name}",
            table_name=table_name,
            check_type="null_check",
            check_query=f"SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM {table_name} WHERE {column_name} IS NULL",
            expected_result="PASS"
        )

    def check_unique_values(self, table_name: str, column_name: str) -> bool:
        """Check that a column has unique values."""
        return self.run_check(
            check_name=f"unique_check_{table_name}_{column_name}",
            table_name=table_name,
            check_type="uniqueness",
            check_query=f"SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT {column_name}) THEN 'PASS' ELSE 'FAIL' END FROM {table_name}",
            expected_result="PASS"
        )

    def check_referential_integrity(self, fact_table: str, fact_column: str,
                                     dim_table: str, dim_column: str) -> bool:
        """Check referential integrity between fact and dimension tables."""
        return self.run_check(
            check_name=f"ref_integrity_{fact_table}_{fact_column}",
            table_name=fact_table,
            check_type="referential_integrity",
            check_query=f"""
                SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM {fact_table} f
                LEFT JOIN {dim_table} d ON f.{fact_column} = d.{dim_column}
                WHERE d.{dim_column} IS NULL AND f.{fact_column} IS NOT NULL
            """,
            expected_result="PASS"
        )

    def check_value_range(self, table_name: str, column_name: str,
                          min_val: float, max_val: float) -> bool:
        """Check that values fall within expected range."""
        return self.run_check(
            check_name=f"range_check_{table_name}_{column_name}",
            table_name=table_name,
            check_type="value_range",
            check_query=f"""
                SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM {table_name}
                WHERE {column_name} < {min_val} OR {column_name} > {max_val}
            """,
            expected_result="PASS"
        )

    def get_summary(self) -> dict:
        """Get summary of all quality checks run."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["is_passed"])
        failed = total - passed

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
            "failed_checks": [r for r in self.results if not r["is_passed"]]
        }

    # ============================================================
    # Tier 2: Post-Staging Comprehensive Checks
    # ============================================================

    def run_staging_quality_checks(self) -> dict:
        """
        Run comprehensive quality checks on staging tables.
        These are the second defense layer AFTER shift-left ingestion validation.
        """
        logger.info("Running Tier 2 post-staging quality checks...")

        checks = [
            # ─── Staging Completeness ───
            ("staging_customers_not_empty", "staging.customers_raw", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw", "PASS"),
            ("staging_products_not_empty", "staging.products_raw", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw", "PASS"),
            ("staging_sales_not_empty", "staging.sales_raw", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw", "PASS"),

            # ─── PK Integrity ───
            ("staging_customer_id_not_null", "staging.customers_raw", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw WHERE source_id IS NULL", "PASS"),
            ("staging_product_id_not_null", "staging.products_raw", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw WHERE source_id IS NULL", "PASS"),
            ("staging_sale_id_not_null", "staging.sales_raw", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE source_id IS NULL", "PASS"),

            # ─── UUID Format Validation (all IDs must be valid UUIDs) ───
            ("staging_customer_id_uuid", "staging.customers_raw", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw WHERE NOT is_valid_uuid(source_id)", "PASS"),
            ("staging_product_id_uuid", "staging.products_raw", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw WHERE NOT is_valid_uuid(source_id)", "PASS"),
            ("staging_sale_id_uuid", "staging.sales_raw", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE NOT is_valid_uuid(source_id)", "PASS"),
            ("staging_sale_customer_ref_uuid", "staging.sales_raw", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE customer_id IS NOT NULL AND NOT is_valid_uuid(customer_id)", "PASS"),
            ("staging_sale_product_ref_uuid", "staging.sales_raw", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE product_id IS NOT NULL AND NOT is_valid_uuid(product_id)", "PASS"),

            # ─── UUID Uniqueness ───
            ("staging_customer_id_unique", "staging.customers_raw", "uniqueness",
             "SELECT CASE WHEN COUNT(source_id) = COUNT(DISTINCT source_id) THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw", "PASS"),
            ("staging_product_id_unique", "staging.products_raw", "uniqueness",
             "SELECT CASE WHEN COUNT(source_id) = COUNT(DISTINCT source_id) THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw", "PASS"),
            ("staging_sale_id_unique", "staging.sales_raw", "uniqueness",
             "SELECT CASE WHEN COUNT(source_id) = COUNT(DISTINCT source_id) THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw", "PASS"),

            # ─── Hash Integrity (ensures no corrupted loads) ───
            ("staging_customer_hash_populated", "staging.customers_raw", "completeness",
             "SELECT CASE WHEN COUNT(*) FILTER (WHERE row_hash IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0) >= 95 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw", "PASS"),
            ("staging_product_hash_populated", "staging.products_raw", "completeness",
             "SELECT CASE WHEN COUNT(*) FILTER (WHERE row_hash IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0) >= 95 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw", "PASS"),

            # ─── Price Sanity ───
            ("staging_product_price_positive", "staging.products_raw", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw WHERE unit_price <= 0", "PASS"),
            ("staging_sale_amount_positive", "staging.sales_raw", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE total_amount <= 0", "PASS"),
            ("staging_sale_quantity_positive", "staging.sales_raw", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.sales_raw WHERE quantity <= 0", "PASS"),

            # ─── Email Format ───
            ("staging_customer_email_format", "staging.customers_raw", "format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw WHERE email !~ '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$'", "PASS"),

            # ─── VARCHAR Length Compliance ───
            ("staging_customer_email_length", "staging.customers_raw", "schema_compliance",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.customers_raw WHERE LENGTH(email) > 255", "PASS"),
            ("staging_product_name_length", "staging.products_raw", "schema_compliance",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw WHERE LENGTH(product_name) > 255", "PASS"),

            # ─── Decimal Precision ───
            ("staging_product_price_precision", "staging.products_raw", "schema_compliance",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM staging.products_raw WHERE unit_price > 9999999999.99", "PASS"),

            # ─── Referential Integrity (staging cross-checks) ───
            ("staging_sale_customer_exists", "staging.sales_raw", "referential_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM staging.sales_raw s
                LEFT JOIN staging.customers_raw c ON s.customer_id = c.source_id
                WHERE c.source_id IS NULL AND s.customer_id IS NOT NULL""", "PASS"),
            ("staging_sale_product_exists", "staging.sales_raw", "referential_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM staging.sales_raw s
                LEFT JOIN staging.products_raw p ON s.product_id = p.source_id
                WHERE p.source_id IS NULL AND s.product_id IS NOT NULL""", "PASS"),
        ]

        for check_name, table, check_type, query, expected in checks:
            self.run_check(check_name, table, check_type, query, expected)

        return self.get_summary()

    def run_warehouse_quality_checks(self) -> dict:
        """
        Run comprehensive quality checks on warehouse dimension/fact tables.
        Final defense layer before data is available for analytics.
        """
        logger.info("Running Tier 2 warehouse quality checks...")

        checks = [
            # ─── Dimension Row Counts ───
            ("wh_customers_populated", "warehouse.dim_customers", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_customers WHERE is_current = TRUE", "PASS"),
            ("wh_products_populated", "warehouse.dim_products", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_products WHERE is_current = TRUE", "PASS"),
            ("wh_sales_populated", "warehouse.fact_sales", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales", "PASS"),
            ("wh_date_dim_populated", "warehouse.dim_date", "row_count",
             "SELECT CASE WHEN COUNT(*) >= 365 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_date", "PASS"),

            # ─── PK Not Null ───
            ("wh_customer_id_not_null", "warehouse.dim_customers", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_customers WHERE customer_id IS NULL", "PASS"),
            ("wh_product_id_not_null", "warehouse.dim_products", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_products WHERE product_id IS NULL", "PASS"),
            ("wh_sale_id_not_null", "warehouse.fact_sales", "null_check",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales WHERE sale_id IS NULL", "PASS"),

            # ─── UUID Format Compliance (warehouse) ───
            ("wh_customer_id_uuid", "warehouse.dim_customers", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_customers WHERE NOT is_valid_uuid(customer_id)", "PASS"),
            ("wh_product_id_uuid", "warehouse.dim_products", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_products WHERE NOT is_valid_uuid(product_id)", "PASS"),
            ("wh_sale_id_uuid", "warehouse.fact_sales", "uuid_format",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales WHERE NOT is_valid_uuid(sale_id)", "PASS"),

            # ─── Uniqueness ───
            ("wh_customer_key_unique", "warehouse.dim_customers", "uniqueness",
             "SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT customer_key) THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_customers", "PASS"),
            ("wh_sale_id_unique", "warehouse.fact_sales", "uniqueness",
             "SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT sale_id) THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales", "PASS"),

            # ─── SCD Type 2 Integrity ───
            ("wh_customer_scd2_one_current", "warehouse.dim_customers", "scd2_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM (SELECT customer_id, COUNT(*) as cnt
                      FROM warehouse.dim_customers WHERE is_current = TRUE
                      GROUP BY customer_id HAVING COUNT(*) > 1) dups""", "PASS"),
            ("wh_product_scd2_one_current", "warehouse.dim_products", "scd2_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM (SELECT product_id, COUNT(*) as cnt
                      FROM warehouse.dim_products WHERE is_current = TRUE
                      GROUP BY product_id HAVING COUNT(*) > 1) dups""", "PASS"),

            # ─── Referential Integrity (Fact ↔ Dimensions) ───
            ("wh_sales_customer_fk", "warehouse.fact_sales", "referential_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM warehouse.fact_sales f
                LEFT JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
                WHERE c.customer_key IS NULL AND f.customer_key IS NOT NULL""", "PASS"),
            ("wh_sales_product_fk", "warehouse.fact_sales", "referential_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM warehouse.fact_sales f
                LEFT JOIN warehouse.dim_products p ON f.product_key = p.product_key
                WHERE p.product_key IS NULL AND f.product_key IS NOT NULL""", "PASS"),
            ("wh_sales_date_fk", "warehouse.fact_sales", "referential_integrity",
             """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
                FROM warehouse.fact_sales f
                LEFT JOIN warehouse.dim_date d ON f.date_key = d.date_key
                WHERE d.date_key IS NULL AND f.date_key IS NOT NULL""", "PASS"),

            # ─── Business Logic ───
            ("wh_net_amount_not_negative", "warehouse.fact_sales", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales WHERE net_amount < 0", "PASS"),
            ("wh_discount_within_range", "warehouse.fact_sales", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales WHERE discount_percent < 0 OR discount_percent > 100", "PASS"),
            ("wh_product_price_range", "warehouse.dim_products", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_products WHERE unit_price < 0 OR unit_price > 100000", "PASS"),
            ("wh_profit_margin_sane", "warehouse.dim_products", "value_range",
             "SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_products WHERE is_current = TRUE AND profit_margin < -50", "PASS"),

            # ─── Data Freshness ───
            ("wh_customers_fresh", "warehouse.dim_customers", "freshness",
             "SELECT CASE WHEN MAX(created_at) >= NOW() - INTERVAL '48 hours' THEN 'PASS' ELSE 'FAIL' END FROM warehouse.dim_customers", "PASS"),
            ("wh_sales_fresh", "warehouse.fact_sales", "freshness",
             "SELECT CASE WHEN MAX(created_at) >= NOW() - INTERVAL '48 hours' THEN 'PASS' ELSE 'FAIL' END FROM warehouse.fact_sales", "PASS"),
        ]

        for check_name, table, check_type, query, expected in checks:
            self.run_check(check_name, table, check_type, query, expected)

        return self.get_summary()

    def log_ingestion_validation(self, report: dict) -> None:
        """
        Persist shift-left DQ validation results from the ingestion validator
        into the audit schema for tracking and trend analysis.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for entity_name, entity_report in report.get("entities", {}).items():
                summary = entity_report.get("summary", {})

                cursor.execute("""
                    INSERT INTO audit.dq_validation_runs
                    (report_id, entity, total_rows, clean_rows, quarantined_rows,
                     warnings_count, pass_rate, overall_status, batch_halted,
                     halt_reason, report_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING validation_id
                """, (
                    report.get("report_id", ""),
                    entity_name,
                    summary.get("total_rows", 0),
                    summary.get("clean_rows", 0),
                    summary.get("quarantined_rows", 0),
                    summary.get("warnings_count", 0),
                    summary.get("pass_rate", 0),
                    report.get("overall_status", "UNKNOWN"),
                    summary.get("batch_halted", False),
                    summary.get("halt_reason", ""),
                    json.dumps(entity_report, default=str),
                ))
                validation_id = cursor.fetchone()[0]

                # Log individual violations
                for violation in entity_report.get("violation_details", []):
                    cursor.execute("""
                        INSERT INTO audit.dq_violations
                        (validation_id, rule_name, rule_type, severity, entity,
                         column_name, violation_count, sample_value, expected, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        validation_id,
                        violation.get("rule", ""),
                        violation.get("type", ""),
                        violation.get("severity", ""),
                        entity_name,
                        None,
                        violation.get("count", 0),
                        violation.get("sample_value", ""),
                        "",
                        violation.get("description", ""),
                    ))

            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Ingestion validation results persisted to audit schema")

        except Exception as e:
            logger.error(f"Failed to persist DQ validation results: {e}")
