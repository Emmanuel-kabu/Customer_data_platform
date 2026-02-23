"""
Data Quality Checks - Validates data at each pipeline stage.
Implements comprehensive quality validation for the CDP.
"""

import logging
from datetime import datetime
from typing import Optional

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
