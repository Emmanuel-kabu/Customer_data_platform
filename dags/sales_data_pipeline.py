"""
Sales Data Pipeline DAG
=======================
Orchestrates the complete ETL flow:
1. Extract CSV files from MinIO (raw-data bucket)
2. Load into PostgreSQL staging tables
3. Transform and load into warehouse dimension/fact tables
4. Run data quality checks
5. Archive processed files
6. Send notifications

Schedule: Every 6 hours
Retries: 3 with exponential backoff
"""

import os
import csv
import logging
import re
import time
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/plugins")

from minio_helper import MinIOClient
from db_helper import (
    get_db_connection, get_conn_params,
    bulk_insert_staging, incremental_load_dimension,
    load_fact_table, log_pipeline_run
)
from schema_validator import SchemaValidator
from apply_analytics_models import apply_models
from data_quality import DataQualityChecker
from slack_notifications import (
    on_success_callback, on_failure_callback, on_retry_callback,
    send_slack_notification
)

logger = logging.getLogger(__name__)

# ============================================================
# DAG Default Arguments
# ============================================================
default_args = {
    "owner": "cdp-team",
    "depends_on_past": False,
    "email": [os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", "admin@cdp.local")],
    "email_on_failure": True,
    "email_on_retry": True,
    "email_on_success": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
    "on_retry_callback": on_retry_callback,
}


# ============================================================
# Task Functions
# ============================================================

def _extract_batch_timestamp(object_name: str, data_type: str) -> str:
    """
    Extract batch timestamp from expected object naming format:
    <data_type>/<data_type>_YYYYMMDD_HHMMSS.csv
    """
    pattern = rf"^{data_type}/{data_type}_(\d{{8}}_\d{{6}})\.csv$"
    match = re.match(pattern, object_name)
    return match.group(1) if match else ""


def extract_from_minio(**context):
    """
    Extract exactly one complete batch from MinIO raw-data bucket.

    A complete batch is a shared timestamp across:
    - customers/customers_<timestamp>.csv
    - products/products_<timestamp>.csv
    - sales/sales_<timestamp>.csv
    """
    start_time = time.time()
    logger.info("Starting extraction from MinIO...")

    minio_client = MinIOClient()
    extracted_data = {"customers": [], "products": [], "sales": []}
    files_processed = []

    files_by_type: Dict[str, Dict[str, str]] = {"customers": {}, "products": {}, "sales": {}}

    for data_type in files_by_type.keys():
        csv_files = sorted(minio_client.list_csv_files("raw-data", prefix=f"{data_type}/"))
        logger.info(f"Found {len(csv_files)} {data_type} files in MinIO")

        for csv_file in csv_files:
            batch_ts = _extract_batch_timestamp(csv_file, data_type)
            if not batch_ts:
                logger.warning(f"Skipping unexpected file naming format: {csv_file}")
                continue
            files_by_type[data_type][batch_ts] = csv_file

    common_timestamps = (
        set(files_by_type["customers"].keys())
        & set(files_by_type["products"].keys())
        & set(files_by_type["sales"].keys())
    )

    if not common_timestamps:
        logger.info("No complete batch found in raw-data. Skipping extraction.")
        raise AirflowSkipException("No complete batch found in raw-data bucket")

    # Process oldest complete batch first (FIFO).
    batch_timestamp = sorted(common_timestamps)[0]
    logger.info(f"Processing batch timestamp: {batch_timestamp}")

    for data_type in ["customers", "products", "sales"]:
        csv_file = files_by_type[data_type][batch_timestamp]
        content = minio_client.download_file_content("raw-data", csv_file)
        reader = csv.DictReader(StringIO(content))
        rows = list(reader)
        extracted_data[data_type].extend(rows)
        files_processed.append(csv_file)
        logger.info(f"Extracted {len(rows)} rows from {csv_file}")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="extracted_data", value=extracted_data)
    context["ti"].xcom_push(key="files_processed", value=files_processed)
    context["ti"].xcom_push(key="batch_timestamp", value=batch_timestamp)

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"Extraction complete in {elapsed}s. Files: {len(files_processed)}")

    return {
        "customers": len(extracted_data["customers"]),
        "products": len(extracted_data["products"]),
        "sales": len(extracted_data["sales"]),
        "files": len(files_processed),
        "elapsed_seconds": elapsed
    }


def validate_before_load(**context):
    """
    Pre-load schema & data validation gate.

    Runs BEFORE any data touches PostgreSQL. Validates:
    - Schema structure (column presence, drift detection)
    - Data types (UUID v4, decimals, dates, timestamps)
    - Constraints (NOT NULL, UNIQUE, ENUM, REGEX, RANGE)
    - Referential integrity (FK: sales → customers, products)
    - Cross-column business rules
    - Batch-level rejection thresholds
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("PRE-LOAD SCHEMA & DATA VALIDATION")
    logger.info("=" * 60)

    extracted_data = context["ti"].xcom_pull(
        key="extracted_data", task_ids="extract_from_minio"
    )

    total_rows = (
        len(extracted_data.get("customers", []))
        + len(extracted_data.get("products", []))
        + len(extracted_data.get("sales", []))
    )

    if not extracted_data or total_rows == 0:
        logger.warning("No extracted data — skipping pre-load validation")
        return {"status": "skipped", "reason": "no_data"}

    # Initialize schema validator
    validator = SchemaValidator()

    customers = extracted_data.get("customers", [])
    products = extracted_data.get("products", [])
    sales = extracted_data.get("sales", [])

    logger.info(
        f"Validating: {len(customers)} customers, "
        f"{len(products)} products, {len(sales)} sales"
    )

    # Run full validation in dependency order
    report = validator.validate_all(
        customers=customers,
        products=products,
        sales=sales,
    )

    # Extract only valid rows for downstream staging load
    validated_data = report.pop("_validated_data", {})
    rejected_data = report.pop("_rejected_data", {})

    # Push validated (clean) data for staging load
    context["ti"].xcom_push(key="validated_data", value=validated_data)
    context["ti"].xcom_push(key="rejected_data", value=rejected_data)
    context["ti"].xcom_push(key="schema_validation_report", value=report)

    # Log rejected rows to PostgreSQL audit
    total_rejected = sum(len(rows) for rows in rejected_data.values())
    if total_rejected > 0:
        try:
            conn = get_db_connection()
            _persist_pre_load_rejections(conn, report, rejected_data)
            conn.close()
        except Exception as e:
            logger.warning(f"Could not persist rejections to audit: {e}")

    # Persist validation report to MinIO
    try:
        minio_client = MinIOClient()
        import json
        report_json = json.dumps(report, indent=2, default=str)
        from io import BytesIO
        report_bytes = report_json.encode("utf-8")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        minio_client.client.put_object(
            bucket_name="processed-data",
            object_name=f"schema-validation/{timestamp}_report.json",
            data=BytesIO(report_bytes),
            length=len(report_bytes),
            content_type="application/json",
        )
        logger.info("Schema validation report saved to MinIO")
    except Exception as e:
        logger.warning(f"Could not save validation report to MinIO: {e}")

    elapsed = round(time.time() - start_time, 2)

    totals = report.get("totals", {})
    logger.info("─" * 50)
    logger.info(f"Pre-Load Validation Complete ({elapsed}s):")
    logger.info(f"  Status:      {report.get('overall_status', 'UNKNOWN')}")
    logger.info(f"  Total Rows:  {totals.get('total_rows', 0)}")
    logger.info(f"  Valid:       {totals.get('total_valid', 0)}")
    logger.info(f"  Rejected:    {totals.get('total_rejected', 0)}")
    logger.info(f"  Pass Rate:   {totals.get('overall_pass_rate', 0)}%")
    logger.info(f"  Coercions:   {totals.get('total_coercions', 0)}")
    logger.info(f"  Violations:  {totals.get('total_violations', 0)}")
    logger.info("─" * 50)

    # HALT pipeline if critical schema mismatch
    if report.get("overall_status") == "HALT":
        halt_reasons = []
        for entity, entity_report in report.get("entities", {}).items():
            summary = entity_report.get("summary", {})
            if summary.get("batch_halted"):
                halt_reasons.append(f"{entity}: {summary.get('halt_reason', 'unknown')}")

        error_msg = (
            f"Pre-load validation HALTED pipeline. "
            f"Reasons: {'; '.join(halt_reasons)}"
        )
        logger.error(error_msg)

        from slack_notifications import send_slack_notification
        send_slack_notification(
            message=f"*PRE-LOAD VALIDATION HALT*\n{error_msg}",
            status="error",
            dag_id="sales_data_pipeline",
        )
        raise ValueError(error_msg)

    return {
        "status": report.get("overall_status"),
        "elapsed_seconds": elapsed,
        **totals,
    }


def _persist_pre_load_rejections(conn, report: dict, rejected_data: dict):
    """Persist pre-load validation rejections to audit tables."""
    cursor = conn.cursor()

    for entity, rows in rejected_data.items():
        if not rows:
            continue

        import json
        for row in rows:
            reasons = row.pop("_rejection_reasons", [])
            rules = row.pop("_violated_rules", [])

            cursor.execute("""
                INSERT INTO audit.pre_load_rejections
                (entity, source_data, rejection_reasons, violated_rules,
                 validation_report_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                entity,
                json.dumps(row, default=str),
                reasons,
                rules,
                report.get("report_id", ""),
            ))

    conn.commit()
    cursor.close()
    logger.info(f"Persisted {sum(len(r) for r in rejected_data.values())} rejections to audit")


def load_to_staging(**context):
    """Load extracted data into PostgreSQL staging tables."""
    start_time = time.time()
    logger.info("Loading data into staging tables...")

    extracted_data = context["ti"].xcom_pull(key="extracted_data", task_ids="extract_from_minio")
    files_processed = context["ti"].xcom_pull(key="files_processed", task_ids="extract_from_minio")

    # Use validated data from pre-load validation (if available)
    validated_data = context["ti"].xcom_pull(
        key="validated_data", task_ids="validate_before_load"
    )

    def _has_rows(payload: dict) -> bool:
        return bool(payload) and any(payload.get(k) for k in ["customers", "products", "sales"])

    # If pre-load validation ran, use only validated (clean) rows
    if _has_rows(validated_data):
        logger.info("Using pre-load validated data (clean rows only)")
        load_data = validated_data
    elif _has_rows(extracted_data):
        logger.warning("No validated data — loading raw extracted data")
        load_data = extracted_data
    else:
        load_data = None

    conn = get_db_connection()
    cursor = conn.cursor()
    total_inserted = 0

    try:
        # Clear staging tables before fresh load
        cursor.execute("TRUNCATE TABLE staging.customers_raw")
        cursor.execute("TRUNCATE TABLE staging.sales_raw")
        cursor.execute("TRUNCATE TABLE staging.products_raw")
        conn.commit()
        logger.info("Staging tables truncated")

        if not load_data:
            logger.warning("No data to load after truncating staging. Skipping downstream tasks.")
            raise AirflowSkipException("No rows available for staging load")

        # Load customers
        if load_data.get("customers"):
            for row in load_data["customers"]:
                row["source_id"] = row.pop("customer_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.customers_raw",
                load_data["customers"],
                source_file=",".join([f for f in (files_processed or []) if "customers" in f])
            )
            total_inserted += count

        # Load products
        if load_data.get("products"):
            for row in load_data["products"]:
                row["source_id"] = row.pop("product_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.products_raw",
                load_data["products"],
                source_file=",".join([f for f in (files_processed or []) if "products" in f])
            )
            total_inserted += count

        # Load sales
        if load_data.get("sales"):
            for row in load_data["sales"]:
                row["source_id"] = row.pop("sale_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.sales_raw",
                load_data["sales"],
                source_file=",".join([f for f in (files_processed or []) if "sales" in f])
            )
            total_inserted += count

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Staging load complete: {total_inserted} total rows in {elapsed}s")

        context["ti"].xcom_push(key="staging_stats", value={
            "total_inserted": total_inserted,
            "elapsed_seconds": elapsed
        })

        return {"total_inserted": total_inserted, "elapsed_seconds": elapsed}

    except Exception as e:
        conn.rollback()
        logger.error(f"Staging load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def transform_customers(**context):
    """Transform and load customers into warehouse dimension (SCD Type 2)."""
    start_time = time.time()
    logger.info("Transforming customers with SCD Type 2...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Add computed columns to staging
        cursor.execute("""
            UPDATE staging.customers_raw
            SET row_hash = md5(
                COALESCE(source_id, '') || '|' ||
                COALESCE(first_name, '') || '|' ||
                COALESCE(last_name, '') || '|' ||
                COALESCE(email, '') || '|' ||
                COALESCE(phone, '') || '|' ||
                COALESCE(city, '') || '|' ||
                COALESCE(state, '') || '|' ||
                COALESCE(country, '')
            )
            WHERE row_hash IS NULL
        """)
        conn.commit()

        # Perform SCD Type 2 merge
        inserted, updated = incremental_load_dimension(
            conn=conn,
            staging_table="staging.customers_raw",
            dim_table="warehouse.dim_customers",
            business_key="customer_id",
            staging_key="source_id",
            hash_columns=["first_name", "last_name", "email", "phone", "city", "state"],
            attribute_columns=[
                "first_name", "last_name", "email", "phone",
                "address", "city", "state", "country", "zip_code",
                "date_of_birth", "registration_date"
            ]
        )

        # Update computed columns
        cursor.execute("""
            UPDATE warehouse.dim_customers
            SET full_name = TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')),
                age_group = CASE
                    WHEN date_of_birth IS NULL THEN 'Unknown'
                    WHEN AGE(date_of_birth) < INTERVAL '25 years' THEN '18-24'
                    WHEN AGE(date_of_birth) < INTERVAL '35 years' THEN '25-34'
                    WHEN AGE(date_of_birth) < INTERVAL '45 years' THEN '35-44'
                    WHEN AGE(date_of_birth) < INTERVAL '55 years' THEN '45-54'
                    WHEN AGE(date_of_birth) < INTERVAL '65 years' THEN '55-64'
                    ELSE '65+'
                END
            WHERE is_current = TRUE AND (full_name IS NULL OR full_name = '')
        """)
        conn.commit()

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Customer transformation complete: {inserted} inserted, {updated} updated in {elapsed}s")

        # Log pipeline run
        log_pipeline_run(
            conn, "transform_customers", "sales_data_pipeline", "success",
            records_inserted=inserted, records_updated=updated,
            execution_time=elapsed
        )

        return {"inserted": inserted, "updated": updated, "elapsed_seconds": elapsed}

    except Exception as e:
        conn.rollback()
        logger.error(f"Customer transformation failed: {e}")
        log_pipeline_run(
            conn, "transform_customers", "sales_data_pipeline", "failed",
            error_message=str(e)
        )
        raise
    finally:
        cursor.close()
        conn.close()


def transform_products(**context):
    """Transform and load products into warehouse dimension (SCD Type 2)."""
    start_time = time.time()
    logger.info("Transforming products with SCD Type 2...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Add computed columns
        cursor.execute("""
            UPDATE staging.products_raw
            SET row_hash = md5(
                COALESCE(source_id, '') || '|' ||
                COALESCE(product_name, '') || '|' ||
                COALESCE(category, '') || '|' ||
                COALESCE(brand, '') || '|' ||
                COALESCE(CAST(unit_price AS TEXT), '') || '|' ||
                COALESCE(CAST(cost_price AS TEXT), '')
            )
            WHERE row_hash IS NULL
        """)
        conn.commit()

        inserted, updated = incremental_load_dimension(
            conn=conn,
            staging_table="staging.products_raw",
            dim_table="warehouse.dim_products",
            business_key="product_id",
            staging_key="source_id",
            hash_columns=["product_name", "category", "brand", "unit_price", "cost_price"],
            attribute_columns=[
                "product_name", "category", "sub_category", "brand",
                "unit_price", "cost_price", "supplier", "stock_quantity"
            ]
        )

        # Update computed columns
        cursor.execute("""
            UPDATE warehouse.dim_products
            SET profit_margin = CASE
                    WHEN unit_price > 0 THEN ROUND(((unit_price - cost_price) / unit_price * 100)::NUMERIC, 2)
                    ELSE 0
                END,
                stock_status = CASE
                    WHEN stock_quantity <= 0 THEN 'Out of Stock'
                    WHEN stock_quantity <= 10 THEN 'Low Stock'
                    WHEN stock_quantity <= 50 THEN 'Medium Stock'
                    ELSE 'In Stock'
                END
            WHERE is_current = TRUE
        """)
        conn.commit()

        elapsed = round(time.time() - start_time, 2)
        log_pipeline_run(
            conn, "transform_products", "sales_data_pipeline", "success",
            records_inserted=inserted, records_updated=updated,
            execution_time=elapsed
        )

        return {"inserted": inserted, "updated": updated, "elapsed_seconds": elapsed}

    except Exception as e:
        conn.rollback()
        logger.error(f"Product transformation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def transform_sales(**context):
    """Transform and load sales into warehouse fact table."""
    start_time = time.time()
    logger.info("Transforming sales data...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Update hash for staging sales
        cursor.execute("""
            UPDATE staging.sales_raw
            SET row_hash = md5(
                COALESCE(source_id, '') || '|' ||
                COALESCE(customer_id, '') || '|' ||
                COALESCE(product_id, '') || '|' ||
                COALESCE(CAST(quantity AS TEXT), '') || '|' ||
                COALESCE(CAST(total_amount AS TEXT), '') || '|' ||
                COALESCE(CAST(sale_date AS TEXT), '')
            )
            WHERE row_hash IS NULL
        """)
        conn.commit()

        # Load fact table with dimension lookups
        records = load_fact_table(
            conn=conn,
            staging_table="staging.sales_raw",
            fact_table="warehouse.fact_sales",
            dim_lookups={
                "customer_id": {
                    "dim_table": "warehouse.dim_customers",
                    "dim_bk": "customer_id",
                    "dim_key": "customer_key"
                },
                "product_id": {
                    "dim_table": "warehouse.dim_products",
                    "dim_bk": "product_id",
                    "dim_key": "product_key"
                }
            }
        )

        elapsed = round(time.time() - start_time, 2)
        log_pipeline_run(
            conn, "transform_sales", "sales_data_pipeline", "success",
            records_inserted=records, execution_time=elapsed
        )

        return {"records_inserted": records, "elapsed_seconds": elapsed}

    except Exception as e:
        conn.rollback()
        logger.error(f"Sales transformation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def ensure_analytics_models(**context):
    """Apply analytics SQL models so dashboards stay in sync with warehouse changes."""
    logger.info("Applying analytics models...")
    result = apply_models()
    logger.info(f"Analytics models applied from {result.get('sql_file')}")
    return result


def run_quality_checks(**context):
    """
    Run comprehensive two-tier data quality checks.

    Tier 1: Verify shift-left ingestion DQ report (from MinIO)
    Tier 2: Post-staging and warehouse SQL-based checks
    """
    logger.info("Running comprehensive data quality checks...")

    conn_params = get_conn_params()
    checker = DataQualityChecker(conn_params)

    # ── Tier 1: Check DQ reports from ingestion validator ──
    try:
        minio_client = MinIOClient()
        dq_reports = minio_client.list_objects("processed-data", prefix="dq-reports/")
        dq_report_files = [obj for obj in dq_reports if obj.endswith(".json")]

        if dq_report_files:
            latest_report_file = sorted(dq_report_files)[-1]
            report_content = minio_client.download_file_content("processed-data", latest_report_file)
            import json
            ingestion_report = json.loads(report_content)

            logger.info(f"Ingestion DQ Report: {ingestion_report.get('overall_status', 'N/A')}")
            logger.info(f"  Pass Rate: {ingestion_report.get('totals', {}).get('overall_pass_rate', 'N/A')}%")

            # Persist to audit tables
            checker.log_ingestion_validation(ingestion_report)

            context["ti"].xcom_push(key="ingestion_dq_report", value=ingestion_report)
        else:
            logger.warning("No ingestion DQ reports found — shift-left may not be active")
    except Exception as e:
        logger.warning(f"Could not load ingestion DQ report: {e}")

    # ── Tier 2: Post-staging checks ──
    staging_summary = checker.run_staging_quality_checks()
    logger.info(f"Staging checks: {staging_summary['passed']}/{staging_summary['total_checks']} passed")

    # ── Tier 2: Warehouse checks ──
    # Reset results for warehouse-specific summary
    warehouse_checker = DataQualityChecker(conn_params)
    warehouse_summary = warehouse_checker.run_warehouse_quality_checks()
    logger.info(f"Warehouse checks: {warehouse_summary['passed']}/{warehouse_summary['total_checks']} passed")

    # Combined summary
    combined_total = staging_summary["total_checks"] + warehouse_summary["total_checks"]
    combined_passed = staging_summary["passed"] + warehouse_summary["passed"]
    combined_failed = staging_summary["failed"] + warehouse_summary["failed"]
    all_failed = staging_summary["failed_checks"] + warehouse_summary["failed_checks"]

    summary = {
        "total_checks": combined_total,
        "passed": combined_passed,
        "failed": combined_failed,
        "pass_rate": round(combined_passed / combined_total * 100, 2) if combined_total > 0 else 0,
        "failed_checks": all_failed,
        "staging_summary": staging_summary,
        "warehouse_summary": warehouse_summary,
    }

    logger.info(f"Combined quality summary: {summary['passed']}/{summary['total_checks']} passed ({summary['pass_rate']}%)")

    context["ti"].xcom_push(key="quality_summary", value=summary)

    if combined_failed > 0:
        failed_names = [c["check_name"] for c in all_failed]
        send_slack_notification(
            message=f"Data quality checks: {combined_failed} of {combined_total} FAILED.\nFailed: {', '.join(failed_names)}",
            status="warning",
            dag_id="sales_data_pipeline"
        )

    return summary


def archive_processed_files(**context):
    """Move processed files from raw-data to archive-data bucket."""
    logger.info("Archiving processed files...")

    files_processed = context["ti"].xcom_pull(key="files_processed", task_ids="extract_from_minio")

    if not files_processed:
        logger.info("No files to archive")
        return {"archived": 0}

    minio_client = MinIOClient()
    archived = 0

    for file_path in files_processed:
        try:
            minio_client.move_to_archive("raw-data", file_path)
            archived += 1
        except Exception as e:
            logger.error(f"Failed to archive {file_path}: {e}")

    logger.info(f"Archived {archived}/{len(files_processed)} files")
    return {"archived": archived, "total": len(files_processed)}


def send_pipeline_summary(**context):
    """Send final pipeline execution summary."""
    extract_result = context["ti"].xcom_pull(task_ids="extract_from_minio")
    quality_summary = context["ti"].xcom_pull(key="quality_summary", task_ids="run_quality_checks")

    message = f"""
*Sales Data Pipeline - Execution Summary*
• Execution Date: {context['execution_date']}
• Files Processed: {extract_result.get('files', 0) if extract_result else 0}
• Quality Checks: {quality_summary.get('passed', 0)}/{quality_summary.get('total_checks', 0)} passed
• Status: {'All checks passed ✓' if quality_summary and quality_summary.get('failed', 0) == 0 else 'Some checks failed ✗'}
"""

    send_slack_notification(
        message=message,
        status="success" if quality_summary and quality_summary.get("failed", 0) == 0 else "warning",
        dag_id="sales_data_pipeline",
        execution_date=str(context["execution_date"])
    )

    logger.info("Pipeline summary sent")
    return {"status": "complete"}


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="sales_data_pipeline",
    default_args=default_args,
    description="ETL pipeline: MinIO → Staging → Warehouse with SCD Type 2 and quality checks",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["cdp", "sales", "etl", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    extract = PythonOperator(
        task_id="extract_from_minio",
        python_callable=extract_from_minio,
    )

    pre_load_validate = PythonOperator(
        task_id="validate_before_load",
        python_callable=validate_before_load,
    )

    stage = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    transform_cust = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers,
    )

    transform_prod = PythonOperator(
        task_id="transform_products",
        python_callable=transform_products,
    )

    transform_sale = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales,
    )

    analytics_models = PythonOperator(
        task_id="ensure_analytics_models",
        python_callable=ensure_analytics_models,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    archive = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
    )

    summary = PythonOperator(
        task_id="send_pipeline_summary",
        python_callable=send_pipeline_summary,
        trigger_rule="none_failed_min_one_success",
    )

    # Task dependencies
    # Extract → Pre-Load Validation → Staging → Transform → Quality → Archive
    start >> extract >> pre_load_validate >> stage
    stage >> [transform_cust, transform_prod]
    [transform_cust, transform_prod] >> transform_sale
    transform_sale >> analytics_models >> quality >> archive >> summary >> end
