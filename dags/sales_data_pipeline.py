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
import hashlib
import logging
import time
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import sys
sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/plugins")

from minio_helper import MinIOClient
from db_helper import (
    get_db_connection, get_conn_params, compute_row_hash,
    bulk_insert_staging, incremental_load_dimension,
    load_fact_table, log_pipeline_run
)
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

def extract_from_minio(**context):
    """Extract CSV files from MinIO raw-data bucket."""
    start_time = time.time()
    logger.info("Starting extraction from MinIO...")

    minio_client = MinIOClient()
    extracted_data = {"customers": [], "products": [], "sales": []}
    files_processed = []

    for data_type in ["customers", "products", "sales"]:
        csv_files = minio_client.list_csv_files("raw-data", prefix=f"{data_type}/")
        logger.info(f"Found {len(csv_files)} {data_type} files in MinIO")

        for csv_file in csv_files:
            content = minio_client.download_file_content("raw-data", csv_file)
            reader = csv.DictReader(StringIO(content))
            rows = list(reader)
            extracted_data[data_type].extend(rows)
            files_processed.append(csv_file)
            logger.info(f"Extracted {len(rows)} rows from {csv_file}")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="extracted_data", value=extracted_data)
    context["ti"].xcom_push(key="files_processed", value=files_processed)

    elapsed = round(time.time() - start_time, 2)
    logger.info(f"Extraction complete in {elapsed}s. Files: {len(files_processed)}")

    return {
        "customers": len(extracted_data["customers"]),
        "products": len(extracted_data["products"]),
        "sales": len(extracted_data["sales"]),
        "files": len(files_processed),
        "elapsed_seconds": elapsed
    }


def load_to_staging(**context):
    """Load extracted data into PostgreSQL staging tables."""
    start_time = time.time()
    logger.info("Loading data into staging tables...")

    extracted_data = context["ti"].xcom_pull(key="extracted_data", task_ids="extract_from_minio")
    files_processed = context["ti"].xcom_pull(key="files_processed", task_ids="extract_from_minio")

    if not extracted_data:
        logger.warning("No data extracted. Skipping staging load.")
        return {"status": "skipped", "reason": "no_data"}

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

        # Load customers
        if extracted_data.get("customers"):
            for row in extracted_data["customers"]:
                row["source_id"] = row.pop("customer_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.customers_raw",
                extracted_data["customers"],
                source_file=",".join([f for f in files_processed if "customers" in f])
            )
            total_inserted += count

        # Load products
        if extracted_data.get("products"):
            for row in extracted_data["products"]:
                row["source_id"] = row.pop("product_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.products_raw",
                extracted_data["products"],
                source_file=",".join([f for f in files_processed if "products" in f])
            )
            total_inserted += count

        # Load sales
        if extracted_data.get("sales"):
            for row in extracted_data["sales"]:
                row["source_id"] = row.pop("sale_id", row.get("source_id", ""))
            count = bulk_insert_staging(
                conn, "staging.sales_raw",
                extracted_data["sales"],
                source_file=",".join([f for f in files_processed if "sales" in f])
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
            business_key="source_id",
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
            business_key="source_id",
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


def run_quality_checks(**context):
    """Run comprehensive data quality checks on warehouse tables."""
    logger.info("Running data quality checks...")

    conn_params = get_conn_params()
    checker = DataQualityChecker(conn_params)

    # Customer checks
    checker.check_row_count("warehouse.dim_customers", min_rows=1)
    checker.check_null_values("warehouse.dim_customers", "customer_id")
    checker.check_unique_values("warehouse.dim_customers", "customer_key")

    # Product checks
    checker.check_row_count("warehouse.dim_products", min_rows=1)
    checker.check_null_values("warehouse.dim_products", "product_id")
    checker.check_value_range("warehouse.dim_products", "unit_price", 0, 100000)

    # Sales checks
    checker.check_row_count("warehouse.fact_sales", min_rows=1)
    checker.check_null_values("warehouse.fact_sales", "sale_id")
    checker.check_value_range("warehouse.fact_sales", "quantity", 0, 10000)
    checker.check_value_range("warehouse.fact_sales", "net_amount", -1000, 1000000)

    # Date dimension check
    checker.check_row_count("warehouse.dim_date", min_rows=365)

    summary = checker.get_summary()
    logger.info(f"Quality check summary: {summary}")

    context["ti"].xcom_push(key="quality_summary", value=summary)

    if summary["failed"] > 0:
        failed_names = [c["check_name"] for c in summary["failed_checks"]]
        send_slack_notification(
            message=f"Data quality checks: {summary['failed']} of {summary['total_checks']} FAILED.\nFailed: {', '.join(failed_names)}",
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
        trigger_rule="none_failed",
    )

    # Task dependencies
    start >> extract >> stage
    stage >> [transform_cust, transform_prod]
    [transform_cust, transform_prod] >> transform_sale
    transform_sale >> quality >> archive >> summary >> end
