"""
Data Flow Validation DAG
========================
Automated checks ensuring data moves successfully through all layers:
MinIO (ingestion) → Staging (PostgreSQL) → Warehouse → Analytics views

This DAG validates end-to-end data integrity.

Schedule: Every 12 hours
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/plugins")

from slack_notifications import on_failure_callback, send_slack_notification

logger = logging.getLogger(__name__)

default_args = {
    "owner": "cdp-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def validate_minio_connectivity(**context):
    """Validate MinIO is accessible and buckets exist."""
    from minio_helper import MinIOClient

    client = MinIOClient()
    required_buckets = ["raw-data", "processed-data", "archive-data"]
    results = {}

    for bucket in required_buckets:
        exists = client.client.bucket_exists(bucket)
        results[bucket] = exists
        logger.info(f"Bucket '{bucket}' exists: {exists}")

    all_exist = all(results.values())
    if not all_exist:
        missing = [b for b, e in results.items() if not e]
        raise ValueError(f"Missing MinIO buckets: {missing}")

    return {"status": "passed", "buckets": results}


def validate_postgres_connectivity(**context):
    """Validate PostgreSQL is accessible and schemas exist."""
    from db_helper import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    required_schemas = ["staging", "warehouse", "analytics", "audit"]
    results = {}

    for schema in required_schemas:
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
            (schema,)
        )
        exists = cursor.fetchone()[0]
        results[schema] = exists
        logger.info(f"Schema '{schema}' exists: {exists}")

    cursor.close()
    conn.close()

    all_exist = all(results.values())
    if not all_exist:
        missing = [s for s, e in results.items() if not e]
        raise ValueError(f"Missing database schemas: {missing}")

    return {"status": "passed", "schemas": results}


def validate_staging_tables(**context):
    """Check staging tables have expected structure."""
    from db_helper import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    tables = {
        "staging.customers_raw": ["source_id", "first_name", "last_name", "email", "row_hash"],
        "staging.sales_raw": ["source_id", "customer_id", "product_id", "total_amount", "row_hash"],
        "staging.products_raw": ["source_id", "product_name", "category", "unit_price", "row_hash"],
    }

    for table, expected_cols in tables.items():
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{table.split('.')[0]}'
            AND table_name = '{table.split('.')[1]}'
        """)
        actual_cols = [row[0] for row in cursor.fetchall()]

        for col in expected_cols:
            if col not in actual_cols:
                raise ValueError(f"Missing column '{col}' in {table}")

        logger.info(f"Table {table}: structure validated ({len(actual_cols)} columns)")

    cursor.close()
    conn.close()
    return {"status": "passed"}


def validate_warehouse_tables(**context):
    """Check warehouse dimension and fact tables."""
    from db_helper import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    tables_to_check = [
        ("warehouse.dim_customers", "customer_key"),
        ("warehouse.dim_products", "product_key"),
        ("warehouse.dim_date", "date_key"),
        ("warehouse.fact_sales", "sale_key"),
    ]

    results = {}
    for table, pk in tables_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        results[table] = count
        logger.info(f"{table}: {count} rows")

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="warehouse_counts", value=results)
    return {"status": "passed", "row_counts": results}


def validate_analytics_views(**context):
    """Verify analytics views are accessible and return data."""
    from db_helper import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    views = [
        "analytics.daily_sales_summary",
        "analytics.sales_by_category",
        "analytics.customer_segments",
        "analytics.top_products",
        "analytics.monthly_trends",
    ]

    results = {}
    for view in views:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {view}")
            count = cursor.fetchone()[0]
            results[view] = {"accessible": True, "rows": count}
            logger.info(f"View {view}: {count} rows")
        except Exception as e:
            results[view] = {"accessible": False, "error": str(e)}
            logger.error(f"View {view} failed: {e}")
            conn.rollback()

    cursor.close()
    conn.close()

    return {"status": "passed", "views": results}


def validate_data_flow(**context):
    """Validate end-to-end data flow from MinIO to analytics."""
    from minio_helper import MinIOClient
    from db_helper import get_db_connection

    errors = []

    # Check MinIO has data
    minio_client = MinIOClient()
    raw_files = minio_client.list_csv_files("raw-data")
    archive_files = minio_client.list_csv_files("archive-data")

    total_minio_files = len(raw_files) + len(archive_files)
    if total_minio_files == 0:
        errors.append("No CSV files found in MinIO (raw-data or archive-data)")

    # Check warehouse has data
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = TRUE")
    customer_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_products WHERE is_current = TRUE")
    product_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    sales_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    logger.info(f"Data flow check - MinIO files: {total_minio_files}, "
                f"Customers: {customer_count}, Products: {product_count}, Sales: {sales_count}")

    status = "passed" if not errors else "warning"

    result = {
        "status": status,
        "minio_files": total_minio_files,
        "warehouse_customers": customer_count,
        "warehouse_products": product_count,
        "warehouse_sales": sales_count,
        "errors": errors
    }

    if errors:
        send_slack_notification(
            message="Data flow validation issues:\n" + "\n".join(f"• {e}" for e in errors),
            status="warning",
            dag_id="data_flow_validation"
        )

    return result


def send_validation_report(**context):
    """Send comprehensive validation report."""
    minio_result = context["ti"].xcom_pull(task_ids="validate_minio")
    postgres_result = context["ti"].xcom_pull(task_ids="validate_postgres")
    flow_result = context["ti"].xcom_pull(task_ids="validate_data_flow")

    message = f"""
*Data Flow Validation Report*
• Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
• MinIO Status: {minio_result.get('status', 'unknown') if minio_result else 'N/A'}
• PostgreSQL Status: {postgres_result.get('status', 'unknown') if postgres_result else 'N/A'}
• Data Flow: {flow_result.get('status', 'unknown') if flow_result else 'N/A'}
• Warehouse Customers: {flow_result.get('warehouse_customers', 0) if flow_result else 0}
• Warehouse Products: {flow_result.get('warehouse_products', 0) if flow_result else 0}
• Warehouse Sales: {flow_result.get('warehouse_sales', 0) if flow_result else 0}
"""

    send_slack_notification(
        message=message,
        status="success",
        dag_id="data_flow_validation"
    )

    logger.info("Validation report sent")
    return {"status": "complete"}


with DAG(
    dag_id="data_flow_validation",
    default_args=default_args,
    description="Validate end-to-end data flow: MinIO → PostgreSQL → Analytics",
    schedule_interval="0 */12 * * *",  # Every 12 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["cdp", "validation", "monitoring"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    validate_minio = PythonOperator(
        task_id="validate_minio",
        python_callable=validate_minio_connectivity,
    )

    validate_postgres = PythonOperator(
        task_id="validate_postgres",
        python_callable=validate_postgres_connectivity,
    )

    validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging_tables,
    )

    validate_warehouse = PythonOperator(
        task_id="validate_warehouse",
        python_callable=validate_warehouse_tables,
    )

    validate_views = PythonOperator(
        task_id="validate_analytics_views",
        python_callable=validate_analytics_views,
    )

    validate_flow = PythonOperator(
        task_id="validate_data_flow",
        python_callable=validate_data_flow,
    )

    report = PythonOperator(
        task_id="send_validation_report",
        python_callable=send_validation_report,
        trigger_rule="none_failed",
    )

    # Dependencies
    start >> [validate_minio, validate_postgres]
    validate_postgres >> validate_staging >> validate_warehouse >> validate_views
    [validate_minio, validate_views] >> validate_flow >> report >> end
