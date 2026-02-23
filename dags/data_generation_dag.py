"""
Data Generation DAG
===================
Scheduled DAG that generates fresh sample data and uploads to MinIO.
This simulates real-world data ingestion from external sources.

Schedule: Daily at midnight
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/plugins")

from slack_notifications import on_success_callback, on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    "owner": "cdp-team",
    "depends_on_past": False,
    "email": [os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", "admin@cdp.local")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    "on_success_callback": on_success_callback,
}


def generate_and_upload(**context):
    """Generate sample data and upload to MinIO raw-data bucket."""
    from generate_data import (
        generate_products, generate_customers, generate_sales,
        dict_list_to_csv
    )
    from minio_helper import MinIOClient

    num_customers = int(os.getenv("NUM_CUSTOMERS", "1000"))
    num_products = int(os.getenv("NUM_PRODUCTS", "50"))
    num_sales = int(os.getenv("NUM_SALES_RECORDS", "5000"))

    logger.info(f"Generating data: {num_customers} customers, {num_products} products, {num_sales} sales")

    products = generate_products(num_products)
    customers = generate_customers(num_customers)
    sales = generate_sales(num_sales, customers, products)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    products_csv = dict_list_to_csv(products, "products")
    customers_csv = dict_list_to_csv(customers, "customers")
    sales_csv = dict_list_to_csv(sales, "sales")

    minio_client = MinIOClient()
    minio_client.upload_file_content("raw-data", f"products/products_{timestamp}.csv", products_csv)
    minio_client.upload_file_content("raw-data", f"customers/customers_{timestamp}.csv", customers_csv)
    minio_client.upload_file_content("raw-data", f"sales/sales_{timestamp}.csv", sales_csv)

    summary = {
        "products": len(products),
        "customers": len(customers),
        "sales": len(sales),
        "timestamp": timestamp
    }
    logger.info(f"Data generation complete: {summary}")

    return summary


with DAG(
    dag_id="data_generation_pipeline",
    default_args=default_args,
    description="Generate sample sales data and upload to MinIO",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["cdp", "data-generation", "sample"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    generate = PythonOperator(
        task_id="generate_and_upload_data",
        python_callable=generate_and_upload,
    )

    start >> generate >> end
