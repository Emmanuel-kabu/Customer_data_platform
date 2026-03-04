"""
Database Helper - Utilities for PostgreSQL operations with incremental loading.
Implements hash-based change detection for efficient upserts.
"""

import os
import hashlib
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)


def get_db_connection(
    host: str = None,
    port: int = None,
    dbname: str = None,
    user: str = None,
    password: str = None
):
    """Create a PostgreSQL database connection."""
    conn = psycopg2.connect(
        host=host or os.getenv("POSTGRES_HOST", "postgres"),
        port=port or int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=dbname or os.getenv("POSTGRES_DB", "customer_data_platform"),
        user=user or os.getenv("POSTGRES_USER", "cdp_admin"),
        password=password or os.getenv("POSTGRES_PASSWORD", "cdp_secure_pass_2026")
    )
    conn.autocommit = False
    return conn


def get_conn_params() -> dict:
    """Get connection parameters as dictionary."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "customer_data_platform"),
        "user": os.getenv("POSTGRES_USER", "cdp_admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "cdp_secure_pass_2026")
    }


def compute_row_hash(row: dict, columns: List[str]) -> str:
    """
    Compute MD5 hash for incremental loading.
    Uses specified columns to detect changes between loads.

    Args:
        row: Dictionary containing row data
        columns: List of column names to include in hash

    Returns:
        MD5 hex digest string
    """
    hash_input = "|".join(str(row.get(col, "")) for col in sorted(columns))
    return hashlib.md5(hash_input.encode("utf-8")).hexdigest()


def bulk_insert_staging(conn, table_name: str, data: List[Dict],
                        source_file: str = "") -> int:
    """
    Bulk insert data into a staging table.

    Args:
        conn: Database connection
        table_name: Target staging table (schema.table)
        data: List of dictionaries containing row data
        source_file: Source file name for tracking

    Returns:
        Number of rows inserted
    """
    if not data:
        logger.warning(f"No data to insert into {table_name}")
        return 0

    cursor = conn.cursor()

    # Add source_file and loaded_at to each row
    for row in data:
        row["source_file"] = source_file
        row["loaded_at"] = datetime.now().isoformat()

    columns = list(data[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    values = [tuple(row.get(col) for col in columns) for row in data]
    cursor.executemany(insert_sql, values)
    conn.commit()

    inserted = len(values)
    logger.info(f"Inserted {inserted} rows into {table_name}")
    cursor.close()
    return inserted


def incremental_load_dimension(
    conn,
    staging_table: str,
    dim_table: str,
    business_key: str,
    hash_columns: List[str],
    attribute_columns: List[str],
    staging_key: str = None
) -> Tuple[int, int]:
    """
    Perform SCD Type 2 incremental load from staging to dimension table.
    Uses hash-based change detection.

    Args:
        conn: Database connection
        staging_table: Source staging table
        dim_table: Target dimension table
        business_key: Business key column name in the dimension table
        hash_columns: Columns used for hash computation
        attribute_columns: Columns to load
        staging_key: Business key column name in staging (defaults to business_key)

    Returns:
        Tuple of (records_inserted, records_updated)
    """
    stg_key = staging_key or business_key
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Step 1: Get existing current records with their hashes
    cursor.execute(f"""
        SELECT {business_key}, row_hash
        FROM {dim_table}
        WHERE is_current = TRUE
    """)
    existing = {row[business_key]: row["row_hash"] for row in cursor.fetchall()}

    # Step 2: Get staging records
    cols = ", ".join([stg_key] + attribute_columns + ["row_hash"])
    cursor.execute(f"SELECT {cols} FROM {staging_table}")
    staging_rows = cursor.fetchall()

    records_inserted = 0
    records_updated = 0

    for row in staging_rows:
        bk = row[stg_key]
        new_hash = row["row_hash"]

        if bk not in existing:
            # New record - insert
            col_list = [business_key] + attribute_columns + ["row_hash", "is_current", "effective_date"]
            val_list = [row[stg_key]] + [row.get(c) for c in attribute_columns] + \
                       [new_hash, True, datetime.now()]
            placeholders = ", ".join(["%s"] * len(col_list))
            cols_str = ", ".join(col_list)

            cursor.execute(
                f"INSERT INTO {dim_table} ({cols_str}) VALUES ({placeholders})",
                val_list
            )
            records_inserted += 1

        elif existing[bk] != new_hash:
            # Changed record - SCD Type 2: expire old, insert new
            cursor.execute(f"""
                UPDATE {dim_table}
                SET is_current = FALSE,
                    expiration_date = %s,
                    updated_at = %s
                WHERE {business_key} = %s AND is_current = TRUE
            """, (datetime.now(), datetime.now(), bk))

            col_list = [business_key] + attribute_columns + ["row_hash", "is_current", "effective_date"]
            val_list = [row[stg_key]] + [row.get(c) for c in attribute_columns] + \
                       [new_hash, True, datetime.now()]
            placeholders = ", ".join(["%s"] * len(col_list))
            cols_str = ", ".join(col_list)

            cursor.execute(
                f"INSERT INTO {dim_table} ({cols_str}) VALUES ({placeholders})",
                val_list
            )
            records_updated += 1

    conn.commit()
    cursor.close()

    logger.info(f"Incremental load to {dim_table}: {records_inserted} inserted, {records_updated} updated")
    return records_inserted, records_updated


def load_fact_table(conn, staging_table: str, fact_table: str,
                    dim_lookups: Dict[str, dict]) -> int:
    """
    Load fact table with dimension key lookups.

    Args:
        conn: Database connection
        staging_table: Source staging table
        fact_table: Target fact table
        dim_lookups: Dict mapping staging column to {dim_table, dim_bk, dim_key}

    Returns:
        Number of records inserted
    """
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute(f"SELECT * FROM {staging_table}")
    staging_rows = cursor.fetchall()

    records_inserted = 0

    for row in staging_rows:
        sale_id = row.get("source_id", row.get("sale_id", ""))

        # Check if already loaded (idempotency)
        cursor.execute(f"SELECT 1 FROM {fact_table} WHERE sale_id = %s", (sale_id,))
        if cursor.fetchone():
            continue

        # Look up dimension keys
        dim_keys = {}
        for staging_col, lookup in dim_lookups.items():
            cursor.execute(f"""
                SELECT {lookup['dim_key']}
                FROM {lookup['dim_table']}
                WHERE {lookup['dim_bk']} = %s AND is_current = TRUE
                LIMIT 1
            """, (row.get(staging_col),))
            result = cursor.fetchone()
            dim_keys[lookup['dim_key']] = result[lookup['dim_key']] if result else None

        # Calculate derived fields
        total_amount = float(row.get("total_amount", 0))
        discount_pct = float(row.get("discount_percent", 0))
        discount_amount = round(total_amount * discount_pct / 100, 2)
        net_amount = round(total_amount - discount_amount, 2)

        # Date key
        sale_date = row.get("sale_date")
        if sale_date:
            if isinstance(sale_date, str):
                sale_date = datetime.fromisoformat(sale_date)
            date_key = int(sale_date.strftime("%Y%m%d"))
        else:
            date_key = None

        cursor.execute(f"""
            INSERT INTO {fact_table}
            (sale_id, customer_key, product_key, date_key, quantity, unit_price,
             total_amount, discount_percent, discount_amount, net_amount,
             payment_method, store_location, row_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING
        """, (
            sale_id,
            dim_keys.get("customer_key"),
            dim_keys.get("product_key"),
            date_key,
            row.get("quantity"),
            row.get("unit_price"),
            total_amount,
            discount_pct,
            discount_amount,
            net_amount,
            row.get("payment_method"),
            row.get("store_location"),
            row.get("row_hash")
        ))
        if cursor.rowcount > 0:
            records_inserted += 1

    conn.commit()
    cursor.close()
    logger.info(f"Loaded {records_inserted} records into {fact_table}")
    return records_inserted


def log_pipeline_run(conn, pipeline_name: str, dag_id: str, status: str,
                     records_processed: int = 0, records_inserted: int = 0,
                     records_updated: int = 0, records_rejected: int = 0,
                     source_file: str = "", error_message: str = "",
                     execution_time: float = 0.0) -> int:
    """Log pipeline run details to audit table."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO audit.pipeline_runs
        (pipeline_name, dag_id, status, records_processed, records_inserted,
         records_updated, records_rejected, source_file, error_message,
         execution_time_seconds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING run_id
    """, (pipeline_name, dag_id, status, records_processed, records_inserted,
          records_updated, records_rejected, source_file, error_message,
          execution_time))
    run_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    logger.info(f"Pipeline run logged: {pipeline_name} - {status} (run_id={run_id})")
    return run_id
