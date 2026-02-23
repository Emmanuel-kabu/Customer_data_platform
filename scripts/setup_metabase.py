"""
Metabase Setup Script
=====================
Automatically configures Metabase after first boot:
- Connects to PostgreSQL data warehouse
- Creates starter dashboard questions
"""

import os
import time
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
METABASE_ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@cdp.local")
METABASE_ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "CDPAdmin2026!")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "customer_data_platform")
POSTGRES_USER = os.getenv("POSTGRES_USER", "cdp_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "cdp_secure_pass_2026")


def wait_for_metabase(max_retries: int = 60, delay: int = 10):
    """Wait for Metabase to be ready."""
    logger.info(f"Waiting for Metabase at {METABASE_URL}...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                logger.info("Metabase is ready!")
                return True
        except Exception:
            pass
        logger.info(f"Waiting... ({i + 1}/{max_retries})")
        time.sleep(delay)
    raise TimeoutError("Metabase did not become ready")


def setup_metabase():
    """Configure Metabase with initial setup."""
    wait_for_metabase()

    # Check if setup is needed
    try:
        resp = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
        props = resp.json()
        if props.get("setup-token"):
            setup_token = props["setup-token"]
            logger.info("Running initial Metabase setup...")

            setup_payload = {
                "token": setup_token,
                "prefs": {
                    "site_name": "Customer Data Platform",
                    "site_locale": "en",
                    "allow_tracking": False
                },
                "user": {
                    "first_name": "CDP",
                    "last_name": "Admin",
                    "email": METABASE_ADMIN_EMAIL,
                    "password": METABASE_ADMIN_PASSWORD,
                    "site_name": "Customer Data Platform"
                },
                "database": {
                    "engine": "postgres",
                    "name": "CDP Warehouse",
                    "details": {
                        "host": POSTGRES_HOST,
                        "port": POSTGRES_PORT,
                        "dbname": POSTGRES_DB,
                        "user": POSTGRES_USER,
                        "password": POSTGRES_PASSWORD,
                        "ssl": False,
                        "schema-filters-type": "inclusion",
                        "schema-filters-patterns": ["warehouse", "analytics"]
                    }
                }
            }

            resp = requests.post(
                f"{METABASE_URL}/api/setup",
                json=setup_payload,
                timeout=30
            )

            if resp.status_code == 200:
                logger.info("Metabase initial setup completed!")
                return resp.json().get("id")
            else:
                logger.error(f"Setup failed: {resp.status_code} - {resp.text}")
        else:
            logger.info("Metabase already configured. Logging in...")

            login_resp = requests.post(
                f"{METABASE_URL}/api/session",
                json={"username": METABASE_ADMIN_EMAIL, "password": METABASE_ADMIN_PASSWORD},
                timeout=10
            )

            if login_resp.status_code == 200:
                session_id = login_resp.json().get("id")
                logger.info(f"Logged in to Metabase (session: {session_id})")
                return session_id
            else:
                logger.warning(f"Login failed: {login_resp.status_code}")

    except Exception as e:
        logger.error(f"Metabase setup error: {e}")
        raise


def create_dashboard_questions(session_id: str):
    """Create sample dashboard questions in Metabase."""
    headers = {"X-Metabase-Session": session_id}

    questions = [
        {
            "name": "Monthly Sales Trend",
            "description": "Total sales revenue by month",
            "native_query": """
                SELECT month_name, year, net_sales, total_orders, unique_customers
                FROM analytics.monthly_trends
                ORDER BY year, month
            """
        },
        {
            "name": "Sales by Category",
            "description": "Revenue breakdown by product category",
            "native_query": """
                SELECT category, SUM(net_sales) as total_revenue,
                       SUM(total_units_sold) as units_sold,
                       SUM(total_orders) as orders
                FROM analytics.sales_by_category
                GROUP BY category
                ORDER BY total_revenue DESC
            """
        },
        {
            "name": "Top 10 Products",
            "description": "Best selling products by revenue",
            "native_query": """
                SELECT product_name, category, brand,
                       total_revenue, total_units_sold, times_ordered
                FROM analytics.top_products
                ORDER BY total_revenue DESC
                LIMIT 10
            """
        },
        {
            "name": "Customer Segments",
            "description": "Customer distribution by spending segment",
            "native_query": """
                SELECT customer_segment, COUNT(*) as customer_count,
                       AVG(total_spent) as avg_spent,
                       SUM(total_orders) as total_orders
                FROM analytics.customer_segments
                GROUP BY customer_segment
                ORDER BY avg_spent DESC
            """
        },
        {
            "name": "Daily Sales Summary",
            "description": "Sales performance by day",
            "native_query": """
                SELECT full_date, total_orders, net_sales,
                       avg_order_value, unique_customers
                FROM analytics.daily_sales_summary
                ORDER BY full_date DESC
                LIMIT 30
            """
        }
    ]

    # Get database ID
    try:
        db_resp = requests.get(f"{METABASE_URL}/api/database", headers=headers, timeout=10)
        databases = db_resp.json().get("data", [])
        db_id = None
        for db in databases:
            if db.get("name") == "CDP Warehouse":
                db_id = db["id"]
                break

        if not db_id:
            logger.warning("CDP Warehouse database not found in Metabase")
            return

        for q in questions:
            payload = {
                "name": q["name"],
                "description": q["description"],
                "dataset_query": {
                    "type": "native",
                    "native": {"query": q["native_query"].strip()},
                    "database": db_id
                },
                "display": "table",
                "visualization_settings": {}
            }

            resp = requests.post(
                f"{METABASE_URL}/api/card",
                json=payload,
                headers=headers,
                timeout=15
            )

            if resp.status_code == 200:
                logger.info(f"Created question: {q['name']}")
            else:
                logger.warning(f"Failed to create '{q['name']}': {resp.status_code}")

    except Exception as e:
        logger.error(f"Error creating questions: {e}")


if __name__ == "__main__":
    session = setup_metabase()
    if session:
        create_dashboard_questions(session)
    logger.info("Metabase setup script complete")
