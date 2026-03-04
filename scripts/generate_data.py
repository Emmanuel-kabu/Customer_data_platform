"""
Customer Data Platform - Sample Data Generator
Generates realistic sales, customer, and product data and uploads to MinIO.
"""

import os
import csv
import hashlib
import logging
import uuid
from datetime import datetime, timedelta
from io import StringIO

from faker import Faker
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DataGenerator")

fake = Faker()
Faker.seed(42)

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_pass_2026")
RAW_BUCKET = "raw-data"
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "1000"))
NUM_SALES_RECORDS = int(os.getenv("NUM_SALES_RECORDS", "5000"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "50"))

# Product catalog
CATEGORIES = {
    "Electronics": {
        "sub_categories": ["Smartphones", "Laptops", "Tablets", "Accessories", "Audio"],
        "brands": ["TechPro", "DigitalMax", "SmartWave", "ElectraPrime", "ByteForce"],
        "price_range": (49.99, 2499.99),
        "cost_multiplier": 0.55
    },
    "Clothing": {
        "sub_categories": ["Men's Wear", "Women's Wear", "Kids", "Sportswear", "Formal"],
        "brands": ["UrbanStyle", "ClassicFit", "TrendSet", "ActiveWear", "ElegantLine"],
        "price_range": (19.99, 299.99),
        "cost_multiplier": 0.40
    },
    "Home & Garden": {
        "sub_categories": ["Furniture", "Kitchen", "Decor", "Garden", "Lighting"],
        "brands": ["HomeComfort", "GardenPro", "CozyLiving", "ModernSpace", "NaturalHome"],
        "price_range": (14.99, 999.99),
        "cost_multiplier": 0.50
    },
    "Books & Media": {
        "sub_categories": ["Fiction", "Non-Fiction", "Educational", "Comics", "Digital"],
        "brands": ["PageTurner", "KnowledgeBase", "StoryWorld", "LearnPro", "MediaHub"],
        "price_range": (4.99, 79.99),
        "cost_multiplier": 0.35
    },
    "Health & Beauty": {
        "sub_categories": ["Skincare", "Haircare", "Supplements", "Fitness", "Personal Care"],
        "brands": ["GlowUp", "VitalLife", "PureCare", "FitWell", "NaturalBliss"],
        "price_range": (9.99, 199.99),
        "cost_multiplier": 0.45
    }
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash", "Crypto"]
STORE_LOCATIONS = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "San Jose, CA", "Austin, TX", "Online Store"
]


def compute_row_hash(row_dict: dict) -> str:
    """Compute MD5 hash of a row for incremental loading detection."""
    hash_input = "|".join(str(v) for v in sorted(row_dict.items()))
    return hashlib.md5(hash_input.encode()).hexdigest()


def generate_products(num_products: int) -> list:
    """Generate product catalog data with UUID primary keys."""
    logger.info(f"Generating {num_products} products...")
    products = []

    for category, config in CATEGORIES.items():
        products_per_category = num_products // len(CATEGORIES)
        for _ in range(products_per_category):
            sub_cat = fake.random_element(config["sub_categories"])
            brand = fake.random_element(config["brands"])
            unit_price = round(fake.pyfloat(
                min_value=config["price_range"][0],
                max_value=config["price_range"][1],
                right_digits=2
            ), 2)
            cost_price = round(unit_price * config["cost_multiplier"], 2)
            stock = fake.random_int(min=0, max=500)

            product = {
                "product_id": str(uuid.uuid4()),
                "product_name": f"{brand} {sub_cat} {fake.word().title()} {fake.random_int(100, 999)}",
                "category": category,
                "sub_category": sub_cat,
                "brand": brand,
                "unit_price": unit_price,
                "cost_price": cost_price,
                "supplier": fake.company(),
                "stock_quantity": stock
            }
            product["row_hash"] = compute_row_hash(product)
            products.append(product)

    logger.info(f"Generated {len(products)} products")
    return products


def generate_customers(num_customers: int) -> list:
    """Generate customer data with diverse profiles."""
    logger.info(f"Generating {num_customers} customers...")
    customers = []

    for i in range(1, num_customers + 1):
        reg_date = fake.date_time_between(start_date="-3y", end_date="now")
        customer = {
            "customer_id": str(uuid.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "zip_code": fake.zipcode(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "registration_date": reg_date.isoformat()
        }
        customer["row_hash"] = compute_row_hash(customer)
        customers.append(customer)

    logger.info(f"Generated {len(customers)} customers")
    return customers


def generate_sales(num_sales: int, customers: list, products: list) -> list:
    """Generate sales transaction data."""
    logger.info(f"Generating {num_sales} sales records...")
    sales = []

    for i in range(1, num_sales + 1):
        customer = fake.random_element(customers)
        product = fake.random_element(products)
        quantity = fake.random_int(min=1, max=10)
        unit_price = float(product["unit_price"])
        discount_pct = fake.random_element([0, 5, 10, 15, 20, 25, 30])
        total_amount = round(unit_price * quantity, 2)

        sale = {
            "sale_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_percent": discount_pct,
            "sale_date": fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
            "payment_method": fake.random_element(PAYMENT_METHODS),
            "store_location": fake.random_element(STORE_LOCATIONS)
        }
        sale["row_hash"] = compute_row_hash(sale)
        sales.append(sale)

    logger.info(f"Generated {len(sales)} sales records")
    return sales


def dict_list_to_csv(data: list, filename: str) -> str:
    """Convert list of dicts to CSV string."""
    if not data:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def upload_to_minio(client: Minio, bucket: str, object_name: str, data: str):
    """Upload CSV data string to MinIO bucket."""
    from io import BytesIO
    data_bytes = data.encode("utf-8")
    data_stream = BytesIO(data_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=data_stream,
        length=len(data_bytes),
        content_type="text/csv"
    )
    logger.info(f"Uploaded {object_name} to {bucket} ({len(data_bytes)} bytes)")


def save_locally(data: str, filepath: str):
    """Save CSV data to local filesystem as backup."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        f.write(data)
    logger.info(f"Saved locally: {filepath}")


def main():
    """Main data generation and upload workflow with shift-left DQ validation."""
    logger.info("=" * 60)
    logger.info("Customer Data Platform - Data Generator")
    logger.info("=" * 60)

    # Generate data
    products = generate_products(NUM_PRODUCTS)
    customers = generate_customers(NUM_CUSTOMERS)
    sales = generate_sales(NUM_SALES_RECORDS, customers, products)

    # ── Shift-Left Data Quality Gate ──────────────────────────
    logger.info("")
    logger.info("Running shift-left data quality validation...")
    try:
        from dq_ingestion_validator import IngestionValidator

        validator = IngestionValidator(
            minio_endpoint=MINIO_ENDPOINT,
            minio_access_key=MINIO_ACCESS_KEY,
            minio_secret_key=MINIO_SECRET_KEY,
        )

        report = validator.validate_and_upload(
            customers=customers,
            products=products,
            sales=sales,
        )

        # Save DQ report locally
        import json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = "/app/data/dq-reports"
        os.makedirs(report_path, exist_ok=True)
        with open(f"{report_path}/dq_report_{timestamp}.json", "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"DQ report saved locally: {report_path}/dq_report_{timestamp}.json")

        # Also save clean data locally as backup
        from dq_rules_engine import DataQualityRulesEngine
        for entity_name, entity_data in [("products", products), ("customers", customers), ("sales", sales)]:
            csv_data = dict_list_to_csv(entity_data, entity_name)
            save_locally(csv_data, f"/app/data/{entity_name}/{entity_name}_{timestamp}.csv")

        # Summary
        totals = report.get("totals", {})
        logger.info("=" * 60)
        logger.info("Data Generation + Quality Summary:")
        logger.info(f"  Products:        {len(products)} records")
        logger.info(f"  Customers:       {len(customers)} records")
        logger.info(f"  Sales:           {len(sales)} records")
        logger.info(f"  DQ Status:       {report.get('overall_status', 'UNKNOWN')}")
        logger.info(f"  Clean Rows:      {totals.get('total_clean', 'N/A')}")
        logger.info(f"  Quarantined:     {totals.get('total_quarantined', 'N/A')}")
        logger.info(f"  Pass Rate:       {totals.get('overall_pass_rate', 'N/A')}%")
        logger.info(f"  Timestamp:       {timestamp}")
        logger.info("=" * 60)

        if report.get("overall_status") == "FAIL":
            logger.error("Data quality gate FAILED — check quarantine bucket and DQ report")
            raise SystemExit(1)

    except ImportError:
        logger.warning("DQ validator not available — falling back to direct upload")
        _fallback_upload(customers, products, sales)

    except SystemExit:
        raise

    except Exception as e:
        logger.error(f"DQ validation error: {e} — falling back to direct upload")
        _fallback_upload(customers, products, sales)


def _fallback_upload(customers: list, products: list, sales: list):
    """Direct upload without DQ validation (legacy fallback)."""
    logger.info("Using legacy direct upload (no DQ validation)...")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    products_csv = dict_list_to_csv(products, "products")
    customers_csv = dict_list_to_csv(customers, "customers")
    sales_csv = dict_list_to_csv(sales, "sales")

    try:
        upload_to_minio(client, RAW_BUCKET, f"products/products_{timestamp}.csv", products_csv)
        upload_to_minio(client, RAW_BUCKET, f"customers/customers_{timestamp}.csv", customers_csv)
        upload_to_minio(client, RAW_BUCKET, f"sales/sales_{timestamp}.csv", sales_csv)
        logger.info("All files uploaded to MinIO successfully (no DQ)!")
    except S3Error as e:
        logger.error(f"MinIO upload failed: {e}")
        raise

    save_locally(products_csv, f"/app/data/products/products_{timestamp}.csv")
    save_locally(customers_csv, f"/app/data/customers/customers_{timestamp}.csv")
    save_locally(sales_csv, f"/app/data/sales/sales_{timestamp}.csv")

    logger.info("=" * 60)
    logger.info("Data Generation Summary (no DQ):")
    logger.info(f"  Products: {len(products)} records")
    logger.info(f"  Customers: {len(customers)} records")
    logger.info(f"  Sales: {len(sales)} records")
    logger.info(f"  Timestamp: {timestamp}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
