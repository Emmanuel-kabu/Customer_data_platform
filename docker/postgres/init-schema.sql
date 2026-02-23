-- ============================================================
-- Customer Data Platform - Database Schema
-- Implements: SCD Type 2 for time travel, incremental loading
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- Schema: staging (raw data landing zone)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging table for raw customer data
CREATE TABLE IF NOT EXISTS staging.customers_raw (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    zip_code VARCHAR(20),
    date_of_birth DATE,
    registration_date TIMESTAMP,
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash VARCHAR(64)  -- MD5 hash for incremental loading
);

-- Staging table for raw sales data
CREATE TABLE IF NOT EXISTS staging.sales_raw (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100),
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(12, 2),
    total_amount DECIMAL(12, 2),
    discount_percent DECIMAL(5, 2),
    sale_date TIMESTAMP,
    payment_method VARCHAR(50),
    store_location VARCHAR(200),
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash VARCHAR(64)
);

-- Staging table for raw product data
CREATE TABLE IF NOT EXISTS staging.products_raw (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100),
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(12, 2),
    cost_price DECIMAL(12, 2),
    supplier VARCHAR(200),
    stock_quantity INTEGER,
    source_file VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash VARCHAR(64)
);

-- ============================================================
-- Schema: warehouse (processed & curated data)
-- Implements SCD Type 2 for historical tracking
-- ============================================================
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension: Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    zip_code VARCHAR(20),
    date_of_birth DATE,
    age_group VARCHAR(20),
    registration_date TIMESTAMP,
    row_hash VARCHAR(64),
    -- SCD Type 2 columns
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_date TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_id ON warehouse.dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_current ON warehouse.dim_customers(is_current);
CREATE INDEX IF NOT EXISTS idx_dim_customers_hash ON warehouse.dim_customers(row_hash);

-- Dimension: Products (SCD Type 2)
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(12, 2),
    cost_price DECIMAL(12, 2),
    profit_margin DECIMAL(5, 2),
    supplier VARCHAR(200),
    stock_quantity INTEGER,
    stock_status VARCHAR(20),
    row_hash VARCHAR(64),
    -- SCD Type 2 columns
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_date TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_products_id ON warehouse.dim_products(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_current ON warehouse.dim_products(is_current);
CREATE INDEX IF NOT EXISTS idx_dim_products_hash ON warehouse.dim_products(row_hash);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sale_key SERIAL PRIMARY KEY,
    sale_id VARCHAR(100) NOT NULL,
    customer_key INTEGER REFERENCES warehouse.dim_customers(customer_key),
    product_key INTEGER REFERENCES warehouse.dim_products(product_key),
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    quantity INTEGER,
    unit_price DECIMAL(12, 2),
    total_amount DECIMAL(12, 2),
    discount_percent DECIMAL(5, 2),
    discount_amount DECIMAL(12, 2),
    net_amount DECIMAL(12, 2),
    payment_method VARCHAR(50),
    store_location VARCHAR(200),
    row_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sale_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON warehouse.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON warehouse.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_hash ON warehouse.fact_sales(row_hash);

-- ============================================================
-- Schema: analytics (aggregated views for dashboards)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- Sales summary by date
CREATE OR REPLACE VIEW analytics.daily_sales_summary AS
SELECT
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS gross_sales,
    SUM(f.discount_amount) AS total_discounts,
    SUM(f.net_amount) AS net_sales,
    AVG(f.net_amount) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.quarter, d.month, d.month_name, d.day_name, d.is_weekend;

-- Sales by product category
CREATE OR REPLACE VIEW analytics.sales_by_category AS
SELECT
    p.category,
    p.sub_category,
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS net_sales,
    AVG(f.net_amount) AS avg_order_value,
    SUM(f.quantity * p.cost_price) AS total_cost,
    SUM(f.net_amount) - SUM(f.quantity * p.cost_price) AS gross_profit
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p ON f.product_key = p.product_key AND p.is_current = TRUE
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY p.category, p.sub_category, d.year, d.month, d.month_name;

-- Customer segmentation
CREATE OR REPLACE VIEW analytics.customer_segments AS
SELECT
    c.customer_id,
    c.full_name,
    c.city,
    c.state,
    c.country,
    c.age_group,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    SUM(f.net_amount) AS total_spent,
    AVG(f.net_amount) AS avg_order_value,
    MIN(d.full_date) AS first_purchase_date,
    MAX(d.full_date) AS last_purchase_date,
    CASE
        WHEN SUM(f.net_amount) >= 5000 THEN 'VIP'
        WHEN SUM(f.net_amount) >= 2000 THEN 'Premium'
        WHEN SUM(f.net_amount) >= 500 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment
FROM warehouse.dim_customers c
JOIN warehouse.fact_sales f ON c.customer_key = f.customer_key
JOIN warehouse.dim_date d ON f.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.full_name, c.city, c.state, c.country, c.age_group;

-- Top products view
CREATE OR REPLACE VIEW analytics.top_products AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT f.sale_id) AS times_ordered,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS total_revenue,
    AVG(f.unit_price) AS avg_selling_price,
    p.cost_price,
    p.profit_margin,
    p.stock_quantity,
    p.stock_status
FROM warehouse.dim_products p
JOIN warehouse.fact_sales f ON p.product_key = f.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_id, p.product_name, p.category, p.brand,
         p.cost_price, p.profit_margin, p.stock_quantity, p.stock_status;

-- Monthly trends
CREATE OR REPLACE VIEW analytics.monthly_trends AS
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.net_amount) AS net_sales,
    SUM(f.quantity) AS units_sold,
    AVG(f.net_amount) AS avg_order_value,
    LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month) AS prev_month_sales,
    CASE
        WHEN LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month) > 0
        THEN ROUND(((SUM(f.net_amount) - LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month))
              / LAG(SUM(f.net_amount)) OVER (ORDER BY d.year, d.month) * 100), 2)
        ELSE NULL
    END AS mom_growth_pct
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name;

-- ============================================================
-- Schema: audit (pipeline tracking)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(200) NOT NULL,
    dag_id VARCHAR(200),
    run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    source_file VARCHAR(500),
    error_message TEXT,
    execution_time_seconds DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(200) NOT NULL,
    table_name VARCHAR(200),
    check_type VARCHAR(50),
    check_query TEXT,
    expected_result VARCHAR(200),
    actual_result VARCHAR(200),
    is_passed BOOLEAN,
    run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id INTEGER REFERENCES audit.pipeline_runs(run_id)
);

-- ============================================================
-- Populate dim_date (2020-2030)
-- ============================================================
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend, fiscal_year, fiscal_quarter)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d)::INTEGER + 1 ELSE EXTRACT(YEAR FROM d)::INTEGER END AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9) THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (1,2,3) THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;
