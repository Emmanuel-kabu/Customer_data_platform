CREATE SCHEMA IF NOT EXISTS analytics;
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

CREATE TABLE IF NOT EXISTS audit.dq_validation_runs (
    validation_id SERIAL PRIMARY KEY,
    report_id VARCHAR(200) NOT NULL,
    entity VARCHAR(100) NOT NULL,
    total_rows INTEGER DEFAULT 0,
    clean_rows INTEGER DEFAULT 0,
    quarantined_rows INTEGER DEFAULT 0,
    warnings_count INTEGER DEFAULT 0,
    pass_rate DECIMAL(5, 2),
    overall_status VARCHAR(20),
    batch_halted BOOLEAN DEFAULT FALSE,
    halt_reason TEXT,
    report_json JSONB,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.pre_load_rejections (
    rejection_id SERIAL PRIMARY KEY,
    entity VARCHAR(100) NOT NULL,
    source_data JSONB NOT NULL,
    rejection_reasons TEXT[],
    violated_rules TEXT[],
    validation_report_id VARCHAR(200),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed BOOLEAN DEFAULT FALSE,
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP,
    resolution VARCHAR(50)
);

CREATE OR REPLACE VIEW analytics.executive_kpis AS
SELECT
    CURRENT_DATE AS as_of_date,
    COUNT(DISTINCT f.sale_id) AS lifetime_orders,
    COUNT(DISTINCT f.customer_key) AS lifetime_customers,
    COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS lifetime_revenue,
    COALESCE(AVG(f.net_amount), 0)::NUMERIC(14, 2) AS lifetime_aov,
    COALESCE(SUM(f.net_amount) FILTER (
        WHERE d.full_date >= DATE_TRUNC('month', CURRENT_DATE)::DATE
    ), 0)::NUMERIC(14, 2) AS revenue_mtd,
    COALESCE(SUM(f.net_amount) FILTER (
        WHERE d.full_date >= DATE_TRUNC('quarter', CURRENT_DATE)::DATE
    ), 0)::NUMERIC(14, 2) AS revenue_qtd,
    COALESCE(SUM(f.net_amount) FILTER (
        WHERE d.full_date >= DATE_TRUNC('year', CURRENT_DATE)::DATE
    ), 0)::NUMERIC(14, 2) AS revenue_ytd
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON d.date_key = f.date_key;


CREATE OR REPLACE VIEW analytics.revenue_profit_trends AS
SELECT
    DATE_TRUNC('month', d.full_date)::DATE AS period_month,
    EXTRACT(YEAR FROM d.full_date)::INT AS year,
    EXTRACT(MONTH FROM d.full_date)::INT AS month,
    TO_CHAR(d.full_date, 'Mon') AS month_short,
    COALESCE(p.category, 'Unknown') AS category,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COALESCE(SUM(f.quantity), 0) AS total_units,
    COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
    COALESCE(SUM(f.quantity * COALESCE(p.cost_price, 0)), 0)::NUMERIC(14, 2) AS cogs,
    (
        COALESCE(SUM(f.net_amount), 0) -
        COALESCE(SUM(f.quantity * COALESCE(p.cost_price, 0)), 0)
    )::NUMERIC(14, 2) AS gross_profit,
    CASE
        WHEN COALESCE(SUM(f.net_amount), 0) = 0 THEN 0
        ELSE ROUND(
            (
                (
                    COALESCE(SUM(f.net_amount), 0) -
                    COALESCE(SUM(f.quantity * COALESCE(p.cost_price, 0)), 0)
                ) / NULLIF(COALESCE(SUM(f.net_amount), 0), 0)
            ) * 100,
            2
        )
    END AS gross_margin_pct
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
LEFT JOIN warehouse.dim_products p ON p.product_key = f.product_key
GROUP BY 1, 2, 3, 4, 5;


CREATE OR REPLACE VIEW analytics.revenue_sales_time_grains AS
WITH sales_enriched AS (
    SELECT
        d.full_date::DATE AS full_date,
        COALESCE(p.category, 'Unknown') AS category,
        f.sale_id,
        COALESCE(f.quantity, 0) AS quantity,
        COALESCE(f.net_amount, 0)::NUMERIC AS net_amount,
        (COALESCE(f.quantity, 0) * COALESCE(p.cost_price, 0))::NUMERIC AS cogs
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    LEFT JOIN warehouse.dim_products p ON p.product_key = f.product_key
),
grain_rollups AS (
    SELECT
        'day'::TEXT AS time_grain,
        full_date::DATE AS period_start,
        TO_CHAR(full_date, 'YYYY-MM-DD') AS period_label,
        category,
        COUNT(DISTINCT sale_id) AS total_sales,
        COALESCE(SUM(quantity), 0) AS total_units_sold,
        COALESCE(SUM(net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        (COALESCE(SUM(net_amount), 0) - COALESCE(SUM(cogs), 0))::NUMERIC(14, 2) AS gross_profit
    FROM sales_enriched
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        'week'::TEXT AS time_grain,
        DATE_TRUNC('week', full_date)::DATE AS period_start,
        TO_CHAR(DATE_TRUNC('week', full_date), 'IYYY-"W"IW') AS period_label,
        category,
        COUNT(DISTINCT sale_id) AS total_sales,
        COALESCE(SUM(quantity), 0) AS total_units_sold,
        COALESCE(SUM(net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        (COALESCE(SUM(net_amount), 0) - COALESCE(SUM(cogs), 0))::NUMERIC(14, 2) AS gross_profit
    FROM sales_enriched
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        'month'::TEXT AS time_grain,
        DATE_TRUNC('month', full_date)::DATE AS period_start,
        TO_CHAR(DATE_TRUNC('month', full_date), 'YYYY-MM') AS period_label,
        category,
        COUNT(DISTINCT sale_id) AS total_sales,
        COALESCE(SUM(quantity), 0) AS total_units_sold,
        COALESCE(SUM(net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        (COALESCE(SUM(net_amount), 0) - COALESCE(SUM(cogs), 0))::NUMERIC(14, 2) AS gross_profit
    FROM sales_enriched
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        'quarter'::TEXT AS time_grain,
        DATE_TRUNC('quarter', full_date)::DATE AS period_start,
        CONCAT(EXTRACT(YEAR FROM full_date)::INT, '-Q', EXTRACT(QUARTER FROM full_date)::INT) AS period_label,
        category,
        COUNT(DISTINCT sale_id) AS total_sales,
        COALESCE(SUM(quantity), 0) AS total_units_sold,
        COALESCE(SUM(net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        (COALESCE(SUM(net_amount), 0) - COALESCE(SUM(cogs), 0))::NUMERIC(14, 2) AS gross_profit
    FROM sales_enriched
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        'year'::TEXT AS time_grain,
        DATE_TRUNC('year', full_date)::DATE AS period_start,
        TO_CHAR(DATE_TRUNC('year', full_date), 'YYYY') AS period_label,
        category,
        COUNT(DISTINCT sale_id) AS total_sales,
        COALESCE(SUM(quantity), 0) AS total_units_sold,
        COALESCE(SUM(net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        (COALESCE(SUM(net_amount), 0) - COALESCE(SUM(cogs), 0))::NUMERIC(14, 2) AS gross_profit
    FROM sales_enriched
    GROUP BY 1, 2, 3, 4
)
SELECT
    time_grain,
    period_start,
    period_label,
    category,
    total_sales,
    total_units_sold,
    net_revenue,
    gross_profit,
    CASE
        WHEN net_revenue = 0 THEN 0
        ELSE ROUND((gross_profit / net_revenue) * 100, 2)
    END AS gross_margin_pct
FROM grain_rollups;


CREATE OR REPLACE VIEW analytics.customer_ltv AS
WITH sales_by_customer AS (
    SELECT
        dc.customer_id,
        COUNT(DISTINCT f.sale_id) AS total_orders,
        COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS lifetime_value,
        COALESCE(AVG(f.net_amount), 0)::NUMERIC(14, 2) AS avg_order_value,
        MIN(d.full_date) AS first_order_date,
        MAX(d.full_date) AS last_order_date
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers dc ON dc.customer_key = f.customer_key
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    GROUP BY dc.customer_id
)
SELECT
    cur.customer_id,
    cur.full_name,
    cur.city,
    cur.state,
    cur.country,
    cur.age_group,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(s.lifetime_value, 0)::NUMERIC(14, 2) AS lifetime_value,
    COALESCE(s.avg_order_value, 0)::NUMERIC(14, 2) AS avg_order_value,
    s.first_order_date,
    s.last_order_date,
    CASE
        WHEN s.last_order_date IS NULL THEN NULL
        ELSE (CURRENT_DATE - s.last_order_date)
    END AS recency_days,
    CASE
        WHEN COALESCE(s.lifetime_value, 0) >= 5000 THEN 'VIP'
        WHEN COALESCE(s.lifetime_value, 0) >= 2000 THEN 'Premium'
        WHEN COALESCE(s.lifetime_value, 0) >= 500 THEN 'Regular'
        ELSE 'New'
    END AS value_segment
FROM warehouse.dim_customers cur
LEFT JOIN sales_by_customer s ON s.customer_id = cur.customer_id
WHERE cur.is_current = TRUE;


CREATE OR REPLACE VIEW analytics.customer_retention_cohorts AS
WITH customer_orders AS (
    SELECT
        dc.customer_id,
        DATE_TRUNC('month', d.full_date)::DATE AS order_month
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers dc ON dc.customer_key = f.customer_key
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
),
first_purchase AS (
    SELECT
        customer_id,
        MIN(order_month) AS cohort_month
    FROM customer_orders
    GROUP BY customer_id
),
cohort_activity AS (
    SELECT
        fp.cohort_month,
        co.order_month AS activity_month,
        COUNT(DISTINCT co.customer_id) AS active_customers
    FROM first_purchase fp
    JOIN customer_orders co ON co.customer_id = fp.customer_id
    GROUP BY fp.cohort_month, co.order_month
),
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_customers
    FROM first_purchase
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    ca.activity_month,
    cs.cohort_customers,
    ca.active_customers,
    (
        ca.active_customers::NUMERIC / NULLIF(cs.cohort_customers, 0) * 100
    )::NUMERIC(6, 2) AS retention_pct,
    (
        (
            DATE_PART('year', AGE(ca.activity_month, ca.cohort_month)) * 12
        ) + DATE_PART('month', AGE(ca.activity_month, ca.cohort_month))
    )::INT AS cohort_age_months
FROM cohort_activity ca
JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month;


CREATE OR REPLACE VIEW analytics.product_performance AS
WITH sales_by_product AS (
    SELECT
        dp.product_id,
        COUNT(DISTINCT f.sale_id) AS total_orders,
        COALESCE(SUM(f.quantity), 0) AS units_sold,
        COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
        COALESCE(AVG(f.unit_price), 0)::NUMERIC(12, 2) AS avg_selling_price,
        COALESCE(SUM(f.quantity * COALESCE(dp.cost_price, 0)), 0)::NUMERIC(14, 2) AS cogs
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products dp ON dp.product_key = f.product_key
    GROUP BY dp.product_id
)
SELECT
    cur.product_id,
    cur.product_name,
    cur.category,
    cur.sub_category,
    cur.brand,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(s.units_sold, 0) AS units_sold,
    COALESCE(s.net_revenue, 0)::NUMERIC(14, 2) AS net_revenue,
    COALESCE(s.avg_selling_price, 0)::NUMERIC(12, 2) AS avg_selling_price,
    COALESCE(s.cogs, 0)::NUMERIC(14, 2) AS cogs,
    (COALESCE(s.net_revenue, 0) - COALESCE(s.cogs, 0))::NUMERIC(14, 2) AS gross_profit,
    cur.stock_quantity,
    cur.stock_status
FROM warehouse.dim_products cur
LEFT JOIN sales_by_product s ON s.product_id = cur.product_id
WHERE cur.is_current = TRUE;


CREATE OR REPLACE VIEW analytics.inventory_health AS
SELECT
    product_id,
    product_name,
    category,
    brand,
    stock_quantity,
    unit_price,
    cost_price,
    (stock_quantity * cost_price)::NUMERIC(14, 2) AS inventory_cost_value,
    (stock_quantity * unit_price)::NUMERIC(14, 2) AS inventory_sales_value,
    CASE
        WHEN stock_quantity <= 0 THEN 'stockout'
        WHEN stock_quantity <= 10 THEN 'critical'
        WHEN stock_quantity <= 50 THEN 'low'
        WHEN stock_quantity <= 200 THEN 'healthy'
        ELSE 'high'
    END AS stock_risk_band
FROM warehouse.dim_products
WHERE is_current = TRUE;


CREATE OR REPLACE VIEW analytics.discount_effectiveness AS
SELECT
    DATE_TRUNC('month', d.full_date)::DATE AS period_month,
    CASE
        WHEN COALESCE(f.discount_percent, 0) = 0 THEN 'No Discount'
        WHEN f.discount_percent <= 10 THEN 'Low Discount (1-10%)'
        WHEN f.discount_percent <= 20 THEN 'Medium Discount (11-20%)'
        ELSE 'High Discount (>20%)'
    END AS discount_band,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
    COALESCE(SUM(f.discount_amount), 0)::NUMERIC(14, 2) AS discount_amount,
    COALESCE(AVG(f.net_amount), 0)::NUMERIC(12, 2) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
GROUP BY 1, 2;


CREATE OR REPLACE VIEW analytics.payment_mix AS
SELECT
    DATE_TRUNC('month', d.full_date)::DATE AS period_month,
    f.payment_method,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS net_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
GROUP BY 1, 2;


CREATE OR REPLACE VIEW analytics.geo_performance AS
SELECT
    dc.state,
    dc.city,
    COUNT(DISTINCT f.sale_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS active_customers,
    COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS net_revenue,
    COALESCE(AVG(f.net_amount), 0)::NUMERIC(12, 2) AS avg_order_value,
    (
        COUNT(DISTINCT CASE WHEN cc.total_orders > 1 THEN f.customer_key END)::NUMERIC /
        NULLIF(COUNT(DISTINCT f.customer_key), 0) * 100
    )::NUMERIC(6, 2) AS repeat_customer_rate_pct
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers dc ON dc.customer_key = f.customer_key
LEFT JOIN (
    SELECT customer_key, COUNT(DISTINCT sale_id) AS total_orders
    FROM warehouse.fact_sales
    GROUP BY customer_key
) cc ON cc.customer_key = f.customer_key
GROUP BY dc.state, dc.city;


CREATE OR REPLACE VIEW analytics.pipeline_health AS
SELECT
    run_id,
    pipeline_name,
    dag_id,
    run_date,
    status,
    records_processed,
    records_inserted,
    records_updated,
    records_rejected,
    execution_time_seconds,
    error_message
FROM audit.pipeline_runs
ORDER BY run_date DESC;


CREATE OR REPLACE VIEW analytics.pre_load_rejection_summary AS
SELECT
    DATE(rejected_at) AS rejection_date,
    entity,
    COUNT(*) AS rejected_rows,
    COUNT(*) FILTER (WHERE reviewed = FALSE) AS pending_review,
    COUNT(*) FILTER (WHERE reviewed = TRUE) AS reviewed_rows
FROM audit.pre_load_rejections
GROUP BY DATE(rejected_at), entity
ORDER BY rejection_date DESC, entity;


CREATE OR REPLACE VIEW analytics.revenue_orders_forecast_30d AS
WITH daily_actuals AS (
    SELECT
        d.full_date::DATE AS metric_date,
        COUNT(DISTINCT f.sale_id)::NUMERIC AS actual_orders,
        COALESCE(SUM(f.net_amount), 0)::NUMERIC AS actual_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    GROUP BY d.full_date
),
history_window AS (
    SELECT
        metric_date,
        actual_orders,
        actual_revenue,
        ROW_NUMBER() OVER (ORDER BY metric_date) AS day_idx
    FROM daily_actuals
    WHERE metric_date >= (CURRENT_DATE - INTERVAL '120 days')::DATE
),
regression AS (
    SELECT
        COALESCE(REGR_SLOPE(actual_orders, day_idx), 0) AS orders_slope,
        COALESCE(REGR_INTERCEPT(actual_orders, day_idx), AVG(actual_orders)) AS orders_intercept,
        COALESCE(REGR_SLOPE(actual_revenue, day_idx), 0) AS revenue_slope,
        COALESCE(REGR_INTERCEPT(actual_revenue, day_idx), AVG(actual_revenue)) AS revenue_intercept,
        COALESCE(STDDEV_SAMP(actual_orders), 0) AS orders_sigma,
        COALESCE(STDDEV_SAMP(actual_revenue), 0) AS revenue_sigma,
        COALESCE(MAX(day_idx), 0) AS max_day_idx
    FROM history_window
),
future_dates AS (
    SELECT
        gs::DATE AS forecast_date,
        ROW_NUMBER() OVER (ORDER BY gs) AS step_ahead
    FROM GENERATE_SERIES(
        (CURRENT_DATE + INTERVAL '1 day')::DATE,
        (CURRENT_DATE + INTERVAL '30 days')::DATE,
        INTERVAL '1 day'
    ) AS gs
)
SELECT
    fd.forecast_date,
    GREATEST(
        0,
        ROUND((r.orders_intercept + r.orders_slope * (r.max_day_idx + fd.step_ahead))::NUMERIC, 0)
    )::INTEGER AS projected_orders,
    GREATEST(
        0,
        ROUND((r.revenue_intercept + r.revenue_slope * (r.max_day_idx + fd.step_ahead))::NUMERIC, 2)
    )::NUMERIC(14, 2) AS projected_revenue,
    GREATEST(
        0,
        ROUND((r.orders_intercept + r.orders_slope * (r.max_day_idx + fd.step_ahead) - 1.96 * r.orders_sigma)::NUMERIC, 0)
    )::INTEGER AS projected_orders_lower,
    GREATEST(
        0,
        ROUND((r.orders_intercept + r.orders_slope * (r.max_day_idx + fd.step_ahead) + 1.96 * r.orders_sigma)::NUMERIC, 0)
    )::INTEGER AS projected_orders_upper,
    GREATEST(
        0,
        ROUND((r.revenue_intercept + r.revenue_slope * (r.max_day_idx + fd.step_ahead) - 1.96 * r.revenue_sigma)::NUMERIC, 2)
    )::NUMERIC(14, 2) AS projected_revenue_lower,
    GREATEST(
        0,
        ROUND((r.revenue_intercept + r.revenue_slope * (r.max_day_idx + fd.step_ahead) + 1.96 * r.revenue_sigma)::NUMERIC, 2)
    )::NUMERIC(14, 2) AS projected_revenue_upper
FROM future_dates fd
CROSS JOIN regression r
ORDER BY fd.forecast_date;


CREATE OR REPLACE VIEW analytics.revenue_orders_actual_vs_forecast AS
WITH daily_actuals AS (
    SELECT
        d.full_date::DATE AS metric_date,
        COUNT(DISTINCT f.sale_id)::INTEGER AS orders_count,
        COALESCE(SUM(f.net_amount), 0)::NUMERIC(14, 2) AS revenue_amount
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    GROUP BY d.full_date
),
actual_series AS (
    SELECT
        metric_date,
        orders_count,
        revenue_amount,
        'actual'::TEXT AS series_type
    FROM daily_actuals
    WHERE metric_date >= (CURRENT_DATE - INTERVAL '30 days')::DATE
),
forecast_series AS (
    SELECT
        forecast_date AS metric_date,
        projected_orders AS orders_count,
        projected_revenue AS revenue_amount,
        'forecast'::TEXT AS series_type
    FROM analytics.revenue_orders_forecast_30d
)
SELECT
    metric_date,
    orders_count,
    revenue_amount,
    series_type
FROM actual_series
UNION ALL
SELECT
    metric_date,
    orders_count,
    revenue_amount,
    series_type
FROM forecast_series
ORDER BY metric_date, series_type;


CREATE OR REPLACE VIEW analytics.revenue_orders_forecast_summary AS
WITH baseline_30d AS (
    SELECT
        COALESCE(SUM(revenue_amount), 0)::NUMERIC(14, 2) AS baseline_revenue_30d,
        COALESCE(SUM(orders_count), 0)::INTEGER AS baseline_orders_30d
    FROM analytics.revenue_orders_actual_vs_forecast
    WHERE series_type = 'actual'
),
forecast_30d AS (
    SELECT
        COALESCE(SUM(projected_revenue), 0)::NUMERIC(14, 2) AS forecast_revenue_30d,
        COALESCE(SUM(projected_orders), 0)::INTEGER AS forecast_orders_30d,
        COALESCE(AVG(projected_revenue), 0)::NUMERIC(14, 2) AS avg_daily_forecast_revenue,
        COALESCE(AVG(projected_orders), 0)::NUMERIC(14, 2) AS avg_daily_forecast_orders
    FROM analytics.revenue_orders_forecast_30d
)
SELECT
    CURRENT_DATE AS as_of_date,
    b.baseline_revenue_30d,
    f.forecast_revenue_30d,
    CASE
        WHEN b.baseline_revenue_30d = 0 THEN NULL
        ELSE ROUND(((f.forecast_revenue_30d - b.baseline_revenue_30d) / b.baseline_revenue_30d) * 100, 2)
    END AS revenue_delta_pct_vs_last_30d,
    b.baseline_orders_30d,
    f.forecast_orders_30d,
    CASE
        WHEN b.baseline_orders_30d = 0 THEN NULL
        ELSE ROUND((((f.forecast_orders_30d - b.baseline_orders_30d)::NUMERIC) / b.baseline_orders_30d) * 100, 2)
    END AS orders_delta_pct_vs_last_30d,
    f.avg_daily_forecast_revenue,
    f.avg_daily_forecast_orders
FROM baseline_30d b
CROSS JOIN forecast_30d f;
