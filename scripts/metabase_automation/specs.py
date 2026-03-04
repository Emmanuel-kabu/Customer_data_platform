"""Declarative Metabase dashboard and card specifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple


PARAM_DATE_FROM = "param_date_from"
PARAM_DATE_TO = "param_date_to"
PARAM_CATEGORY = "param_category"
PARAM_STATE = "param_state"
PARAM_PAYMENT_METHOD = "param_payment_method"
PARAM_SEGMENT = "param_segment"
PARAM_TIME_GRAIN = "param_time_grain"


def _tag_date(name: str, display_name: str) -> Dict[str, dict]:
    return {
        name: {
            "id": name,
            "name": name,
            "display-name": display_name,
            "type": "date",
            "required": False,
        }
    }


def _tag_text(name: str, display_name: str) -> Dict[str, dict]:
    return {
        name: {
            "id": name,
            "name": name,
            "display-name": display_name,
            "type": "text",
            "required": False,
        }
    }


def _tag_field_filter(name: str, display_name: str, field_ref: str, widget_type: str = "category") -> Dict[str, dict]:
    # dimension is resolved to a Metabase field reference by the provisioner.
    return {
        name: {
            "id": name,
            "name": name,
            "display-name": display_name,
            "type": "dimension",
            "dimension": field_ref,
            "widget-type": widget_type,
            "required": False,
        }
    }


def _merge_tags(*parts: Dict[str, dict]) -> Dict[str, dict]:
    merged: Dict[str, dict] = {}
    for part in parts:
        merged.update(part)
    return merged


@dataclass(frozen=True)
class CardSpec:
    name: str
    description: str
    query: str
    display: str
    layout: Dict[str, int]
    template_tags: Dict[str, dict] = field(default_factory=dict)
    mappings: List[Tuple[str, str]] = field(default_factory=list)
    question_ids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class DashboardSpec:
    name: str
    description: str
    collection_path: List[str]
    parameters: List[dict]
    cards: List[CardSpec]


DATE_PARAMETERS = [
    {"id": PARAM_DATE_FROM, "name": "Date From", "slug": "date_from", "type": "date/single"},
    {"id": PARAM_DATE_TO, "name": "Date To", "slug": "date_to", "type": "date/single"},
]

FILTER_PARAMETERS = {
    PARAM_CATEGORY: {"id": PARAM_CATEGORY, "name": "Category", "slug": "category", "type": "category"},
    PARAM_STATE: {"id": PARAM_STATE, "name": "State", "slug": "state", "type": "category"},
    PARAM_PAYMENT_METHOD: {
        "id": PARAM_PAYMENT_METHOD,
        "name": "Payment Method",
        "slug": "payment_method",
        "type": "category",
    },
    PARAM_SEGMENT: {"id": PARAM_SEGMENT, "name": "Segment", "slug": "segment", "type": "category"},
    PARAM_TIME_GRAIN: {"id": PARAM_TIME_GRAIN, "name": "Time Grain", "slug": "time_grain", "type": "category"},
}


DASHBOARD_SPECS: List[DashboardSpec] = [
    DashboardSpec(
        name="Executive Command Center",
        description="High-level view of commercial performance and platform reliability.",
        collection_path=["00 Executive"],
        parameters=DATE_PARAMETERS + [FILTER_PARAMETERS[PARAM_CATEGORY], FILTER_PARAMETERS[PARAM_STATE]],
        cards=[
            CardSpec(
                name="Executive KPI Snapshot",
                description="Lifetime and current-period KPI values.",
                query="""
                    SELECT *
                    FROM analytics.executive_kpis
                """,
                display="table",
                layout={"row": 0, "col": 0, "sizeX": 8, "sizeY": 5},
                question_ids=["EXE-001"],
            ),
            CardSpec(
                name="Revenue and Margin Trend",
                description="Month-over-month revenue and gross margin trend.",
                query="""
                    SELECT
                        period_month,
                        SUM(net_revenue) AS net_revenue,
                        SUM(gross_profit) AS gross_profit,
                        AVG(gross_margin_pct) AS gross_margin_pct
                    FROM analytics.revenue_profit_trends
                    WHERE 1=1
                      [[AND period_month >= {{date_from}}]]
                      [[AND period_month <= {{date_to}}]]
                      [[AND {{category}}]]
                    GROUP BY period_month
                    ORDER BY period_month
                """,
                display="line",
                layout={"row": 0, "col": 8, "sizeX": 10, "sizeY": 5},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_field_filter("category", "Category", "analytics.revenue_profit_trends.category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["EXE-002"],
            ),
            CardSpec(
                name="Revenue by State",
                description="Revenue and repeat rate by state.",
                query="""
                    SELECT
                        state,
                        SUM(net_revenue) AS net_revenue,
                        SUM(total_orders) AS total_orders,
                        AVG(repeat_customer_rate_pct) AS repeat_customer_rate_pct
                    FROM analytics.geo_performance
                    WHERE 1=1
                      [[AND {{state}}]]
                    GROUP BY state
                    ORDER BY net_revenue DESC
                """,
                display="bar",
                layout={"row": 5, "col": 0, "sizeX": 9, "sizeY": 6},
                template_tags=_tag_field_filter("state", "State", "analytics.geo_performance.state"),
                mappings=[(PARAM_STATE, "state")],
                question_ids=["EXE-003"],
            ),
            CardSpec(
                name="Pipeline Health Status",
                description="Latest ETL and orchestration reliability details.",
                query="""
                    SELECT
                        run_date,
                        pipeline_name,
                        status,
                        records_inserted,
                        records_updated,
                        records_rejected,
                        execution_time_seconds
                    FROM analytics.pipeline_health
                    ORDER BY run_date DESC
                    LIMIT 50
                """,
                display="table",
                layout={"row": 5, "col": 9, "sizeX": 9, "sizeY": 6},
                question_ids=["EXE-004"],
            ),
        ],
    ),
    DashboardSpec(
        name="Revenue and Profit Intelligence",
        description="Revenue growth, category margins, discounts, and payment composition.",
        collection_path=["10 Revenue and Profit"],
        parameters=DATE_PARAMETERS + [
            FILTER_PARAMETERS[PARAM_CATEGORY],
            FILTER_PARAMETERS[PARAM_PAYMENT_METHOD],
            FILTER_PARAMETERS[PARAM_TIME_GRAIN],
        ],
        cards=[
            CardSpec(
                name="Revenue KPI by Grain",
                description="Total net revenue for the selected time grain, period, and optional category.",
                query="""
                    SELECT COALESCE(SUM(net_revenue), 0) AS total_revenue
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                """,
                display="scalar",
                layout={"row": 0, "col": 0, "sizeX": 4, "sizeY": 4},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-007"],
            ),
            CardSpec(
                name="Sales KPI by Grain",
                description="Total completed sales for the selected time grain, period, and optional category.",
                query="""
                    SELECT COALESCE(SUM(total_sales), 0) AS total_sales
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                """,
                display="scalar",
                layout={"row": 0, "col": 4, "sizeX": 4, "sizeY": 4},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-008"],
            ),
            CardSpec(
                name="Units Sold KPI by Grain",
                description="Total units sold for the selected time grain, period, and optional category.",
                query="""
                    SELECT COALESCE(SUM(total_units_sold), 0) AS total_units_sold
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                """,
                display="scalar",
                layout={"row": 0, "col": 8, "sizeX": 5, "sizeY": 4},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-009"],
            ),
            CardSpec(
                name="Gross Profit KPI by Grain",
                description="Total gross profit for the selected time grain, period, and optional category.",
                query="""
                    SELECT COALESCE(SUM(gross_profit), 0) AS total_gross_profit
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                """,
                display="scalar",
                layout={"row": 0, "col": 13, "sizeX": 5, "sizeY": 4},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-010"],
            ),
            CardSpec(
                name="Revenue, Sales, and Units by Time Grain",
                description="Revenue, sales count, and units sold across daily to yearly grains.",
                query="""
                    SELECT
                        period_start,
                        time_grain,
                        SUM(total_sales) AS total_sales,
                        SUM(total_units_sold) AS total_units_sold,
                        SUM(net_revenue) AS net_revenue,
                        SUM(gross_profit) AS gross_profit,
                        AVG(gross_margin_pct) AS gross_margin_pct
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                    GROUP BY period_start, time_grain
                    ORDER BY period_start
                """,
                display="line",
                layout={"row": 4, "col": 0, "sizeX": 12, "sizeY": 6},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-001", "FIN-005"],
            ),
            CardSpec(
                name="Category Performance by Time Grain",
                description="Revenue, sales count, and units sold by category for a selected grain.",
                query="""
                    SELECT
                        time_grain,
                        category,
                        SUM(total_sales) AS total_sales,
                        SUM(total_units_sold) AS total_units_sold,
                        SUM(net_revenue) AS net_revenue,
                        SUM(gross_profit) AS gross_profit,
                        AVG(gross_margin_pct) AS gross_margin_pct
                    FROM analytics.revenue_sales_time_grains
                    WHERE 1=1
                      [[AND period_start >= {{date_from}}]]
                      [[AND period_start <= {{date_to}}]]
                      [[AND time_grain = {{time_grain}}]]
                      [[AND category = {{category}}]]
                    GROUP BY time_grain, category
                    ORDER BY net_revenue DESC
                """,
                display="bar",
                layout={"row": 4, "col": 12, "sizeX": 6, "sizeY": 6},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_text("time_grain", "Time Grain"),
                    _tag_text("category", "Category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_TIME_GRAIN, "time_grain"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["FIN-002", "FIN-006"],
            ),
            CardSpec(
                name="Discount Effectiveness",
                description="Impact of discounting on orders and revenue.",
                query="""
                    SELECT
                        period_month,
                        discount_band,
                        total_orders,
                        net_revenue,
                        discount_amount,
                        avg_order_value
                    FROM analytics.discount_effectiveness
                    WHERE 1=1
                      [[AND period_month >= {{date_from}}]]
                      [[AND period_month <= {{date_to}}]]
                    ORDER BY period_month, discount_band
                """,
                display="table",
                layout={"row": 10, "col": 0, "sizeX": 9, "sizeY": 6},
                template_tags=_merge_tags(_tag_date("date_from", "Date From"), _tag_date("date_to", "Date To")),
                mappings=[(PARAM_DATE_FROM, "date_from"), (PARAM_DATE_TO, "date_to")],
                question_ids=["FIN-003"],
            ),
            CardSpec(
                name="Payment Mix",
                description="Order and revenue distribution by payment method.",
                query="""
                    SELECT
                        period_month,
                        payment_method,
                        total_orders,
                        net_revenue
                    FROM analytics.payment_mix
                    WHERE 1=1
                      [[AND period_month >= {{date_from}}]]
                      [[AND period_month <= {{date_to}}]]
                      [[AND {{payment_method}}]]
                    ORDER BY period_month, net_revenue DESC
                """,
                display="table",
                layout={"row": 10, "col": 9, "sizeX": 9, "sizeY": 6},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_field_filter("payment_method", "Payment Method", "analytics.payment_mix.payment_method"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_PAYMENT_METHOD, "payment_method"),
                ],
                question_ids=["FIN-004"],
            ),
        ],
    ),
    DashboardSpec(
        name="Customer Growth and Retention",
        description="Customer value, segmentation, and retention movement.",
        collection_path=["20 Customer Analytics"],
        parameters=[FILTER_PARAMETERS[PARAM_STATE], FILTER_PARAMETERS[PARAM_SEGMENT]],
        cards=[
            CardSpec(
                name="Customer LTV Leaderboard",
                description="Highest value customers and purchase cadence.",
                query="""
                    SELECT
                        customer_id,
                        full_name,
                        state,
                        value_segment,
                        total_orders,
                        lifetime_value,
                        avg_order_value,
                        recency_days
                    FROM analytics.customer_ltv
                    WHERE 1=1
                      [[AND {{state}}]]
                      [[AND {{segment}}]]
                    ORDER BY lifetime_value DESC
                    LIMIT 100
                """,
                display="table",
                layout={"row": 0, "col": 0, "sizeX": 9, "sizeY": 6},
                template_tags=_merge_tags(
                    _tag_field_filter("state", "State", "analytics.customer_ltv.state"),
                    _tag_field_filter("segment", "Segment", "analytics.customer_ltv.value_segment"),
                ),
                mappings=[(PARAM_STATE, "state"), (PARAM_SEGMENT, "segment")],
                question_ids=["CUS-001"],
            ),
            CardSpec(
                name="Cohort Retention Matrix",
                description="Customer retention trajectory by cohort age.",
                query="""
                    SELECT
                        cohort_month,
                        cohort_age_months,
                        retention_pct,
                        active_customers,
                        cohort_customers
                    FROM analytics.customer_retention_cohorts
                    ORDER BY cohort_month, cohort_age_months
                """,
                display="table",
                layout={"row": 0, "col": 9, "sizeX": 9, "sizeY": 6},
                question_ids=["CUS-002"],
            ),
            CardSpec(
                name="Customer Segment Distribution",
                description="Customer count and value by segment.",
                query="""
                    SELECT
                        value_segment,
                        COUNT(*) AS customers,
                        SUM(lifetime_value) AS total_value,
                        AVG(lifetime_value) AS avg_value
                    FROM analytics.customer_ltv
                    GROUP BY value_segment
                    ORDER BY total_value DESC
                """,
                display="bar",
                layout={"row": 6, "col": 0, "sizeX": 9, "sizeY": 6},
                question_ids=["CUS-003"],
            ),
            CardSpec(
                name="Repeat Rate by Geography",
                description="Repeat customer rate by state and city.",
                query="""
                    SELECT
                        state,
                        city,
                        total_orders,
                        net_revenue,
                        repeat_customer_rate_pct
                    FROM analytics.geo_performance
                    WHERE 1=1
                      [[AND {{state}}]]
                    ORDER BY repeat_customer_rate_pct DESC
                    LIMIT 100
                """,
                display="table",
                layout={"row": 6, "col": 9, "sizeX": 9, "sizeY": 6},
                template_tags=_tag_field_filter("state", "State", "analytics.geo_performance.state"),
                mappings=[(PARAM_STATE, "state")],
                question_ids=["CUS-004"],
            ),
        ],
    ),
    DashboardSpec(
        name="Product and Inventory Intelligence",
        description="Merchandising performance, stock posture, and demand evolution.",
        collection_path=["30 Product and Inventory"],
        parameters=DATE_PARAMETERS + [FILTER_PARAMETERS[PARAM_CATEGORY]],
        cards=[
            CardSpec(
                name="Top Product Performance",
                description="Revenue and unit leaders.",
                query="""
                    SELECT
                        product_id,
                        product_name,
                        category,
                        brand,
                        total_orders,
                        units_sold,
                        net_revenue,
                        gross_profit
                    FROM analytics.product_performance
                    WHERE 1=1
                      [[AND {{category}}]]
                    ORDER BY net_revenue DESC
                    LIMIT 100
                """,
                display="table",
                layout={"row": 0, "col": 0, "sizeX": 10, "sizeY": 6},
                template_tags=_tag_field_filter("category", "Category", "analytics.product_performance.category"),
                mappings=[(PARAM_CATEGORY, "category")],
                question_ids=["PRD-001"],
            ),
            CardSpec(
                name="Inventory Risk Monitor",
                description="Stock health and inventory valuation by SKU.",
                query="""
                    SELECT
                        product_id,
                        product_name,
                        category,
                        stock_quantity,
                        stock_risk_band,
                        inventory_cost_value,
                        inventory_sales_value
                    FROM analytics.inventory_health
                    WHERE 1=1
                      [[AND {{category}}]]
                    ORDER BY stock_quantity ASC, inventory_cost_value DESC
                    LIMIT 150
                """,
                display="table",
                layout={"row": 0, "col": 10, "sizeX": 8, "sizeY": 6},
                template_tags=_tag_field_filter("category", "Category", "analytics.inventory_health.category"),
                mappings=[(PARAM_CATEGORY, "category")],
                question_ids=["PRD-002"],
            ),
            CardSpec(
                name="Category Demand Trend",
                description="Demand and revenue trend by month and category.",
                query="""
                    SELECT
                        period_month,
                        category,
                        SUM(total_units) AS total_units,
                        SUM(net_revenue) AS net_revenue
                    FROM analytics.revenue_profit_trends
                    WHERE 1=1
                      [[AND period_month >= {{date_from}}]]
                      [[AND period_month <= {{date_to}}]]
                      [[AND {{category}}]]
                    GROUP BY period_month, category
                    ORDER BY period_month, net_revenue DESC
                """,
                display="line",
                layout={"row": 6, "col": 0, "sizeX": 18, "sizeY": 6},
                template_tags=_merge_tags(
                    _tag_date("date_from", "Date From"),
                    _tag_date("date_to", "Date To"),
                    _tag_field_filter("category", "Category", "analytics.revenue_profit_trends.category"),
                ),
                mappings=[
                    (PARAM_DATE_FROM, "date_from"),
                    (PARAM_DATE_TO, "date_to"),
                    (PARAM_CATEGORY, "category"),
                ],
                question_ids=["PRD-003"],
            ),
        ],
    ),
    DashboardSpec(
        name="Operations and Data Quality",
        description="Pipeline reliability, DQ signals, and rejection monitoring.",
        collection_path=["40 Operations and DQ"],
        parameters=[],
        cards=[
            CardSpec(
                name="Latest Pipeline Runs",
                description="Recent pipeline run states and processing stats.",
                query="""
                    SELECT
                        run_date,
                        pipeline_name,
                        status,
                        records_inserted,
                        records_updated,
                        records_rejected,
                        execution_time_seconds,
                        error_message
                    FROM analytics.pipeline_health
                    ORDER BY run_date DESC
                    LIMIT 100
                """,
                display="table",
                layout={"row": 0, "col": 0, "sizeX": 12, "sizeY": 6},
                question_ids=["OPS-001"],
            ),
            CardSpec(
                name="DQ Trend by Entity",
                description="Validation pass rates and quarantine trend.",
                query="""
                    SELECT
                        validation_date,
                        entity,
                        validation_runs,
                        avg_pass_rate,
                        total_rows_processed,
                        total_quarantined,
                        batches_halted
                    FROM analytics.dq_trends
                    ORDER BY validation_date DESC, entity
                    LIMIT 180
                """,
                display="table",
                layout={"row": 0, "col": 12, "sizeX": 6, "sizeY": 6},
                question_ids=["OPS-002"],
            ),
            CardSpec(
                name="Pre-Load Rejection Summary",
                description="Entity-level pre-load rejection counts and review backlog.",
                query="""
                    SELECT
                        rejection_date,
                        entity,
                        rejected_rows,
                        pending_review,
                        reviewed_rows
                    FROM analytics.pre_load_rejection_summary
                    ORDER BY rejection_date DESC, entity
                    LIMIT 180
                """,
                display="table",
                layout={"row": 6, "col": 0, "sizeX": 18, "sizeY": 6},
                question_ids=["OPS-003"],
            ),
        ],
    ),
    DashboardSpec(
        name="Forecasting and Planning",
        description="Next 30-day projections for revenue and order volume with baseline comparison.",
        collection_path=["50 Forecasting and Planning"],
        parameters=[],
        cards=[
            CardSpec(
                name="30-Day Revenue Forecast",
                description="Daily projected revenue and confidence band for the next 30 days.",
                query="""
                    SELECT
                        forecast_date,
                        projected_revenue,
                        projected_revenue_lower,
                        projected_revenue_upper
                    FROM analytics.revenue_orders_forecast_30d
                    ORDER BY forecast_date
                """,
                display="line",
                layout={"row": 0, "col": 0, "sizeX": 12, "sizeY": 6},
                question_ids=["FRC-001"],
            ),
            CardSpec(
                name="30-Day Orders Forecast",
                description="Daily projected orders and confidence band for the next 30 days.",
                query="""
                    SELECT
                        forecast_date,
                        projected_orders,
                        projected_orders_lower,
                        projected_orders_upper
                    FROM analytics.revenue_orders_forecast_30d
                    ORDER BY forecast_date
                """,
                display="line",
                layout={"row": 0, "col": 12, "sizeX": 6, "sizeY": 6},
                question_ids=["FRC-002"],
            ),
            CardSpec(
                name="Actual vs Forecast Bridge",
                description="Last 30 days actuals plus next 30 days projected trend.",
                query="""
                    SELECT
                        metric_date,
                        series_type,
                        orders_count,
                        revenue_amount
                    FROM analytics.revenue_orders_actual_vs_forecast
                    ORDER BY metric_date, series_type
                """,
                display="line",
                layout={"row": 6, "col": 0, "sizeX": 12, "sizeY": 6},
                question_ids=["FRC-002"],
            ),
            CardSpec(
                name="Forecast vs Baseline Summary",
                description="Forecast totals compared to the most recent 30-day actual baseline.",
                query="""
                    SELECT
                        as_of_date,
                        baseline_revenue_30d,
                        forecast_revenue_30d,
                        revenue_delta_pct_vs_last_30d,
                        baseline_orders_30d,
                        forecast_orders_30d,
                        orders_delta_pct_vs_last_30d,
                        avg_daily_forecast_revenue,
                        avg_daily_forecast_orders
                    FROM analytics.revenue_orders_forecast_summary
                """,
                display="table",
                layout={"row": 6, "col": 12, "sizeX": 6, "sizeY": 6},
                question_ids=["FRC-003"],
            ),
        ],
    ),
]
