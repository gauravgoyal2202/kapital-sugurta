{{ config(materialized='table') }}

/*
  Dashboard 16 — Aggregated Customer Base Summary
  ------------------------------------------------
  Final summary dashboard table aggregating the retention spine 
  into historical time-series metrics.
*/

WITH spine AS (
    SELECT * FROM {{ ref('mart_customer_retention_spine') }}
)

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month) AS report_year,
    EXTRACT(QUARTER FROM report_month) AS report_quarter,
    
    legal_form,
    customer_segment,
    oked_industry_code,
    region_code,
    
    -- Scenario flag matching dashboard Plan/Fact/Previous Year legend
    'Actual' AS scenario,
    
    -- Top Metric #1: Active Customers
    COUNT(DISTINCT customer_id) AS active_customers,
    
    -- Sub-Metric: Raw Policies Active 
    SUM(active_policy_count) AS total_active_policies,
    
    -- Top Metric #2: Average Policies Per Customer
    ROUND((SUM(active_policy_count)::NUMERIC / NULLIF(COUNT(DISTINCT customer_id), 0)), 2) AS avg_policies_per_customer,
    
    -- Top Metric #3: Retention Rate components
    SUM(is_retained) AS retained_customers_from_prev_month
    
    -- NOTE: In Power BI, True Retention Rate % = 
    -- ( SUM(retained_customers_from_prev_month) / CALCULATE(SUM(active_customers), PREVIOUSMONTH(report_month)) ) * 100
    
FROM spine
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY report_month DESC
