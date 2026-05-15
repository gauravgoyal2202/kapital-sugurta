{{ config(materialized='table') }}

/*
  Dashboard 17 — Customer LTV & CAC (Quarterly) - Client Logic Version
  ---------------------------------------------------------------------
  Updated Logic:
  LTV = Avg Margin (Profit per Customer) * Retention Rate * Average Lifetime
  CAC = Total Marketing Spend / New Customers
*/

WITH quarterly_metrics AS (
    SELECT
        report_month,
        DATE_TRUNC('quarter', report_month)::DATE AS report_quarter,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        active_customers,
        total_active_policies,
        retained_customers,
        customers_at_start_of_period,
        retention_rate_pct
    FROM {{ ref('mart_customer_base_summary') }}
),

quarterly_agg AS (
    SELECT
        report_quarter,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        
        -- Avg Active Customers in the quarter
        AVG(active_customers) AS avg_active_customers,
        
        -- Avg Retention Rate in the quarter
        AVG(retention_rate_pct) / 100.0 AS avg_retention_rate,
        
        -- New Customers (Simple sum of months? No, we should ideally use the quarterly new flag)
        -- For now, using the average monthly active as a proxy for the quarterly base
        SUM(active_customers) AS total_customer_months
    FROM quarterly_metrics
    GROUP BY 1, 2, 3, 4, 5
),

-- Financial components (Profit and Marketing)
financial_agg AS (
    SELECT
        DATE_TRUNC('quarter', report_date)::DATE AS report_quarter,
        SUM(net_profit) AS total_net_profit_uzs,
        SUM(marketing_expenses) AS total_marketing_expenses_uzs,
        SUM(premium_income) AS total_premium_income_uzs
    FROM {{ ref('curated_financial_performance') }}
    GROUP BY 1
),

-- New Customers identification (Quarterly level)
-- (We use the monthly spine to find who was NEW this quarter vs previous quarter)
new_customers_q AS (
    SELECT
        DATE_TRUNC('quarter', report_month)::DATE AS report_quarter,
        legal_form,
        customer_segment,
        oked_industry_code,
        region_code,
        COUNT(DISTINCT customer_id) AS new_customers_acquired
    FROM {{ ref('mart_customer_retention_spine') }}
    WHERE is_active_curr = 1 AND was_active_prev = 0
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    q.report_quarter,
    EXTRACT(YEAR FROM q.report_quarter)::INT AS report_year,
    EXTRACT(QUARTER FROM q.report_quarter)::INT AS report_quarter_num,
    
    q.legal_form,
    q.customer_segment,
    q.oked_industry_code,
    q.region_code,
    'Actual' AS scenario,

    -- Metrics
    ROUND(q.avg_active_customers::NUMERIC, 0) AS active_customers,
    
    -- Line 92: LTV Calculation
    -- Avg Margin = Total Net Profit / Total Active Customers
    -- LTV = Avg Margin * Retention * Lifetime (proxied by 4 quarters for annualization if not specified)
    CASE WHEN q.avg_active_customers > 0 
        THEN ROUND(
            ( (COALESCE(f.total_net_profit_uzs, 0) / NULLIF(q.avg_active_customers, 0)) 
              * COALESCE(q.avg_retention_rate, 0) 
              * 4 -- Standard annualization factor for Time
            )::NUMERIC, 0
        )
        ELSE 0 
    END AS customer_ltv_uzs,

    -- Line 92: CAC Calculation
    -- CAC = Total Marketing Spend / New Customers
    COALESCE(f.total_marketing_expenses_uzs, 0) AS total_marketing_expenses_uzs,
    COALESCE(nc.new_customers_acquired, 0) AS new_customers_acquired,
    
    CASE WHEN COALESCE(nc.new_customers_acquired, 0) > 0
        THEN ROUND( (COALESCE(f.total_marketing_expenses_uzs, 0) / nc.new_customers_acquired)::NUMERIC, 0)
        ELSE 0
    END AS customer_acquisition_cost_uzs

FROM quarterly_agg q
LEFT JOIN financial_agg f ON f.report_quarter = q.report_quarter
LEFT JOIN new_customers_q nc ON nc.report_quarter = q.report_quarter 
    AND nc.legal_form = q.legal_form 
    AND nc.customer_segment = q.customer_segment
    AND nc.oked_industry_code = q.oked_industry_code
    AND nc.region_code = q.region_code
ORDER BY q.report_quarter DESC
