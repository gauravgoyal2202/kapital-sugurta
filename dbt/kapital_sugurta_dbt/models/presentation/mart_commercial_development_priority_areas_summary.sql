{{ config(materialized='table') }}

/*
  Dashboard 11 — Bottom Section: Fully Standardized Multi-Tab Summary
  ------------------------------------------------------------------
  Normalized format with 'report_year' and 'report_quarter' columns.
  Supports 'Market Share' and 'Profitability' tabs.
*/

WITH monthly_base AS (
    SELECT * FROM {{ ref('mart_commercial_development_priority_areas_base') }}
),

-- 1. Aggregate and Normalize (One row per Tab, Area, Year, Quarter, Month)
raw_summary AS (
    -- Market Share Measurements
    SELECT
        'Market Share' as category,
        priority_area,
        report_year,
        EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
        report_month,
        MAX(CASE WHEN report_year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN mkt_prem_prev_year_bn ELSE mkt_prem_curr_year_bn END) as market_value,
        SUM(co_premium) as company_value
    FROM monthly_base
    GROUP BY 1, 2, 3, 4, 5
    
    UNION ALL
    
    -- Profitability Measurements
    SELECT
        'Profitability' as category,
        priority_area,
        report_year,
        EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
        report_month,
        NULL::NUMERIC as market_value,
        AVG(profitability_pct) as company_value
    FROM monthly_base
    GROUP BY 1, 2, 3, 4, 5
),

calculations AS (
    SELECT
        *,
        -- Share / Percentage Calculations
        CASE 
            WHEN category = 'Market Share' AND market_value > 0 THEN (company_value / market_value) * 100 
            ELSE company_value -- For CR tab, the company_value is already a %
        END as company_share_pct,
        
        -- YoY Comparison Logic
        LAG(company_value) OVER (PARTITION BY category, priority_area, EXTRACT(MONTH FROM report_month) ORDER BY report_year) as prev_year_val
    FROM raw_summary
)

SELECT
    category,
    priority_area,
    report_year,
    report_quarter,
    report_month,
    ROUND(market_value::NUMERIC, 3) as market_value,
    ROUND(company_value::NUMERIC, 3) as company_value,
    ROUND(company_share_pct::NUMERIC, 2) as company_share_pct,
    
    -- Growth metrics (comparing same month of previous year)
    CASE 
        WHEN report_year = (SELECT MAX(report_year) FROM calculations) THEN ROUND((company_share_pct - LAG(company_share_pct) OVER (PARTITION BY category, priority_area, EXTRACT(MONTH FROM report_month) ORDER BY report_year))::NUMERIC, 2)
        -- Support for all years
        WHEN LAG(company_share_pct) OVER (PARTITION BY category, priority_area, EXTRACT(MONTH FROM report_month) ORDER BY report_year) IS NOT NULL THEN ROUND((company_share_pct - LAG(company_share_pct) OVER (PARTITION BY category, priority_area, EXTRACT(MONTH FROM report_month) ORDER BY report_year))::NUMERIC, 2)
        ELSE NULL 
    END as share_change_pp,
    
    CASE 
        WHEN prev_year_val > 0 THEN ROUND(((company_value - prev_year_val) / prev_year_val * 100)::NUMERIC, 2)
        ELSE NULL 
    END as growth_pct

FROM calculations
ORDER BY category, priority_area, report_month DESC
