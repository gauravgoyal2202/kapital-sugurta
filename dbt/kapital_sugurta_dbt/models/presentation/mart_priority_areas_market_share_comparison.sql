{{ config(materialized='table') }}

/*
  Dashboard 11 — Priority Areas Market Share Comparison
  -----------------------------------------------------
  Pre-pivoted table to exactly match the requested Power BI table visual
  comparing 2024 to 2025 Market Share metrics for Priority Areas.
*/

WITH max_years AS (
    SELECT 
        MAX(report_year) as curr_year,
        MAX(report_year) - 1 as prev_year
    FROM {{ ref('mart_commercial_development_priority_areas_summary') }}
),

data_prev_year AS (
    SELECT 
        priority_area,
        report_month,
        EXTRACT(MONTH FROM report_month) as month_num,
        market_value as mkt_prev,
        company_value as co_prev,
        company_share_pct as share_prev
    FROM {{ ref('mart_commercial_development_priority_areas_summary') }}
    WHERE category = 'Market Share' AND report_year = (SELECT prev_year FROM max_years)
),

data_curr_year AS (
    SELECT 
        priority_area,
        report_month,
        EXTRACT(MONTH FROM report_month) as month_num,
        market_value as mkt_curr,
        company_value as co_curr,
        company_share_pct as share_curr,
        share_change_pp,
        growth_pct
    FROM {{ ref('mart_commercial_development_priority_areas_summary') }}
    WHERE category = 'Market Share' AND report_year = (SELECT curr_year FROM max_years)
)

SELECT
    COALESCE(dc.priority_area, dp.priority_area) as priority_area,
    COALESCE(dc.report_month, dp.report_month) as report_month,
    
    ROUND(dp.mkt_prev::NUMERIC, 3) as market_premium_volume_prev_year_bn_uzs,
    ROUND(dp.co_prev::NUMERIC, 3) as company_premium_volume_prev_year_bn_uzs,
    ROUND(dp.share_prev::NUMERIC, 2) as company_share_in_market_premium_prev_year_pct,
    
    ROUND(dc.mkt_curr::NUMERIC, 3) as market_premium_volume_curr_year_bn_uzs,
    ROUND(dc.co_curr::NUMERIC, 3) as company_premium_volume_curr_year_bn_uzs,
    ROUND(dc.share_curr::NUMERIC, 2) as company_share_in_market_premium_curr_year_pct,
    
    ROUND(dc.share_change_pp::NUMERIC, 2) as share_change_pp,
    ROUND(dc.growth_pct::NUMERIC, 2) as growth_pct

FROM data_curr_year dc
FULL OUTER JOIN data_prev_year dp 
  ON dp.priority_area = dc.priority_area 
 AND dp.month_num = dc.month_num

ORDER BY report_month DESC, priority_area
