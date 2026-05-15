{{ config(materialized='table') }}

/*
  Dashboard 11 — Commercial Development: Priority Areas (PIVOTED)
  --------------------------------------------------------------
  This version provides separate columns for each priority area to make
  Power BI widget setup easier.
*/

WITH base AS (
    SELECT * FROM {{ ref('mart_commercial_development_priority_areas_base') }}
),

pivoted AS (
    SELECT
        report_month,
        report_year,
        
        -- MOTOR Metrics
        MAX(CASE WHEN priority_area = 'Motor' THEN co_premium_bn END) as motor_premium_bn,
        MAX(CASE WHEN priority_area = 'Motor' THEN co_claims_bn END) as motor_claims_bn,
        MAX(CASE WHEN priority_area = 'Motor' THEN combined_ratio_pct END) as motor_combined_ratio_pct,
        MAX(CASE WHEN priority_area = 'Motor' THEN company_market_share_pct END) as motor_market_share_pct,
        
        -- BANKING Metrics
        MAX(CASE WHEN priority_area = 'Banking' THEN co_premium_bn END) as banking_premium_bn,
        MAX(CASE WHEN priority_area = 'Banking' THEN co_claims_bn END) as banking_claims_bn,
        MAX(CASE WHEN priority_area = 'Banking' THEN combined_ratio_pct END) as banking_combined_ratio_pct,
        MAX(CASE WHEN priority_area = 'Banking' THEN company_market_share_pct END) as banking_market_share_pct,
        
        -- PROPERTY Metrics
        MAX(CASE WHEN priority_area = 'Property' THEN co_premium_bn END) as property_premium_bn,
        MAX(CASE WHEN priority_area = 'Property' THEN co_claims_bn END) as property_claims_bn,
        MAX(CASE WHEN priority_area = 'Property' THEN combined_ratio_pct END) as property_combined_ratio_pct,
        MAX(CASE WHEN priority_area = 'Property' THEN company_market_share_pct END) as property_market_share_pct,
        
        -- Market Reference (Year-specific)
        MAX(CASE WHEN report_year = 2024 THEN mkt_prem_prev_year_bn ELSE mkt_prem_curr_year_bn END) as mkt_premium_bn,
        
        'Actual' as scenario
        
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM pivoted
ORDER BY report_month DESC
