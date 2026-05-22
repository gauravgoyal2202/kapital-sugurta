{{ config(
    materialized='table',
    indexes=[
        {'columns': ['report_month']},
        {'columns': ['customer_id']}
    ]
) }}

/*
  Dashboard 16 — Optimized Retention Spine
  -----------------------------------------
  Provides both the Numerator (is_retained) and the Denominator (was_active_prev_month)
  to ensure Power BI matches the client's "Retention = Retained / Start of Period" logic.
*/

WITH customer_monthly_state AS (
    SELECT 
        report_month,
        customer_id,
        MAX(legal_form) AS legal_form,
        MAX(customer_segment) AS customer_segment,
        MAX(oked_industry_code) AS oked_industry_code,
        MAX(region_code) AS region_code,
        COUNT(DISTINCT policy_id) AS active_policy_count,
        
        -- Point in Time checks:
        -- 1. Was active on EXACTLY the first day of the month?
        MAX(CASE 
            WHEN exact_start_date <= report_month 
             AND exact_end_date >= report_month 
            THEN 1 ELSE 0 
        END) AS active_on_first_day,
        
        -- 2. Was active on EXACTLY the last day of the month?
        MAX(CASE 
            WHEN exact_start_date <= (report_month + INTERVAL '1 month' - INTERVAL '1 day')::DATE 
             AND exact_end_date >= (report_month + INTERVAL '1 month' - INTERVAL '1 day')::DATE 
            THEN 1 ELSE 0 
        END) AS active_on_last_day
        
    FROM {{ ref('curated_active_customer_portfolio') }}
    GROUP BY 1, 2
)

SELECT 
    report_month,
    customer_id,
    legal_form,
    customer_segment,
    oked_industry_code,
    region_code,
    active_policy_count,
    
    -- Active at any point in the month (since they are in this table, they were active)
    1 AS is_active_curr,
    
    -- Client's Denominator: Active on the FIRST day of the month
    active_on_first_day AS was_active_prev, -- Kept alias to prevent breaking downstream models
    
    -- Client's Numerator: Active on BOTH first day and last day
    CASE WHEN active_on_first_day = 1 AND active_on_last_day = 1 THEN 1 ELSE 0 END AS is_retained

FROM customer_monthly_state
