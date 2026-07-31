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
        MAX(legal_form_ru) AS legal_form_ru,
        MAX(legal_form_uz_cyrl) AS legal_form_uz_cyrl,
        MAX(legal_form_uz_latn) AS legal_form_uz_latn,
        MAX(customer_segment) AS customer_segment,
        MAX(customer_segment_ru) AS customer_segment_ru,
        MAX(customer_segment_uz_cyrl) AS customer_segment_uz_cyrl,
        MAX(customer_segment_uz_latn) AS customer_segment_uz_latn,
        MAX(oked_industry_code) AS oked_industry_code,
        MAX(region_code) AS region_code,
        MAX(region_code_uz) AS region_code_uz,
        MAX(region_code_lat) AS region_code_lat,
        COUNT(DISTINCT policy_id) AS active_policy_count,
        
        MAX(CASE 
            WHEN exact_start_date <= report_month 
             AND exact_end_date >= report_month 
            THEN 1 ELSE 0 
        END) AS active_on_first_day,
        
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
    legal_form_ru,
    legal_form_uz_cyrl,
    legal_form_uz_latn,
    customer_segment,
    customer_segment_ru,
    customer_segment_uz_cyrl,
    customer_segment_uz_latn,
    oked_industry_code,
    region_code,
    region_code_uz,
    region_code_lat,
    active_policy_count,
    
    1 AS is_active_curr,
    active_on_first_day AS was_active_prev,
    CASE WHEN active_on_first_day = 1 AND active_on_last_day = 1 THEN 1 ELSE 0 END AS is_retained

FROM customer_monthly_state
