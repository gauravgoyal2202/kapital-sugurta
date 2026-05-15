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
        COUNT(DISTINCT policy_id) AS active_policy_count
    FROM {{ ref('curated_active_customer_portfolio') }}
    GROUP BY 1, 2
),

-- Step 2: Create a full set of Customer-Months to track drop-offs (churn)
-- We need this to see customers who were active last month but are NOT active this month.
all_customers AS (
    SELECT DISTINCT customer_id FROM customer_monthly_state
),

month_spine AS (
    SELECT DISTINCT report_month FROM customer_monthly_state
),

full_spine AS (
    SELECT 
        m.report_month,
        c.customer_id
    FROM month_spine m
    CROSS JOIN all_customers c
),

retention_flags AS (
    SELECT 
        f.report_month,
        f.customer_id,
        cms.legal_form,
        cms.customer_segment,
        cms.oked_industry_code,
        cms.region_code,
        COALESCE(cms.active_policy_count, 0) AS active_policy_count,
        
        -- Was active THIS month
        CASE WHEN cms.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_active_curr,
        
        -- Was active LAST month (Denominator)
        LAG(CASE WHEN cms.customer_id IS NOT NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY f.customer_id ORDER BY f.report_month
        ) AS was_active_prev,
        
        -- Retained (Numerator): Active both last month and this month
        CASE WHEN cms.customer_id IS NOT NULL AND LAG(cms.customer_id) OVER (
            PARTITION BY f.customer_id ORDER BY f.report_month
        ) IS NOT NULL THEN 1 ELSE 0 END AS is_retained

    FROM full_spine f
    LEFT JOIN customer_monthly_state cms 
        ON f.report_month = cms.report_month 
       AND f.customer_id = cms.customer_id
)

SELECT * 
FROM retention_flags
WHERE is_active_curr = 1 OR was_active_prev = 1
