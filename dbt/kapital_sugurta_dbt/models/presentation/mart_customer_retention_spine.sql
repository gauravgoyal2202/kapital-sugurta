{{ config(
    materialized='table',
    indexes=[
      {'columns': ['report_month']},
      {'columns': ['customer_id']},
      {'columns': ['region_code']}
    ]
) }}

/*
  Dashboard 16 — Active Customers & Retention Rate Spine
  -------------------------------------------------------
  Aggregates the curated policy spine into a unique Customer-Month grain.
  Calculates native retention status dynamically per customer per month.
*/

WITH customer_monthly_state AS (
    SELECT 
        report_month,
        customer_id,
        
        -- Native customer attributes pulled consistently
        MAX(legal_form) AS legal_form,
        MAX(customer_segment) AS customer_segment,
        MAX(oked_industry_code) AS oked_industry_code,
        MAX(region_code) AS region_code,
        
        -- How many unique active policies did this exact customer hold this month?
        COUNT(DISTINCT policy_id) AS active_policy_count
        
    FROM {{ ref('curated_active_customer_portfolio') }}
    GROUP BY 1, 2
),

retention_calc AS (
    SELECT 
        *,
        -- Window function looks directly at the previous active month for this customer
        LAG(report_month) OVER (
            PARTITION BY customer_id 
            ORDER BY report_month ASC
        ) AS previous_active_month
    FROM customer_monthly_state
)

SELECT 
    report_month,
    customer_id,
    
    legal_form,
    customer_segment,
    oked_industry_code,
    region_code,
    
    active_policy_count,
    
    -- If their previous active month is exactly 1 month prior, they successfully retained!
    CASE 
        WHEN previous_active_month = (report_month - INTERVAL '1 month')::DATE THEN 1 
        ELSE 0 
    END AS is_retained,
    
    -- Expose the denominator trigger (were they active at all last month?)
    -- This helps the dashboard calculate structural retention % cleanly
    -- Actually, to build the denominator, we must know if they were active precisely 1 month ago.
    CASE 
        WHEN previous_active_month = (report_month - INTERVAL '1 month')::DATE THEN 1 
        ELSE 0 
    END AS was_active_prev_month
    
FROM retention_calc
ORDER BY report_month DESC
