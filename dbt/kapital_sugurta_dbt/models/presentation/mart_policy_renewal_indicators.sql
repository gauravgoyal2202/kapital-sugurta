{{ config(materialized='table') }}

/*
  Dashboard 15 — Policy Renewals (Top Chart)
  ------------------------------------------------
  Aggregates policy renewal eligibility and retention 
  by month, tracking the renewal rate.
*/

WITH monthly_renewals AS (
    SELECT
        EXTRACT(YEAR FROM report_month)::INT as report_year,
        EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
        report_month,
        insurance_type,
        product_category,
        product_name,
        COUNT(*) as total_expiring_policies,
        SUM(is_renewed) as total_renewed_policies
    FROM {{ ref('curated_policy_renewals') }}
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    report_year,
    report_quarter,
    report_month,
    insurance_type,
    product_category,
    product_name,
    total_expiring_policies,
    total_renewed_policies,
    CASE 
        WHEN total_expiring_policies > 0 
        THEN ROUND((total_renewed_policies::NUMERIC / total_expiring_policies::NUMERIC) * 100, 2)
        ELSE 0 
    END as renewal_rate_pct
FROM monthly_renewals
ORDER BY report_month DESC
