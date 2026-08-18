{{ config(materialized='table') }}

/*
  Dashboard 16 — Aggregated Customer Base Summary (Optimized with NPS)
  -----------------------------------------------------------
  Updated to align with client logic and include NPS metrics.
*/

WITH spine AS (
    SELECT * FROM {{ ref('mart_customer_retention_spine') }}
),

nps_data AS (
    SELECT
        DATE_TRUNC('month', response_timestamp)::DATE as report_month,
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors
    FROM {{ ref('curated_nps_survey') }}
    WHERE survey_type = 'General NPS'
    GROUP BY 1
),

metrics AS (
    SELECT
        report_month,
        report_month AS month,
        TO_CHAR(report_month, 'Month') AS month_name,
        EXTRACT(YEAR FROM report_month) AS report_year,
        EXTRACT(QUARTER FROM report_month) AS report_quarter,
        
        COALESCE(legal_form, 'Unknown') AS legal_form,
        COALESCE(legal_form_ru, 'Unknown') AS legal_form_ru,
        COALESCE(legal_form_uz_cyrl, 'Unknown') AS legal_form_uz_cyrl,
        COALESCE(legal_form_uz_latn, 'Unknown') AS legal_form_uz_latn,
        COALESCE(customer_segment, 'Unknown') AS customer_segment,
        COALESCE(customer_segment_ru, 'Unknown') AS customer_segment_ru,
        COALESCE(customer_segment_uz_cyrl, 'Unknown') AS customer_segment_uz_cyrl,
        COALESCE(customer_segment_uz_latn, 'Unknown') AS customer_segment_uz_latn,
        COALESCE(oked_industry_code, 'Unknown') AS oked_industry_code,
        COALESCE(region_code, 'Unknown') AS region_code,
        COALESCE(region_code_uz, 'Unknown') AS region_code_uz,
        COALESCE(region_code_lat, 'Unknown') AS region_code_lat,
        
        'Actual' AS scenario,
        
        -- Line 89: Active Customers
        SUM(is_active_curr) AS active_customers,
        
        -- Line 91: Policies per Customer
        SUM(active_policy_count) AS total_active_policies,
        ROUND(SUM(active_policy_count)::NUMERIC / NULLIF(SUM(is_active_curr), 0), 4) AS avg_policies_per_customer,
        ROUND(SUM(active_policy_count)::NUMERIC / NULLIF(SUM(is_active_curr), 0), 4) AS policies_per_customer,
        
        -- Line 90: Retention Rate Components
        SUM(is_retained) AS retained_customers,
        SUM(is_retained) AS retained_customers_from_prev_month, -- Alias for backward compatibility
        SUM(was_active_prev) AS customers_at_start_of_period,
        
        ROUND((SUM(is_retained)::NUMERIC / NULLIF(SUM(was_active_prev), 0)) * 100, 2) AS retention_rate_pct
        
    FROM spine
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)

SELECT
    m.*,
    COALESCE(n.total_responses, 0) as nps_total_responses,
    CASE 
        WHEN n.total_responses > 0 
        THEN ROUND(((n.promoters - n.detractors)::NUMERIC / n.total_responses) * 100, 1)
        ELSE 0 
    END as nps_index
FROM metrics m
LEFT JOIN nps_data n ON m.report_month = n.report_month
ORDER BY m.report_month DESC
