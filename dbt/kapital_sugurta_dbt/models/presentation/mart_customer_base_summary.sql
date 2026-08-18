{{ config(materialized='table') }}

/*
  Dashboard 16 — Aggregated Customer Base Summary (Optimized with NPS)
  -----------------------------------------------------------
  Actual only — Oracle customer portfolio by segment / region / legal form.
  No FM plan data (FM has retention coefficient only, not customer counts).

  Metric definitions (Oracle validation sheet — direct query confirmed):
    active_customers              = distinct owners with any in-month policy overlap
    policies_per_customer         = distinct in-month policies / active customers
    customers_at_start_of_period  = customers with a policy active on day 1 of the month
    retention_rate_pct            = retained (day 1 and last day) / day-1 active * 100
*/

WITH spine AS (
    SELECT * FROM {{ ref('mart_customer_retention_spine') }}
),

nps_data AS (
    SELECT
        DATE_TRUNC('month', response_timestamp)::DATE AS report_month,
        COUNT(*) AS total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) AS detractors
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

        'Actual'::TEXT AS scenario,

        SUM(is_active_curr) AS active_customers,
        SUM(active_policy_count) AS total_active_policies,
        ROUND(SUM(active_policy_count)::NUMERIC / NULLIF(SUM(is_active_curr), 0), 4) AS avg_policies_per_customer,
        ROUND(SUM(active_policy_count)::NUMERIC / NULLIF(SUM(is_active_curr), 0), 4) AS policies_per_customer,
        SUM(is_retained) AS retained_customers,
        SUM(is_retained) AS retained_customers_from_prev_month,
        SUM(was_active_prev) AS customers_at_start_of_period,
        ROUND((SUM(is_retained)::NUMERIC / NULLIF(SUM(was_active_prev), 0)) * 100, 2) AS retention_rate_pct

    FROM spine
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)

SELECT
    m.*,
    COALESCE(n.total_responses, 0) AS nps_total_responses,
    CASE
        WHEN n.total_responses > 0
        THEN ROUND(((n.promoters - n.detractors)::NUMERIC / n.total_responses) * 100, 1)
        ELSE 0
    END AS nps_index
FROM metrics m
LEFT JOIN nps_data n ON m.report_month = n.report_month
ORDER BY m.report_month DESC
