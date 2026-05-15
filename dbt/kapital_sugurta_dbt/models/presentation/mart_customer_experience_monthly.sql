{{ config(materialized='table') }}

/*
  Dashboard 20 — Customer Experience (CX) Analytics
  ------------------------------------------------
  This model standardizes Customer NPS feedback and comments.
*/

WITH raw_nps AS (
    SELECT
        DATE_TRUNC('month', response_timestamp)::DATE as report_month,
        nps_score_raw,
        nps_category,
        comment_text,
        survey_type
    FROM {{ ref('curated_nps_survey') }}
    WHERE survey_type = 'General NPS' -- Only Customer Data
),

monthly_totals AS (
    SELECT
        report_month,
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors
    FROM raw_nps
    GROUP BY 1
)

SELECT
    r.report_month,
    r.nps_score_raw,
    r.nps_category,
    r.comment_text,
    m.total_responses,
    ROUND(((m.promoters - m.detractors)::NUMERIC / NULLIF(m.total_responses, 0)) * 100, 1) as customer_nps_index
FROM raw_nps r
JOIN monthly_totals m ON r.report_month = m.report_month
ORDER BY r.report_month DESC, r.nps_score_raw ASC
