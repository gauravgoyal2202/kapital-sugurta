{{ config(materialized='table') }}

/*
  Dashboard 18 — NPS and CSI (Customer Satisfaction)
  --------------------------------------------------
  Calculates NPS Trend and average Satisfaction Index.
  NPS = % Promoters - % Detractors
*/

WITH monthly_categories AS (
    SELECT
        report_month,
        survey_type,
        -- Placeholder dimensions to align with dashboard filters
        'Unknown'::VARCHAR AS region_code,
        'Unknown'::VARCHAR AS customer_segment,
        'Unknown'::VARCHAR AS legal_form,
        'Unknown'::VARCHAR AS oked_industry_code,
        'Actual'::VARCHAR AS scenario,
        
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors,
        AVG(nps_score_raw) as avg_satisfaction_score
    FROM {{ ref('curated_nps_survey') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month)::INT as report_year,
    EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
    survey_type,
    region_code,
    customer_segment,
    legal_form,
    oked_industry_code,
    scenario,
    
    total_responses,
    avg_satisfaction_score,
    
    -- NPS Calculation: ((Promoters - Detractors) / Total) * 100
    ROUND(
        ( (promoters - detractors)::NUMERIC / NULLIF(total_responses, 0) ) * 100, 
        1
    ) AS nps_index

FROM monthly_categories
ORDER BY report_month DESC, survey_type
