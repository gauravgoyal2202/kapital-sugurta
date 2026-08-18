{{ config(materialized='table') }}

/*
  Dashboard 18 — NPS and CSI (Customer Satisfaction)
  --------------------------------------------------
  Updated Logic (Line 93):
  - NPS = % Promoters - % Detractors
  - CSI = Average score per survey index
  - Joined active dimensions from mart_customer_base_summary to align with dashboard filters (region, segment, legal form, oked).
*/

WITH nps_base AS (
    SELECT
        DATE_TRUNC('month', response_timestamp)::DATE as report_month,
        survey_type,
        COUNT(*) as total_responses,
        SUM(CASE WHEN nps_category = 'Promoter' THEN 1 ELSE 0 END) as promoters,
        SUM(CASE WHEN nps_category = 'Detractor' THEN 1 ELSE 0 END) as detractors,
        AVG(nps_score_raw) as csi_score
    FROM {{ ref('curated_nps_survey') }}
    GROUP BY 1, 2
),

customer_dims AS (
    SELECT DISTINCT
        report_month,
        legal_form,
        legal_form_ru,
        legal_form_uz_cyrl,
        legal_form_uz_latn,
        customer_segment,
        customer_segment_ru,
        customer_segment_uz_cyrl,
        customer_segment_uz_latn,
        oked_industry_code,
        region_code
    FROM {{ ref('mart_customer_base_summary') }}
),

monthly_categories AS (
    SELECT
        nb.report_month,
        nb.survey_type,
        COALESCE(cd.region_code, 'Unknown') AS region_code,
        COALESCE(cd.customer_segment, 'Unknown') AS customer_segment,
        COALESCE(cd.customer_segment_ru, 'Unknown') AS customer_segment_ru,
        COALESCE(cd.customer_segment_uz_cyrl, 'Unknown') AS customer_segment_uz_cyrl,
        COALESCE(cd.customer_segment_uz_latn, 'Unknown') AS customer_segment_uz_latn,
        COALESCE(cd.legal_form, 'Unknown') AS legal_form,
        COALESCE(cd.legal_form_ru, 'Unknown') AS legal_form_ru,
        COALESCE(cd.legal_form_uz_cyrl, 'Unknown') AS legal_form_uz_cyrl,
        COALESCE(cd.legal_form_uz_latn, 'Unknown') AS legal_form_uz_latn,
        COALESCE(cd.oked_industry_code, 'Unknown') AS oked_industry_code,
        'Actual'::VARCHAR AS scenario,
        nb.total_responses,
        nb.promoters,
        nb.detractors,
        nb.csi_score
    FROM nps_base nb
    LEFT JOIN customer_dims cd
        ON cd.report_month = nb.report_month
)

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month)::INT as report_year,
    EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
    survey_type,
    region_code,
    customer_segment,
    customer_segment_ru,
    customer_segment_uz_cyrl,
    customer_segment_uz_latn,
    legal_form,
    legal_form_ru,
    legal_form_uz_cyrl,
    legal_form_uz_latn,
    oked_industry_code,
    scenario,
    total_responses,
    ROUND(
        ( (promoters - detractors)::NUMERIC / NULLIF(total_responses, 0) ) * 100, 
        1
    ) AS nps_index,
    ROUND(csi_score::NUMERIC, 2) AS csi_index

FROM monthly_categories
ORDER BY report_month DESC, survey_type
