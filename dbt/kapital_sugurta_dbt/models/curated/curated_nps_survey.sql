{{ config(materialized='view') }}

/*
  Curated NPS Survey Data
  ------------------------------------
  Standardizes responses from Bitrix/Google Form sources.
  Categorizes responses into Promoters, Passives, and Detractors.
*/

SELECT
    response_timestamp,
    DATE_TRUNC('month', response_timestamp)::DATE as report_month,
    nps_score_raw,
    comment_text,
    survey_type,
    
    -- NPS Specific Categorization
    CASE 
        WHEN nps_score_raw >= 9 THEN 'Promoter'
        WHEN nps_score_raw >= 7 THEN 'Passive'
        ELSE 'Detractor'
    END AS nps_category,
    
    -- Score weighting for calculation (1 for promoter, 0 for passive, -1 for detractor)
    CASE 
        WHEN nps_score_raw >= 9 THEN 1
        WHEN nps_score_raw >= 7 THEN 0
        ELSE -1
    END AS nps_weight

FROM {{ source('raw', 'nps_survey_responses') }}
WHERE nps_score_raw IS NOT NULL
