{{ config(materialized='table') }}

/*
  Dashboard 15 — Policy Renewal Indicators
  ------------------------------------------------
  Flattened Fact Table for dynamic DAX distinct counting in Power BI.
  Grain: One row per expiring policy.
*/

SELECT
    report_month,
    EXTRACT(YEAR FROM report_month)::INT as report_year,
    EXTRACT(QUARTER FROM report_month)::INT as report_quarter,
    insurance_type,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Обязательное'
        WHEN 'Mandatory'  THEN 'Обязательное'
        ELSE 'Добровольное'
    END AS insurance_type_ru,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Мажбурий'
        WHEN 'Mandatory'  THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END AS insurance_type_uz_cyrl,
    CASE insurance_type
        WHEN 'Compulsory' THEN 'Majburiy'
        WHEN 'Mandatory'  THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END AS insurance_type_uz_latn,
    product_category,
    product_name,
    
    -- CRITICAL: We now pass the raw client ID to Power BI so it can perform DISTINCTCOUNT
    client_id, 
    
    -- We keep your exact column names so nothing breaks, but they now act as row-level flags (1 or 0)
    1 as expired_clients,
    is_renewed as renewed_clients,
    
    1 as total_expiring_policies,
    is_renewed as total_renewed_policies,
    
    -- Power BI DAX should calculate the final percentage to ensure it aggregates perfectly
    0::NUMERIC as renewal_rate_pct,
    
    'Actual' as scenario
FROM {{ ref('curated_policy_renewals') }}
