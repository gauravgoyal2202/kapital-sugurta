{{ config(
    materialized='table',
    indexes=[
      {'columns': ['report_month']},
      {'columns': ['customer_id']},
      {'columns': ['policy_id']}
    ]
) }}

/*
  Curated Active Customer Portfolio
  ------------------------------------
  Creates a monthly snapshot dimension linking every active policy
  and customer to the months they were actively insured.
  Used fundamentally for Dashboard 16 retention and LTV calculations.
*/

WITH month_spine AS (
    -- Generates a spine for all months in the analysis window
    SELECT DATE_TRUNC('month', d)::DATE as report_month
    FROM generate_series('2021-01-01'::DATE, '2030-12-31'::DATE, '1 month'::interval) d
),

raw_active_policies AS (
    SELECT 
        po.tb_id as policy_id,
        a.owner as customer_id,
        DATE_TRUNC('month', po.tb_date_begin)::DATE as start_month,
        DATE_TRUNC('month', po.tb_date_end)::DATE as end_month,
        po.tb_date_begin::DATE as exact_start_date,
        po.tb_date_end::DATE as exact_end_date,
        
        a.fizyur,
        k.tb_orgoked as oked_code,
        COALESCE(a.temp_div, a.ins_div) as primary_division_code
        
    FROM {{ source('raw', 'ins_polis_oracle') }} po
    JOIN {{ source('raw', 'ins_anketa_oracle') }} a ON po.tb_anketa = a.ins_id
    LEFT JOIN {{ source('raw', 'ins_kontragent_oracle') }} k ON a.owner = k.tb_id
    WHERE po.tb_status IN (2, 9, 10)
      AND po.tb_date_end IS NOT NULL
      AND po.tb_date_begin IS NOT NULL
      -- Filter to prevent anomalies
      AND po.tb_date_begin >= '2021-01-01' 
)

SELECT 
    s.report_month,
    p.policy_id,
    p.customer_id,
    p.exact_start_date,
    p.exact_end_date,
    
    CASE WHEN p.fizyur = 0 THEN 'Physical' ELSE 'Juridical' END AS legal_form,
    CASE WHEN p.fizyur = 0 THEN 'Физическое лицо' ELSE 'Юридическое лицо' END AS legal_form_ru,
    CASE WHEN p.fizyur = 0 THEN 'Жисмоний' ELSE 'Юридик' END AS legal_form_uz_cyrl,
    CASE WHEN p.fizyur = 0 THEN 'Jismoniy' ELSE 'Yuridik' END AS legal_form_uz_latn,

    CASE WHEN p.fizyur = 0 THEN 'Retail' ELSE 'Corporate' END AS customer_segment,
    CASE WHEN p.fizyur = 0 THEN 'Розничный' ELSE 'Корпоративный' END AS customer_segment_ru,
    CASE WHEN p.fizyur = 0 THEN 'Чакана' ELSE 'Корпоратив' END AS customer_segment_uz_cyrl,
    CASE WHEN p.fizyur = 0 THEN 'Chakana' ELSE 'Korporativ' END AS customer_segment_uz_latn,
    
    COALESCE(p.oked_code::VARCHAR, 'Unknown') AS oked_industry_code,
    
    -- Extracting region name (falling back to exact division name, region prefix code, or Unknown)
    COALESCE(div_main.sp_name1, div_exact.sp_name1, SUBSTRING(p.primary_division_code::VARCHAR, 1, 2), 'Unknown') AS region_code,
    -- Uzbek Cyrillic region name
    COALESCE(div_main.sp_name2, div_exact.sp_name2, SUBSTRING(p.primary_division_code::VARCHAR, 1, 2), 'Unknown') AS region_code_uz,
    -- Uzbek Latin region name
    COALESCE(div_main.sp_name3, div_exact.sp_name3, SUBSTRING(p.primary_division_code::VARCHAR, 1, 2), 'Unknown') AS region_code_lat

FROM month_spine s
-- Core overlap logic: Policy spans over this month!
JOIN raw_active_policies p 
    ON s.report_month >= p.start_month 
   AND s.report_month <= p.end_month
LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div_main
    ON div_main.sp_id::text = (SUBSTRING(p.primary_division_code::VARCHAR, 1, 2) || '000')
LEFT JOIN {{ source('raw', 'sp_division_oracle') }} div_exact
    ON div_exact.sp_id = p.primary_division_code
