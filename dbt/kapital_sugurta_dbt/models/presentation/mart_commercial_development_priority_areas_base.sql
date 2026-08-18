{{ config(materialized='table') }}

/*
  Dashboard 11 — Commercial Development: Priority Areas (BASE)
  ------------------------------------------------------------
  Reads pre-aggregated data from curated_commercial_development_priority_areas.
  Maps filtr codes to friendly priority_area names and joins market share data.
*/

WITH curated AS (
    SELECT * FROM {{ ref('curated_commercial_development_priority_areas') }}
),

-- Map filtr codes to friendly priority area names
aggregated_base AS (
    SELECT
        c.month::DATE                           AS report_month,
        EXTRACT(YEAR FROM c.month)::INT         AS report_year,
        c.pturi_id,

        CASE 
            WHEN c.channels LIKE 'Banks%' THEN 'Banking'
            WHEN c.filtr = '3'            THEN 'Motor'
            WHEN c.filtr = '8,9'          THEN 'Property'
            ELSE c.filtr
        END                                     AS priority_area,

        c.channels,
        c.oplsum,
        c.kom_sum,
        c.claim_value,
        c.fifty,
        c.ras_value
    FROM curated c
),

-- Market Data (from Raw - for regulatory Excel)
mkt_agg AS (
    SELECT
        CASE
            WHEN insurance_type_name ILIKE '%1,3,10 klasslar%'
              OR insurance_type_name ILIKE '%3,14 klasslar%'
              OR insurance_type_name ILIKE '%3-klass – Yer usti transport vositalarini sug‘urta qilish%'
              OR insurance_type_name ILIKE '%transport vositalari egalarining fuqarolik javobgarligi%'
            THEN 'Motor'
            WHEN insurance_type_name ILIKE '%tashuvchilarning fuqarolik javobgarligi%'
              OR insurance_type_name ILIKE '%qurilish-montaj qaltisliklari%'
              OR insurance_type_name ILIKE '%8-klass – Mol-mulkni olovdan va tabiiy ofatlardan sug‘urta qilish%'
              OR insurance_type_name ILIKE '%9-klass – Mol-mulkni zarardan sug‘urta qilish%'
              OR insurance_type_name ILIKE '%8,9 klasslar%'
              OR insurance_type_name ILIKE '%7,8,9,13 klasslar%'
              OR insurance_type_name ILIKE '%8,9,13 klasslar%'
              OR insurance_type_name ILIKE '%8,9,16 klasslar%'
              OR insurance_type_name ILIKE '%8,9,13,16 klasslar%'
              OR insurance_type_name ILIKE '%1,2,8,9 klasslar%'
              OR insurance_type_name ILIKE '%1,8,9,13 klasslar%'
            THEN 'Property'
            ELSE 'Other'
        END AS priority_area,
        SUM(CASE WHEN report_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT - 1
                 THEN COALESCE(total_premium, 0) ELSE 0 END) AS mkt_prem_prev_year,
        SUM(CASE WHEN report_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT
                 THEN COALESCE(total_premium, 0) ELSE 0 END) AS mkt_prem_curr_year
    FROM {{ source('raw', 'market_share_insurance_class_stats') }}
    GROUP BY 1
)

SELECT
    ab.report_month,
    ab.report_year,
    ab.priority_area,
    COALESCE(pd.insurance_type, 'Voluntary') AS insurance_type,
    COALESCE(pd.insurance_type_ru, 'Добровольное') AS insurance_type_ru,
    COALESCE(pd.insurance_type_uz_cyrl, 'Ихтиёрий') AS insurance_type_uz_cyrl,
    COALESCE(pd.insurance_type_uz_latn, 'Ixtiyoriy') AS insurance_type_uz_latn,
    COALESCE(pd.category, 'Other') AS product_category,
    COALESCE(pd.product_name, 'Other') AS product_name,
    ab.channels,
    ROUND(ab.oplsum::NUMERIC,      3) AS co_premium,
    ROUND(ab.claim_value::NUMERIC, 3) AS co_claims,
    ROUND(ab.kom_sum::NUMERIC,     3) AS co_expenses,
    ROUND(ab.fifty::NUMERIC,       3) AS co_fifty,
    ROUND(ab.ras_value::NUMERIC,   3) AS co_ras,

    CASE
        WHEN ab.oplsum > 0 THEN
            ROUND(
                ((ab.oplsum - ab.kom_sum - ab.claim_value - ab.fifty)
                / ab.oplsum * 100)::NUMERIC,
                2
            )
        ELSE 0
    END AS profitability_pct,

    m.mkt_prem_prev_year,
    m.mkt_prem_curr_year,

    CASE
        WHEN ab.report_year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
         AND m.mkt_prem_prev_year > 0
        THEN ROUND((ab.oplsum / m.mkt_prem_prev_year * 100)::NUMERIC, 2)
        WHEN ab.report_year = EXTRACT(YEAR FROM CURRENT_DATE)
         AND m.mkt_prem_curr_year > 0
        THEN ROUND((ab.oplsum / m.mkt_prem_curr_year * 100)::NUMERIC, 2)
        ELSE NULL
    END AS company_market_share_pct,

    'Actual'::TEXT AS scenario

FROM aggregated_base ab
LEFT JOIN mkt_agg m ON m.priority_area = ab.priority_area
LEFT JOIN {{ ref('curated_product_dimension') }} pd ON pd.pturi_id = ab.pturi_id

UNION ALL

SELECT
    p.report_month,
    p.report_year,
    p.priority_area,
    p.insurance_type,
    p.insurance_type_ru,
    p.insurance_type_uz_cyrl,
    p.insurance_type_uz_latn,
    p.product_category,
    p.product_name,
    p.channels,
    p.co_premium,
    p.co_claims,
    p.co_expenses,
    p.co_fifty,
    p.co_ras,
    p.profitability_pct,
    NULL::NUMERIC AS mkt_prem_prev_year,
    NULL::NUMERIC AS mkt_prem_curr_year,
    NULL::NUMERIC AS company_market_share_pct,
    p.scenario
FROM {{ ref('curated_fm_plan_commercial_initiatives_monthly') }} p

ORDER BY report_month DESC, priority_area
