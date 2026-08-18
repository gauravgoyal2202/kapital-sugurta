{{ config(materialized='table') }}

/*
  Dashboard — Own Sales
  ----------------------
  Includes own-sales / in-house channels from curated_partner_indicators:
    - In-House - Agent    - Not API
    - In-House - Internal - API
    - In-House - Internal - Not API
    - Website  - Internal - API

  Plus the Call Center channel from curated_call_center_indicators,
  which also exposes `strah_summa_uzs` (Sum Insured).

  All raw Oracle joins and business logic live in the curated layer.
  This mart is a pure aggregation / presentation layer.
*/

-- ──────────────────────────────────────────────
-- 1. Own-Sales channels (In-House + Website)
-- ──────────────────────────────────────────────
WITH own_sales AS (

    SELECT
        month                AS report_month,
        channels,
        insurance_type,
        product_category,
        product_name,

        SUM(oplsum)          AS premium_uzs,
        SUM(kom_sum)         AS commission_uzs,
        SUM(claim_value)     AS claims_uzs,
        SUM(fifty)           AS motivation_uzs,
        SUM(ras_value)       AS terminated_uzs,
        0::NUMERIC           AS strah_summa_uzs

    FROM {{ ref('curated_partner_indicators') }}

    -- Include only own-sales / in-house channels
    WHERE channels IN (
        'In-House - Agent - Not API',
        'In-House - Internal - API',
        'In-House - Internal - Not API',
        'Website - Internal - API'
    )

    GROUP BY
        month,
        channels,
        insurance_type,
        product_category,
        product_name

),

-- ──────────────────────────────────────────────
-- 2. Call Center channel (dedicated curated model)
-- ──────────────────────────────────────────────
call_center AS (

    SELECT
        month                AS report_month,
        channels,
        insurance_type,
        product_category,
        product_name,

        oplsum               AS premium_uzs,
        kom_sum              AS commission_uzs,
        claim_value          AS claims_uzs,
        fifty                AS motivation_uzs,
        ras_value            AS terminated_uzs,
        strah_summa          AS strah_summa_uzs

    FROM {{ ref('curated_call_center_indicators') }}

),

-- ──────────────────────────────────────────────
-- 3. Combine both channels
-- ──────────────────────────────────────────────
combined AS (

    SELECT * FROM own_sales
    UNION ALL
    SELECT * FROM call_center

)

-- ──────────────────────────────────────────────
-- 4. Final output with derived KPIs
-- ──────────────────────────────────────────────
SELECT
    report_month,
    EXTRACT(YEAR    FROM report_month)::INT                             AS report_year,
    EXTRACT(QUARTER FROM report_month)::INT                             AS report_quarter,

    channels,
    CASE WHEN insurance_type = 'Call Center' THEN '' ELSE insurance_type END AS insurance_type,
    CASE
        WHEN insurance_type = 'Call Center' THEN ''
        WHEN insurance_type = 'Compulsory' THEN 'Обязательное'
        WHEN insurance_type = 'Mandatory'  THEN 'Обязательное'
        ELSE 'Добровольное'
    END AS insurance_type_ru,
    CASE
        WHEN insurance_type = 'Call Center' THEN ''
        WHEN insurance_type = 'Compulsory' THEN 'Мажбурий'
        WHEN insurance_type = 'Mandatory'  THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END AS insurance_type_uz_cyrl,
    CASE
        WHEN insurance_type = 'Call Center' THEN ''
        WHEN insurance_type = 'Compulsory' THEN 'Majburiy'
        WHEN insurance_type = 'Mandatory'  THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END AS insurance_type_uz_latn,
    CASE
        WHEN product_category IN (
            'Call Center',
            'GENERAL_INSURANCE',
            'OSAGO',
            'OSAGO – General Insurance',
            'General Insurance'
        ) THEN ''
        ELSE product_category
    END AS product_category,
    CASE WHEN product_name = 'Call Center' THEN '' ELSE product_name END AS product_name,

    -- Raw metrics (UZS)
    premium_uzs,
    commission_uzs,
    claims_uzs,
    motivation_uzs,
    terminated_uzs,
    strah_summa_uzs,

    -- Net profit after all deductions
    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs - terminated_uzs)
        AS net_profit_uzs,

    -- Profitability %
    CASE
        WHEN premium_uzs > 0
        THEN ROUND(
                (
                    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs - terminated_uzs)
                    / premium_uzs * 100
                )::NUMERIC,
             2)
        ELSE 0
    END                                                                 AS profitability_pct,

    'Actual'                                                            AS scenario

FROM combined

ORDER BY
    report_month DESC,
    channels,
    insurance_type,
    product_category,
    product_name
