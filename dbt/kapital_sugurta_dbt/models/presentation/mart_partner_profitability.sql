{{ config(
    materialized = 'table',
    tags         = ['partner', 'profitability', 'monthly']
) }}

/*
  Partner / Channel Profitability (Mart)
  ------------------------------------
  Actual only — external / partner channels from curated_partner_indicators.
  No FM plan data (FM initiatives have no Oracle partner / user mapping).
*/

SELECT
    report_month,
    EXTRACT(YEAR    FROM report_month)::INT     AS report_year,
    EXTRACT(QUARTER FROM report_month)::INT     AS report_quarter,

    channels,
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

    premium_uzs,
    commission_uzs,
    claims_uzs,
    motivation_uzs,
    terminated_uzs,

    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs - terminated_uzs)
        AS net_profit_uzs,

    CASE
        WHEN premium_uzs > 0
        THEN ROUND(
                (
                    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs - terminated_uzs)
                    / premium_uzs * 100
                )::NUMERIC,
             2)
        ELSE 0
    END                                         AS profitability_pct,

    'Actual'::TEXT                              AS scenario

FROM (
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
        SUM(ras_value)       AS terminated_uzs

    FROM {{ ref('curated_partner_indicators') }}

    WHERE channels NOT IN (
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
) monthly_channel_agg

ORDER BY
    report_month DESC,
    channels,
    insurance_type,
    product_category,
    product_name
