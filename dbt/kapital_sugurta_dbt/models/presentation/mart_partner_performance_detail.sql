{{ config(
    materialized = 'table',
    tags         = ['partner', 'performance', 'detail']
) }}

/*
  mart_partner_performance_detail
  ────────────────────────────────────────────────────────────────
  Actual only — Oracle user / channel / product grain.
  No FM plan data (FM initiatives have no Oracle partner / user mapping).

  Grain: report_month × user_id × channels × product
*/

WITH base AS (

    SELECT
        month,
        user_id,
        channels,
        insurance_type,
        product_category,
        product_name,
        oplsum       AS premium_uzs,
        kom_sum      AS commission_uzs,
        claim_value  AS claims_uzs,
        fifty        AS motivation_uzs,
        ras_value    AS terminated_uzs,
        bank_name
    FROM {{ ref('curated_partner_indicators_detail') }}

),

partner_dim AS (

    SELECT
        tb_id                                                       AS partner_id,
        TRIM(tb_surname)                                            AS partner_surname,
        TRIM(tb_name)                                               AS partner_name,
        TRIM(tb_surname) || ' ' || TRIM(tb_name)                   AS partner_full_name,
        is_partner_api,
        is_marketplace,
        CASE
            WHEN UPPER(TRIM(tb_surname)) LIKE '%BANK%'
              OR UPPER(TRIM(tb_name))    LIKE '%BANK%'  THEN 'Banks'
            WHEN is_marketplace = 1                     THEN 'Marketplaces'
            ELSE                                             'API Partners'
        END                                                         AS partner_category
    FROM {{ source('raw', 'tb_users_oracle') }}

),

monthly_total AS (

    SELECT
        month,
        SUM(premium_uzs) AS total_premium_uzs
    FROM base
    GROUP BY 1

),

enriched AS (

    SELECT
        b.month,
        b.user_id,
        b.channels,
        b.insurance_type,
        b.product_category,
        b.product_name,
        b.premium_uzs,
        b.commission_uzs,
        b.claims_uzs,
        b.motivation_uzs,
        b.terminated_uzs,
        b.bank_name,
        p.partner_id,
        p.partner_surname,
        p.partner_name,
        p.partner_full_name AS dim_partner_full_name
    FROM base b
    LEFT JOIN partner_dim p
        ON p.partner_id = b.user_id

)

SELECT
    b.month                                                         AS report_month,
    EXTRACT(YEAR    FROM b.month)::INT                              AS report_year,
    EXTRACT(QUARTER FROM b.month)::INT                              AS report_quarter,

    b.channels,

    b.channels IN (
        'In-House - Agent - Not API',
        'In-House - Internal - API',
        'In-House - Internal - Not API',
        'Website - Internal - API'
    )                                                               AS is_own,

    b.channels IN (
        'Banks - Agent - API',
        'Banks - Agent - Not API',
        'In-House - Agent - Not API',
        'Marketplace - Agent - API'
    )                                                               AS is_agent,

    b.channels IN (
        'Banks - Agent - API',
        'Banks - Agent - Not API',
        'Banks - Internal - API',
        'Banks - Internal - Not API',
        'Marketplace - Agent - API',
        'Marketplace - Internal - API'
    )                                                               AS is_partner,

    COALESCE(b.partner_id, 0)                                       AS partner_id,
    COALESCE(b.partner_surname, '')                                 AS partner_surname,
    COALESCE(b.partner_name, '')                                    AS partner_name,
    CASE
        WHEN b.channels LIKE 'Banks%'
        THEN COALESCE(b.bank_name, b.dim_partner_full_name)
        ELSE b.dim_partner_full_name
    END                                                             AS partner_full_name,
    CASE
        WHEN b.channels LIKE 'Banks%' THEN 'Banks'
        WHEN b.channels LIKE 'Marketplace%' THEN 'Marketplaces'
        WHEN b.channels LIKE '% - API' AND b.channels NOT LIKE 'Website%' THEN 'API Partners'
        ELSE 'Other'
    END                                                             AS partner_category,
    CASE
        WHEN b.channels LIKE 'Banks%' THEN 'Банки'
        WHEN b.channels LIKE 'Marketplace%' THEN 'Маркетплейсы'
        WHEN b.channels LIKE '% - API' AND b.channels NOT LIKE 'Website%' THEN 'API-партнёры'
        ELSE 'Прочее'
    END                                                             AS partner_category_ru,
    CASE
        WHEN b.channels LIKE 'Banks%' THEN 'Банклар'
        WHEN b.channels LIKE 'Marketplace%' THEN 'Маркетплейслар'
        WHEN b.channels LIKE '% - API' AND b.channels NOT LIKE 'Website%' THEN 'API hamkorlar'
        ELSE 'Бошқа'
    END                                                             AS partner_category_uz_cyrl,
    CASE
        WHEN b.channels LIKE 'Banks%' THEN 'Banklar'
        WHEN b.channels LIKE 'Marketplace%' THEN 'Marketpleyslar'
        WHEN b.channels LIKE '% - API' AND b.channels NOT LIKE 'Website%' THEN 'API hamkorlar'
        ELSE 'Boshqa'
    END                                                             AS partner_category_uz_latn,
    CASE
        WHEN b.user_id IN (19202, 19588, 20322, 40791)
        THEN 'Yes'
        ELSE 'No'
    END                                                             AS is_anor_bank,

    b.insurance_type,
    CASE b.insurance_type
        WHEN 'Compulsory' THEN 'Обязательное'
        WHEN 'Mandatory'  THEN 'Обязательное'
        ELSE 'Добровольное'
    END AS insurance_type_ru,
    CASE b.insurance_type
        WHEN 'Compulsory' THEN 'Мажбурий'
        WHEN 'Mandatory'  THEN 'Мажбурий'
        ELSE 'Ихтиёрий'
    END AS insurance_type_uz_cyrl,
    CASE b.insurance_type
        WHEN 'Compulsory' THEN 'Majburiy'
        WHEN 'Mandatory'  THEN 'Majburiy'
        ELSE 'Ixtiyoriy'
    END AS insurance_type_uz_latn,
    b.product_category,
    b.product_name,

    ROUND(b.premium_uzs::NUMERIC, 2)                                AS premium_cy,
    ROUND(
        COALESCE(
            LAG(b.premium_uzs, 12) OVER (
                PARTITION BY
                    b.user_id,
                    b.channels,
                    b.insurance_type,
                    b.product_category,
                    b.product_name
                ORDER BY b.month
            ),
            0
        )::NUMERIC,
        2
    )                                                               AS premium_py,
    ROUND(b.commission_uzs::NUMERIC, 2)                             AS commission_cy,
    ROUND(
        COALESCE(
            LAG(b.commission_uzs, 12) OVER (
                PARTITION BY
                    b.user_id,
                    b.channels,
                    b.insurance_type,
                    b.product_category,
                    b.product_name
                ORDER BY b.month
            ),
            0
        )::NUMERIC,
        2
    )                                                               AS commission_py,
    ROUND(b.claims_uzs::NUMERIC, 2)                                 AS claims_uzs,
    ROUND(b.motivation_uzs::NUMERIC, 2)                               AS motivation_uzs,
    ROUND(b.terminated_uzs::NUMERIC, 2)                               AS terminated_uzs,

    ROUND(
        (b.premium_uzs - b.commission_uzs - b.claims_uzs
            - b.motivation_uzs - b.terminated_uzs)::NUMERIC,
        2
    )                                                               AS net_profit_uzs,

    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND((b.claims_uzs / b.premium_uzs * 100)::NUMERIC, 2)
        ELSE 0
    END                                                             AS loss_ratio_pct,

    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND(
                ((b.premium_uzs - b.commission_uzs - b.claims_uzs - b.motivation_uzs - b.terminated_uzs)
                  / b.premium_uzs * 100)::NUMERIC,
                2
            )
        ELSE 0
    END                                                             AS profitability_pct,

    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND((b.commission_uzs / b.premium_uzs * 100)::NUMERIC, 2)
        ELSE 0
    END                                                             AS commission_ratio_pct,

    CASE
        WHEN mt.total_premium_uzs > 0
        THEN ROUND((b.premium_uzs / mt.total_premium_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                             AS premium_share_pct,

    CASE
        WHEN (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'In-House - Agent - Not API', 'Marketplace - Agent - API'))
         AND (b.channels IN ('In-House - Agent - Not API', 'In-House - Internal - API', 'In-House - Internal - Not API', 'Website - Internal - API'))
         AND (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'Banks - Internal - API', 'Banks - Internal - Not API', 'Marketplace - Agent - API', 'Marketplace - Internal - API'))
            THEN 'partner-own-agent'
        WHEN (b.channels IN ('In-House - Agent - Not API', 'In-House - Internal - API', 'In-House - Internal - Not API', 'Website - Internal - API'))
         AND (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'In-House - Agent - Not API', 'Marketplace - Agent - API'))
            THEN 'own-agent'
        WHEN (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'Banks - Internal - API', 'Banks - Internal - Not API', 'Marketplace - Agent - API', 'Marketplace - Internal - API'))
         AND (b.channels IN ('In-House - Agent - Not API', 'In-House - Internal - API', 'In-House - Internal - Not API', 'Website - Internal - API'))
            THEN 'partner-own'
        WHEN (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'Banks - Internal - API', 'Banks - Internal - Not API', 'Marketplace - Agent - API', 'Marketplace - Internal - API'))
         AND (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'In-House - Agent - Not API', 'Marketplace - Agent - API'))
            THEN 'partner-agent'
        WHEN (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'Banks - Internal - API', 'Banks - Internal - Not API', 'Marketplace - Agent - API', 'Marketplace - Internal - API'))
            THEN 'partner'
        WHEN (b.channels IN ('In-House - Agent - Not API', 'In-House - Internal - API', 'In-House - Internal - Not API', 'Website - Internal - API'))
            THEN 'own'
        WHEN (b.channels IN ('Banks - Agent - API', 'Banks - Agent - Not API', 'In-House - Agent - Not API', 'Marketplace - Agent - API'))
            THEN 'agent'
        ELSE 'none'
    END                                                             AS partner_type,

    'Actual'::TEXT                                                  AS scenario

FROM enriched b
LEFT JOIN monthly_total mt
    ON mt.month = b.month
ORDER BY
    b.month DESC,
    b.channels,
    partner_full_name,
    b.insurance_type,
    b.product_category,
    b.product_name
