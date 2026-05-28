{{ config(materialized='table') }}

/*
  mart_partner_performance_detail
  ────────────────────────────────────────────────────────────────
  Single flat table for dashboard table charts.

  Source : curated_partner_indicators_detail (has user_id in grain)
  Enriched with:
    • Partner info     — partner_id, partner_surname, partner_name,
                         partner_full_name, partner_category
    • channel_group    — Own Channels / Partner Channels / Agency Network
    • Derived KPIs     — net_profit, loss_ratio, profitability,
                         commission_ratio, premium_share

  Grain: report_month × user_id × channels × insurance_type × product_category × product_name
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
        ras_value    AS reinsurance_uzs
    FROM {{ ref('curated_partner_indicators_detail') }}

),

-- Partner name lookup
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

-- Monthly company total for share calculation
monthly_total AS (

    SELECT
        month,
        SUM(premium_uzs) AS total_premium_uzs
    FROM base
    GROUP BY month

)

SELECT
    -- ── Time dimensions ──────────────────────────────────────────────────
    b.month                                                         AS report_month,
    EXTRACT(YEAR    FROM b.month)::INT                              AS report_year,
    EXTRACT(QUARTER FROM b.month)::INT                              AS report_quarter,

    -- ── Channel dimensions ───────────────────────────────────────────────
    b.channels,

    -- Own: In-House (all) + Website
    b.channels IN (
        'In-House - Agent - Not API',
        'In-House - Internal - API',
        'In-House - Internal - Not API',
        'Website - Internal - API'
    )                                                               AS is_own,

    -- Agent: any channel with an Agent role
    b.channels IN (
        'Banks - Agent - API',
        'Banks - Agent - Not API',
        'In-House - Agent - Not API',
        'Marketplace - Agent - API'
    )                                                               AS is_agent,

    -- Partner: Banks + Marketplace channels
    b.channels IN (
        'Banks - Agent - API',
        'Banks - Agent - Not API',
        'Banks - Internal - API',
        'Banks - Internal - Not API',
        'Marketplace - Agent - API',
        'Marketplace - Internal - API'
    )                                                               AS is_partner,

    -- ── Partner info (from tb_users_oracle) ──────────────────────────────
    p.partner_id,
    p.partner_surname,
    p.partner_name,
    p.partner_full_name,
    p.partner_category,
    CASE WHEN b.user_id IN (19202, 19588, 20322, 40791) THEN 'Yes' ELSE 'No' END AS is_anor_bank,

    -- ── Product dimensions ───────────────────────────────────────────────
    b.insurance_type,
    b.product_category,
    b.product_name,

    -- ── Financial metrics (UZS) ──────────────────────────────────────────
    ROUND(b.premium_uzs::NUMERIC,       2)                          AS premium_uzs,
    ROUND(b.commission_uzs::NUMERIC,    2)                          AS commission_uzs,
    ROUND(b.claims_uzs::NUMERIC,        2)                          AS claims_uzs,
    ROUND(b.motivation_uzs::NUMERIC,    2)                          AS motivation_uzs,
    ROUND(b.reinsurance_uzs::NUMERIC,   2)                          AS reinsurance_uzs,

    -- Net Profit
    ROUND(
        (b.premium_uzs - b.commission_uzs - b.claims_uzs
            - b.motivation_uzs - b.reinsurance_uzs)::NUMERIC, 2
    )                                                               AS net_profit_uzs,

    -- ── KPI ratios ───────────────────────────────────────────────────────

    -- Loss Ratio %
    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND((b.claims_uzs / b.premium_uzs * 100)::NUMERIC, 2)
        ELSE 0
    END                                                             AS loss_ratio_pct,

    -- Profitability %
    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND(
                ((b.premium_uzs - b.commission_uzs - b.claims_uzs - b.motivation_uzs)
                  / b.premium_uzs * 100)::NUMERIC, 2)
        ELSE 0
    END                                                             AS profitability_pct,

    -- Commission Ratio %
    CASE
        WHEN b.premium_uzs > 0
        THEN ROUND((b.commission_uzs / b.premium_uzs * 100)::NUMERIC, 2)
        ELSE 0
    END                                                             AS commission_ratio_pct,

    -- Premium Share %
    CASE
        WHEN mt.total_premium_uzs > 0
        THEN ROUND((b.premium_uzs / mt.total_premium_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                             AS premium_share_pct,

    'Actual'                                                        AS scenario

FROM base b

LEFT JOIN partner_dim p
    ON p.partner_id = b.user_id

LEFT JOIN monthly_total mt
    ON mt.month = b.month

ORDER BY
    b.month DESC,
    b.channels,
    p.partner_full_name,
    b.insurance_type,
    b.product_category,
    b.product_name
