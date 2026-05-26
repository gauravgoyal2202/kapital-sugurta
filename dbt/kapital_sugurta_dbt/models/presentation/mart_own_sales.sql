{{ config(materialized='table') }}

/*
  Dashboard — Own Sales
  ----------------------
  Includes ONLY own-sales / in-house channels:
    - In-House - Agent - Not API
    - In-House - Internal - API
    - In-House - Internal - Not API
    - Website - Internal - API

  Source columns from curated_partner_indicators:
    oplsum      → gross premium collected (UZS)
    kom_sum     → agency commission paid  (UZS)
    claim_value → claims paid             (UZS, from ins_loss_oracle)
    fifty       → motivation / fifty pool (UZS, from ins_fifty_pack)
    ras_value   → reinsurance / rastorg   (UZS, from ins_rastorg_oracle)

  Profitability formula:
    (Premium - Commission - Claims - Motivation - Reinsurance) / Premium × 100
*/

WITH monthly_channel_agg AS (

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
        SUM(ras_value)       AS reinsurance_uzs

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

)

SELECT
    report_month,
    EXTRACT(YEAR    FROM report_month)::INT     AS report_year,
    EXTRACT(QUARTER FROM report_month)::INT     AS report_quarter,

    channels,
    insurance_type,
    product_category,
    product_name,

    -- Raw metrics (UZS)
    premium_uzs,
    commission_uzs,
    claims_uzs,
    motivation_uzs,
    reinsurance_uzs,

    -- Net profit after all deductions
    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs - reinsurance_uzs)
        AS net_profit_uzs,

    -- Profitability %
    CASE
        WHEN premium_uzs > 0
        THEN ROUND(
                (
                    (premium_uzs - commission_uzs - claims_uzs - motivation_uzs)
                    / premium_uzs * 100
                )::NUMERIC,
             2)
        ELSE 0
    END                                         AS profitability_pct,

    'Actual'                                    AS scenario

FROM monthly_channel_agg
ORDER BY
    report_month DESC,
    channels,
    insurance_type,
    product_category,
    product_name
