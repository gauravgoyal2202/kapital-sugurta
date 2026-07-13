{{config(
    materialized = 'table',
    post_hook    = [
      "CREATE INDEX IF NOT EXISTS idx_mpp_month     ON {{ this }} (report_month)",
      "CREATE INDEX IF NOT EXISTS idx_mpp_channels  ON {{ this }} (channels)"
    ]
)}}

/*
  Dashboard — Partner / Channel Profitability
  --------------------------------------------
  Includes only EXTERNAL / PARTNER channels — i.e. everything EXCEPT own-sales channels:
    - In-House - Agent - Not API
    - In-House - Internal - API
    - In-House - Internal - Not API
    - Website - Internal - API

  Source columns from curated_partner_indicators:
    oplsum      → gross premium collected (UZS)
    kom_sum     → agency commission paid  (UZS)
    claim_value → claims paid             (UZS, from ins_loss_oracle)
    fifty       → motivation / fifty pool (UZS, from ins_fifty_pack)
    ras_value   → terminated contracts    (UZS, from ins_rastorg_oracle)

  Profitability formula:
    (Premium - Commission - Claims - Terminated - Motivation) / Premium × 100
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
        SUM(ras_value)       AS terminated_uzs

    FROM {{ ref('curated_partner_indicators') }}

    -- Exclude own-sales / in-house channels
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
    terminated_uzs,

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
    END                                         AS profitability_pct,

    'Actual'                                    AS scenario

FROM monthly_channel_agg
ORDER BY
    report_month DESC,
    channels,
    insurance_type,
    product_category,
    product_name
