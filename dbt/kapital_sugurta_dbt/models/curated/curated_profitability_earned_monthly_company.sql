{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'earned'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpemc_month ON {{ this }} (report_month)"
    ]
) }}

/*
  Company-wide earned premium / commission / bonus (monthly).
  Rollup of curated_profitability_earned_monthly_detail — no duplicate pro-rata logic.
*/

SELECT
    report_month,
    SUM(earned_premium_uzs)                                               AS earned_premium_uzs,
    SUM(earned_bonus_uzs)                                                 AS earned_bonus_uzs,
    SUM(earned_commission_uzs)                                            AS earned_commission_uzs,
    SUM(policy_exposure)                                                  AS policy_exposure
FROM {{ ref('curated_profitability_earned_monthly_detail') }}
GROUP BY report_month
