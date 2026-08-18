{{ config(materialized='table') }}

/*
  Insurance Operations — Monthly (Mart)
  Actual: premiums / claims from operational curated models
  Plan:   FM Excel consolidated insurance block (Jul 2025 – Dec 2028)
*/

WITH premium_agg AS (
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS report_month,
        'Actual' AS scenario,
        SUM(premium_amount) AS insurance_premium_volume_uzs,
        SUM(liability_amount) AS insurance_liabilities_volume_uzs
    FROM {{ ref('curated_insurance_premium') }}
    GROUP BY 1, 2
),

terminated_agg AS (
    SELECT
        DATE_TRUNC('month', termination_date)::DATE AS report_month,
        'Actual' AS scenario,
        SUM(terminated_amount) AS terminated_contracts_volume_uzs
    FROM {{ ref('curated_terminated_contracts') }}
    GROUP BY 1, 2
),

plan_agg AS (
    SELECT
        month_start_date                                                        AS report_month,
        scenario,
        gross_direct_premium_uzs                                                AS insurance_premium_volume_uzs,
        0::NUMERIC                                                              AS insurance_liabilities_volume_uzs,
        paid_amount_uzs                                                         AS insurance_claims_volume_uzs,
        actual_loss_ratio                                                       AS loss_ratio_pct,
        0::NUMERIC                                                              AS terminated_contracts_volume_uzs
    FROM {{ ref('curated_fm_plan_insurance_monthly') }}
),

time_spine AS (
    SELECT DISTINCT report_month, scenario FROM premium_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM terminated_agg
    UNION
    SELECT DISTINCT report_month, scenario FROM {{ ref('mart_financial_ratios_monthly') }}
    UNION
    SELECT DISTINCT report_month, scenario FROM plan_agg
)

SELECT
    s.report_month,
    s.scenario,

    COALESCE(p.insurance_premium_volume_uzs, pl.insurance_premium_volume_uzs, 0)
        AS insurance_premium_volume_uzs,

    COALESCE(pl.insurance_claims_volume_uzs, r.claims_payout, 0)
        AS insurance_claims_volume_uzs,

    COALESCE(pl.loss_ratio_pct, r.loss_ratio_pct, 0)
        AS loss_ratio_pct,

    COALESCE(p.insurance_liabilities_volume_uzs, pl.insurance_liabilities_volume_uzs, 0)
        AS insurance_liabilities_volume_uzs,

    COALESCE(t.terminated_contracts_volume_uzs, pl.terminated_contracts_volume_uzs, 0)
        AS terminated_contracts_volume_uzs

FROM time_spine s
LEFT JOIN premium_agg p
    ON s.report_month = p.report_month
   AND s.scenario = p.scenario
LEFT JOIN terminated_agg t
    ON s.report_month = t.report_month
   AND s.scenario = t.scenario
LEFT JOIN plan_agg pl
    ON s.report_month = pl.report_month
   AND s.scenario = pl.scenario
LEFT JOIN {{ ref('mart_financial_ratios_monthly') }} r
    ON s.report_month = r.report_month
   AND s.scenario = r.scenario
ORDER BY s.report_month DESC, s.scenario ASC
