{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'cash_accounting'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_cpcam_month ON {{ this }} (month_start_date)"
    ]
) }}

/*
  Profitability — Cash / Accounting (Monthly, company-wide)
  -----------------------------------------------------------
  Rollup of curated_profitability_cash_accounting_monthly_detail plus
  company-only outgoing reinsurance (not split by branch/product).
*/

WITH month_spine AS (
    SELECT
        DATE_TRUNC('month', d)::DATE                                      AS month_start_date,
        EXTRACT(YEAR    FROM d)::INT                                      AS report_year,
        EXTRACT(MONTH   FROM d)::INT                                      AS report_month,
        EXTRACT(QUARTER FROM d)::INT                                      AS report_quarter,
        TO_CHAR(d, 'YYYY-MM')                                             AS period_label
    FROM generate_series(
        '2021-01-01'::DATE,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS d
),

detail_totals AS (
    SELECT
        month_start_date,
        SUM(premium_uzs)                                                AS premium_uzs,
        SUM(terminated_contracts_volume_uzs)                            AS terminated_contracts_volume_uzs,
        SUM(agent_commission_uzs)                                       AS agent_commission_uzs,
        SUM(claims_volume_uzs)                                          AS claims_volume_uzs,
        SUM(motivation_bonus_fifty_uzs)                                 AS motivation_bonus_fifty_uzs
    FROM {{ ref('curated_profitability_cash_accounting_monthly_detail') }}
    GROUP BY 1
),

reinsurance_excel AS (
    SELECT
        DATE_TRUNC('month', premium_accrual_date::DATE)::DATE              AS month_start_date,
        SUM(COALESCE(total_accrued_premium_uzs::NUMERIC, 0))              AS outgoing_reinsurance_premium_uzs
    FROM {{ source('raw', 'reinsurance_outgoing_portfolio') }}
    WHERE premium_accrual_date IS NOT NULL
    GROUP BY 1
),

reinsurance_oracle AS (
    SELECT
        DATE_TRUNC('month', r.slip_date)::DATE                            AS month_start_date,
        SUM(COALESCE(r.netto_accrual_premium, 0))                         AS outgoing_reinsurance_premium_uzs
    FROM {{ source('raw', 'ins_reinsurance_oracle') }} r
    WHERE r.direction = 1
      AND r.slip_date >= DATE '2026-01-01'
      AND COALESCE(r.netto_accrual_premium, 0) <> 0
    GROUP BY 1
),

reinsurance_outgoing AS (
    SELECT month_start_date, SUM(outgoing_reinsurance_premium_uzs)        AS outgoing_reinsurance_premium_uzs
    FROM (
        SELECT * FROM reinsurance_excel
        UNION ALL
        SELECT * FROM reinsurance_oracle
    ) u
    GROUP BY 1
)

SELECT
    ms.month_start_date,
    ms.report_year,
    ms.report_month,
    ms.report_quarter,
    ms.period_label,

    COALESCE(d.premium_uzs, 0)                                          AS premium_uzs,
    COALESCE(d.terminated_contracts_volume_uzs, 0)                      AS terminated_contracts_volume_uzs,
    COALESCE(d.agent_commission_uzs, 0)                                 AS agent_commission_uzs,
    COALESCE(d.claims_volume_uzs, 0)                                    AS claims_volume_uzs,
    COALESCE(d.motivation_bonus_fifty_uzs, 0)                           AS motivation_bonus_fifty_uzs,
    COALESCE(r.outgoing_reinsurance_premium_uzs, 0)                     AS outgoing_reinsurance_premium_uzs

FROM month_spine ms
LEFT JOIN detail_totals d
    ON d.month_start_date = ms.month_start_date
LEFT JOIN reinsurance_outgoing r
    ON r.month_start_date = ms.month_start_date

ORDER BY ms.month_start_date
