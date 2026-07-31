{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'cash_accounting', 'monthly'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpcam_month  ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpcam_period ON {{ this }} (report_year, report_month)"
    ]
) }}

/*
  Profitability Cash / Accounting Dashboard — Monthly Mart
  --------------------------------------------------------
  Power BI source for company-wide cash/accounting profitability KPIs.

  Metrics (Oracle workbook mapping):
    Premium                      — collected premium (rows 47 + 48)
    Terminated Contracts Volume  — contract terminations (ras_value)
    Agent commission             — agency commission paid (rows 65 + 66)
    Claims Volume                — claims paid (row 51)
    Motivation (Bonus) Fifty     — fifty pool motivation
    Reinsurance                  — outgoing accrued premium (premium_accrual_date)
    Profit amount                — Premium − Terminated − Commission − Claims − Motivation − Reinsurance
    Profitability percentage     — Profit amount / Premium × 100

  Grain: one row per month (company total only).

  Branch / product breakdown: mart_profitability_cash_accounting_monthly_detail
  For earned / accrual profitability use mart_profitability_actuarial_monthly.
*/

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    premium_uzs,
    terminated_contracts_volume_uzs,
    agent_commission_uzs,
    claims_volume_uzs,
    motivation_bonus_fifty_uzs,
    outgoing_reinsurance_premium_uzs                                              AS reinsurance_uzs,

    (
        premium_uzs
        - terminated_contracts_volume_uzs
        - agent_commission_uzs
        - claims_volume_uzs
        - motivation_bonus_fifty_uzs
        - outgoing_reinsurance_premium_uzs
    )                                                                               AS profit_amount_uzs,

    CASE
        WHEN premium_uzs > 0
        THEN ROUND(
            (
                (
                    premium_uzs
                    - terminated_contracts_volume_uzs
                    - agent_commission_uzs
                    - claims_volume_uzs
                    - motivation_bonus_fifty_uzs
                    - outgoing_reinsurance_premium_uzs
                )
                / premium_uzs * 100
            )::NUMERIC,
            2
        )
        ELSE 0
    END                                                                             AS profitability_pct,

    'Actual'::TEXT                                                                  AS scenario

FROM {{ ref('curated_profitability_cash_accounting_monthly') }}

ORDER BY month_start_date
