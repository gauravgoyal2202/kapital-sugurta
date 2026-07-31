{{ config(
    materialized = 'table',
    tags         = ['profitability', 'dashboard', 'cash_accounting', 'monthly', 'detail'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpcamd_month   ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpcamd_period  ON {{ this }} (report_year, report_month)",
        "CREATE INDEX IF NOT EXISTS idx_mpcamd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_mpcamd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Profitability Cash / Accounting — Monthly by Branch & Product (Mart)
  --------------------------------------------------------------------
  Dimensional table for Power BI filters / detail views.

  Grain: month × branch × product (no company total rows).

  Company-wide KPIs: mart_profitability_cash_accounting_monthly
*/

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    branch_name,
    branch_name_uz,
    product_name,
    product_name_uz,
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
    product_category_uz,

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

FROM {{ ref('curated_profitability_cash_accounting_monthly_detail') }}

ORDER BY month_start_date, branch_name, product_name
