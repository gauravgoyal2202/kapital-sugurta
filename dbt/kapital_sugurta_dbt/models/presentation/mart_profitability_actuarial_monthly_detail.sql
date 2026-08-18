{{ config(
    materialized = 'table',
    tags         = ['profitability', 'actuarial', 'monthly', 'detail'],
    post_hook    = [
        "CREATE INDEX IF NOT EXISTS idx_mpamd_month   ON {{ this }} (month_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_period  ON {{ this }} (report_year, report_month)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_branch  ON {{ this }} (branch_name)",
        "CREATE INDEX IF NOT EXISTS idx_mpamd_product ON {{ this }} (product_name)"
    ]
) }}

/*
  Profitability Actuarial — Monthly by Branch & Product (Mart)
  ------------------------------------------------------------
  Dimensional table for Power BI filters / detail views.

  Grain: month × branch × product (no company total rows).

  Company-wide KPIs: mart_profitability_actuarial_monthly
*/

SELECT
    month_start_date,
    report_year,
    report_month,
    report_quarter,
    period_label,

    branch_name,
    product_name,
    product_id,
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

    earned_premium_uzs,
    earned_bonus_uzs,
    earned_commission_uzs,

    total_events,
    paid_events,
    total_claimed_loss_uzs,
    paid_amount_uzs,
    unpaid_claimed_loss_uzs,
    incurred_claims_amount_uzs,

    policy_exposure,

    ceded_earned_reinsurance_premium_uzs,

    claim_frequency,
    CASE
        WHEN total_claimed_loss_uzs > 0
        THEN ROUND((total_claimed_loss_uzs / NULLIF(total_events, 0))::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_value_of_znl,
    CASE
        WHEN total_events > 0
        THEN ROUND((total_claimed_loss_uzs / total_events)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_unsettled_claims_uzs,
    CASE
        WHEN paid_events > 0
        THEN ROUND((paid_amount_uzs / paid_events)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS avg_paid_uzs,
    CASE
        WHEN total_claimed_loss_uzs > 0
        THEN ROUND((paid_amount_uzs / total_claimed_loss_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS payment_to_znu_share,
    CASE
        WHEN earned_premium_uzs > 0
        THEN ROUND((paid_amount_uzs / earned_premium_uzs * 100)::NUMERIC, 4)
        ELSE 0
    END                                                                             AS actual_loss_ratio,

    validation_status,

    (
        earned_premium_uzs
        - earned_commission_uzs
        - earned_bonus_uzs
        - incurred_claims_amount_uzs
        - ceded_earned_reinsurance_premium_uzs
    )                                                                               AS profit_amount_uzs,

    CASE
        WHEN earned_premium_uzs > 0
        THEN ROUND(
            (
                (
                    earned_premium_uzs
                    - earned_commission_uzs
                    - earned_bonus_uzs
                    - incurred_claims_amount_uzs
                    - ceded_earned_reinsurance_premium_uzs
                )
                / earned_premium_uzs * 100
            )::NUMERIC,
            2
        )
        ELSE 0
    END                                                                             AS profitability_pct,

    'Actual'::TEXT                                                                  AS scenario

FROM {{ ref('curated_profitability_dashboard_monthly_detail') }}

ORDER BY month_start_date, branch_name, product_name
